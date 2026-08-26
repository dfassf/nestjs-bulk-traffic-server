package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// OrderEvent 는 카프카로 실려 오는 주문 이벤트다.
// Node 쪽 OrderEventPayload 와 같은 모양이어야 한다.
type OrderEvent struct {
	EventType string `json:"eventType"`
	OrderID   string `json:"orderId"`
	UserID    string `json:"userId"`
	Amount    int64  `json:"amount"`
	// EmittedAt 은 이벤트가 만들어진 시각(밀리초)이다.
	EmittedAt int64 `json:"emittedAt"`
}

// knownEventTypes 는 Node 쪽 OrderEventType 과 같아야 한다.
var knownEventTypes = map[string]bool{
	"orders.created":     true,
	"inventory.reserved": true,
	"payments.approved":  true,
	"shipments.started":  true,
}

// Consumer 는 컨슈머 하나다.
//
// 오프셋을 수동으로 커밋한다. 자동 커밋은 언제 커밋되는지 제어할 수 없어
// 중복·유실 실험이 성립하지 않는다. Node 컨슈머와 같은 이유다.
type Consumer struct {
	reader *kafka.Reader
	config Config
	stats  *Stats
	id     string
}

// New 는 컨슈머 하나를 만든다. 아직 읽기 시작하지는 않는다.
func New(cfg Config, consumerID string) *Consumer {
	startOffset := kafka.LastOffset
	if cfg.FromBeginning {
		startOffset = kafka.FirstOffset
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		GroupID: cfg.GroupID,
		// GroupTopics 로 여러 토픽을 한 그룹에서 함께 구독한다.
		GroupTopics: cfg.Topics,
		StartOffset: startOffset,
		// 처리 지연을 크게 주면 기본 세션 타임아웃 안에 하트비트를 못 보내
		// 컨슈머가 그룹에서 쫓겨난다. 실험에서 일부러 느리게 만들 것이라 넉넉히 둔다.
		SessionTimeout: 60 * time.Second,
		// CommitInterval 을 0 으로 두면 CommitMessages 를 부를 때만 커밋한다.
		// 값을 주면 주기적으로 알아서 커밋해버려 커밋 시점을 제어할 수 없다.
		CommitInterval: 0,
	})

	return &Consumer{
		reader: reader,
		config: cfg,
		stats:  NewStats(consumerID),
		id:     consumerID,
	}
}

func (c *Consumer) Stats() *Stats { return c.stats }

func (c *Consumer) Close() error { return c.reader.Close() }

// Run 은 문맥이 끝날 때까지 메시지를 읽는다.
//
// MaxMessages 가 설정돼 있으면 그만큼 처리한 뒤 스스로 멈춘다.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		if c.config.MaxMessages > 0 && c.stats.Processed() >= c.config.MaxMessages {
			return nil
		}

		// FetchMessage 는 읽기만 하고 커밋하지 않는다.
		// ReadMessage 를 쓰면 읽는 즉시 커밋돼서 커밋 시점을 제어할 수 없다.
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			// 정상 종료 경로다. 문맥이 끝났거나 리더가 닫힌 경우.
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("메시지 읽기 실패: %w", err)
		}

		if err := c.handle(ctx, msg); err != nil {
			return err
		}
	}
}

func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	if c.config.CommitMode == BeforeProcess {
		// 처리 전에 커밋한다. 여기서 죽으면 이 건은 영영 처리되지 않는다(유실).
		if err := c.commit(ctx, msg); err != nil {
			return err
		}
	}

	startedAt := time.Now()
	if err := c.process(msg); err != nil {
		c.stats.RecordFailure()
		// 실패를 조용히 넘기면 처리된 것처럼 보인다. 어느 건이 왜 실패했는지 남긴다.
		log.Printf("[%s] 처리 실패 topic=%s partition=%d offset=%d: %v",
			c.id, msg.Topic, msg.Partition, msg.Offset, err)
	} else {
		c.stats.RecordSuccess(msg.Partition, time.Since(startedAt))
	}

	if c.config.CommitMode == AfterProcess {
		if err := c.commit(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) process(msg kafka.Message) error {
	if len(msg.Value) == 0 {
		return errors.New("메시지 본문이 비어 있습니다")
	}

	var event OrderEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("본문을 읽지 못했습니다: %w", err)
	}

	// 빈 값으로 메우면 어느 주문인지 모르는 기록이 남는다. 깨진 건 깨진 대로 알린다.
	if event.OrderID == "" || event.EventType == "" {
		return errors.New("필수 항목이 없습니다: orderId·eventType 확인 필요")
	}
	if !knownEventTypes[event.EventType] {
		return fmt.Errorf("알 수 없는 이벤트 종류입니다: %s", event.EventType)
	}

	// 한 건 처리에 걸리는 시간을 흉내낸다.
	if c.config.ProcessingDelay > 0 {
		time.Sleep(c.config.ProcessingDelay)
	}

	// Node 컨슈머와 달리 DB 에 남기지 않는다. 두 프로세스가 같은 SQLite 파일에
	// 동시에 쓰면 잠금 경합이 생겨, 카프카 소비 성능이 아니라 DB 잠금을 재게 된다.
	return nil
}

// commit 은 이 메시지까지 처리했다고 카프카에 알린다.
//
// kafkajs 를 쓰는 Node 컨슈머는 "다음에 읽을 위치" 를 직접 계산해서
// offset + 1 을 넘긴다. kafka-go 는 메시지를 통째로 받아 다음 위치를
// 내부에서 계산하므로, 여기서 +1 을 하면 한 건씩 건너뛴다.
// 같은 동작을 두 라이브러리가 다른 방식으로 표현하는 자리다.
func (c *Consumer) commit(ctx context.Context, msg kafka.Message) error {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("오프셋 커밋 실패 topic=%s partition=%d offset=%d: %w",
			msg.Topic, msg.Partition, msg.Offset, err)
	}
	return nil
}
