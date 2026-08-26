package consumer

import (
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// process 는 Node 컨슈머와 같은 기준으로 메시지를 거부해야 한다.
// 한쪽만 통과시키면 두 런타임의 처리 건수가 달라져 비교가 성립하지 않는다.
func TestProcessRejectsBadMessages(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"본문이 빔", ""},
		{"JSON 이 깨짐", "{not json"},
		{"orderId 가 없음", `{"eventType":"orders.created"}`},
		{"eventType 이 없음", `{"orderId":"ord-1"}`},
		{"모르는 이벤트 종류", `{"orderId":"ord-1","eventType":"orders.unknown"}`},
	}

	c := &Consumer{config: Config{}, stats: NewStats("t"), id: "t"}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := c.process(kafka.Message{Value: []byte(tc.value)})
			if err == nil {
				t.Error("거부해야 하는 메시지가 통과했습니다")
			}
		})
	}
}

func TestProcessAcceptsKnownEvents(t *testing.T) {
	c := &Consumer{config: Config{}, stats: NewStats("t"), id: "t"}

	for eventType := range knownEventTypes {
		payload := `{"orderId":"ord-1","eventType":"` + eventType + `","userId":"u","amount":1000}`
		if err := c.process(kafka.Message{Value: []byte(payload)}); err != nil {
			t.Errorf("%s 를 받아야 합니다: %v", eventType, err)
		}
	}
}

// 토픽 이름은 Node 쪽 ORDER_TOPICS 와 같아야 한다.
// 어긋나면 Go 컨슈머가 아무것도 못 읽는데 에러도 안 난다.
func TestDefaultTopicsMatchEventTypes(t *testing.T) {
	if len(DefaultTopics) != len(knownEventTypes) {
		t.Fatalf("토픽 수와 이벤트 종류 수가 다릅니다: %d vs %d",
			len(DefaultTopics), len(knownEventTypes))
	}

	for _, topic := range DefaultTopics {
		if !knownEventTypes[topic] {
			t.Errorf("토픽 %s 에 대응하는 이벤트 종류가 없습니다", topic)
		}
	}
}

func TestProcessAppliesDelay(t *testing.T) {
	c := &Consumer{
		config: Config{ProcessingDelay: 20 * time.Millisecond},
		stats:  NewStats("t"),
		id:     "t",
	}

	startedAt := time.Now()
	err := c.process(kafka.Message{
		Value: []byte(`{"orderId":"ord-1","eventType":"orders.created"}`),
	})
	elapsed := time.Since(startedAt)

	if err != nil {
		t.Fatalf("정상 메시지가 실패했습니다: %v", err)
	}
	if elapsed < 20*time.Millisecond {
		t.Errorf("처리 지연이 적용되지 않았습니다: %s", elapsed)
	}
}
