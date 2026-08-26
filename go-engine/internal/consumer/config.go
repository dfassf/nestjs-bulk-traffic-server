// Package consumer 는 주문 이벤트를 카프카에서 읽어 처리량을 재는 컨슈머다.
//
// Node 컨슈머(src/orders/consumer)와 같은 토픽을 다른 그룹으로 소비해서
// 두 런타임의 처리량·지연을 나란히 비교하는 것이 목적이다.
//
// Node 쪽과 달리 소비 기록을 DB 에 남기지 않는다. 두 프로세스가 같은 SQLite
// 파일에 동시에 쓰면 잠금 경합이 생겨, 카프카 소비 성능이 아니라 DB 잠금을
// 재게 된다. 중복·순서 관측은 Node 컨슈머가 맡는다.
package consumer

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// CommitMode 는 오프셋을 언제 커밋할지 정한다.
//
// 카프카는 "어디까지 읽었나" 를 오프셋으로 기억한다. 이걸 언제 기록하느냐에
// 따라 프로세스가 갑자기 죽었을 때 결과가 갈린다.
//
//	AfterProcess   처리 후 커밋. 처리했는데 커밋 전에 죽으면 재시작 후 다시 처리(중복).
//	               실무 기본값이고, 최소 한 번은 처리된다.
//	BeforeProcess  처리 전 커밋. 커밋했는데 처리 전에 죽으면 그 건은 영영 처리 안 됨(유실).
type CommitMode string

const (
	AfterProcess  CommitMode = "after-process"
	BeforeProcess CommitMode = "before-process"
)

// DefaultTopics 는 주문 흐름의 네 단계다. Node 쪽 ORDER_TOPICS 와 같아야 한다.
var DefaultTopics = []string{
	"orders.created",
	"inventory.reserved",
	"payments.approved",
	"shipments.started",
}

// Config 는 컨슈머 프로세스 하나의 설정이다.
type Config struct {
	Brokers []string
	GroupID string
	Topics  []string

	// Instances 는 이 프로세스에서 띄울 컨슈머 개수다.
	// 파티션 수를 넘기면 남는 컨슈머는 논다.
	Instances int

	// ProcessingDelay 는 한 건 처리에 걸리는 시간을 흉내낸다. Lag 을 쌓을 때 올린다.
	ProcessingDelay time.Duration

	CommitMode CommitMode

	// FromBeginning 이면 처음부터 읽는다. 새 그룹으로 과거를 재생할 때.
	FromBeginning bool

	// ReportInterval 마다 처리 현황을 출력한다.
	ReportInterval time.Duration

	// MaxMessages 가 0 보다 크면 그만큼 처리한 뒤 스스로 멈춘다.
	// 정해진 건수로 두 런타임을 비교할 때 쓴다.
	MaxMessages int
}

// envString 은 값이 없으면 기본값을 준다. 이름표 성격의 값에만 쓴다.
func envString(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// envInt 는 0 이상 정수를 읽는다.
//
// 형식이 틀리면 기본값으로 흡수하지 않고 에러를 돌려준다.
// 오타를 조용히 삼키면 어떤 설정으로 측정했는지 모른 채 결과를 읽게 된다.
func envInt(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s 는 0 이상 정수여야 합니다. 현재 값: %q", key, raw)
	}
	return n, nil
}

// envBool 은 "true" 만 참으로 본다. 그 외 값은 거짓이며, 오타는 에러다.
func envBool(key string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}

	switch strings.ToLower(raw) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("%s 는 true 또는 false 여야 합니다. 현재 값: %q", key, raw)
	}
}

func parseCommitMode(raw string) (CommitMode, error) {
	trimmed := strings.TrimSpace(strings.ToLower(raw))
	if trimmed == "" {
		return AfterProcess, nil
	}

	switch CommitMode(trimmed) {
	case AfterProcess, BeforeProcess:
		return CommitMode(trimmed), nil
	default:
		// 오타를 기본값으로 흡수하면 어떤 방식으로 돌고 있는지 모른 채 실험하게 된다.
		return "", fmt.Errorf(
			"GO_CONSUMER_COMMIT_MODE 는 %s 또는 %s 여야 합니다. 현재 값: %q",
			AfterProcess, BeforeProcess, raw,
		)
	}
}

func parseBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	return brokers
}

func parseTopics(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return DefaultTopics
	}

	parts := strings.Split(raw, ",")
	topics := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			topics = append(topics, trimmed)
		}
	}
	return topics
}

// FromEnv 는 환경변수에서 설정을 읽는다. 잘못된 값은 에러로 돌려준다.
func FromEnv() (Config, error) {
	brokers := parseBrokers(envString("KAFKA_BROKERS", "localhost:9092"))
	if len(brokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS 가 비어 있습니다")
	}

	instances, err := envInt("GO_CONSUMER_COUNT", 1)
	if err != nil {
		return Config{}, err
	}
	if instances < 1 {
		return Config{}, fmt.Errorf("GO_CONSUMER_COUNT 는 1 이상이어야 합니다: %d", instances)
	}

	delayMs, err := envInt("GO_CONSUMER_DELAY_MS", 0)
	if err != nil {
		return Config{}, err
	}

	reportMs, err := envInt("GO_CONSUMER_REPORT_INTERVAL_MS", 10000)
	if err != nil {
		return Config{}, err
	}

	maxMessages, err := envInt("GO_CONSUMER_MAX_MESSAGES", 0)
	if err != nil {
		return Config{}, err
	}

	fromBeginning, err := envBool("GO_CONSUMER_FROM_BEGINNING", false)
	if err != nil {
		return Config{}, err
	}

	commitMode, err := parseCommitMode(os.Getenv("GO_CONSUMER_COMMIT_MODE"))
	if err != nil {
		return Config{}, err
	}

	return Config{
		Brokers:         brokers,
		GroupID:         envString("GO_CONSUMER_GROUP_ID", "order-processor-go"),
		Topics:          parseTopics(os.Getenv("GO_CONSUMER_TOPICS")),
		Instances:       instances,
		ProcessingDelay: time.Duration(delayMs) * time.Millisecond,
		CommitMode:      commitMode,
		FromBeginning:   fromBeginning,
		ReportInterval:  time.Duration(reportMs) * time.Millisecond,
		MaxMessages:     maxMessages,
	}, nil
}

// Describe 는 어떤 설정으로 돌고 있는지 한 줄로 보여준다.
// 측정 결과를 나중에 읽을 때 조건을 같이 남기려고 쓴다.
func (c Config) Describe() string {
	parts := []string{
		fmt.Sprintf("그룹=%s", c.GroupID),
		fmt.Sprintf("인스턴스=%d", c.Instances),
		fmt.Sprintf("커밋=%s", c.CommitMode),
	}
	if c.ProcessingDelay > 0 {
		parts = append(parts, fmt.Sprintf("처리지연=%s", c.ProcessingDelay))
	}
	if c.FromBeginning {
		parts = append(parts, "처음부터")
	}
	if c.MaxMessages > 0 {
		parts = append(parts, fmt.Sprintf("%d건에서정지", c.MaxMessages))
	}
	return strings.Join(parts, " ")
}
