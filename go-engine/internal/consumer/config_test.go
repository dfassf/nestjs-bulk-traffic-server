package consumer

import (
	"testing"
	"time"
)

func TestFromEnvDefaults(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "localhost:9092")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("기본 설정을 읽지 못했습니다: %v", err)
	}

	if cfg.GroupID != "order-processor-go" {
		t.Errorf("그룹 기본값이 다릅니다: %s", cfg.GroupID)
	}
	// Node 컨슈머와 그룹이 달라야 둘 다 전량을 읽어 비교가 성립한다.
	if cfg.GroupID == "order-processor" {
		t.Error("Node 컨슈머와 그룹이 같으면 파티션을 나눠 가져 비교가 안 됩니다")
	}
	if cfg.Instances != 1 {
		t.Errorf("인스턴스 기본값이 다릅니다: %d", cfg.Instances)
	}
	if cfg.CommitMode != AfterProcess {
		t.Errorf("커밋 기본값이 다릅니다: %s", cfg.CommitMode)
	}
	if len(cfg.Topics) != len(DefaultTopics) {
		t.Errorf("토픽 기본값 개수가 다릅니다: %d", len(cfg.Topics))
	}
}

// 오타를 기본값으로 흡수하면 어떤 설정으로 측정했는지 모른 채 결과를 읽게 된다.
func TestFromEnvRejectsBadValues(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		value string
	}{
		{"컨슈머 개수에 숫자가 아닌 값", "GO_CONSUMER_COUNT", "세개"},
		{"컨슈머 개수에 음수", "GO_CONSUMER_COUNT", "-1"},
		{"컨슈머 개수에 0", "GO_CONSUMER_COUNT", "0"},
		{"처리 지연에 음수", "GO_CONSUMER_DELAY_MS", "-5"},
		{"최대 건수에 숫자가 아닌 값", "GO_CONSUMER_MAX_MESSAGES", "많이"},
		{"커밋 방식에 오타", "GO_CONSUMER_COMMIT_MODE", "after_process"},
		{"처음부터 여부에 오타", "GO_CONSUMER_FROM_BEGINNING", "yes"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KAFKA_BROKERS", "localhost:9092")
			t.Setenv(tc.key, tc.value)

			if _, err := FromEnv(); err == nil {
				t.Errorf("%s=%s 는 에러여야 합니다", tc.key, tc.value)
			}
		})
	}
}

func TestFromEnvRejectsEmptyBrokers(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "  ,  ")

	if _, err := FromEnv(); err == nil {
		t.Error("브로커가 비면 에러여야 합니다")
	}
}

func TestFromEnvReadsValues(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "a:9092, b:9092")
	t.Setenv("GO_CONSUMER_GROUP_ID", "bench")
	t.Setenv("GO_CONSUMER_COUNT", "3")
	t.Setenv("GO_CONSUMER_DELAY_MS", "50")
	t.Setenv("GO_CONSUMER_COMMIT_MODE", "before-process")
	t.Setenv("GO_CONSUMER_FROM_BEGINNING", "true")
	t.Setenv("GO_CONSUMER_MAX_MESSAGES", "1000")
	t.Setenv("GO_CONSUMER_TOPICS", "orders.created, payments.approved")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("설정을 읽지 못했습니다: %v", err)
	}

	if len(cfg.Brokers) != 2 || cfg.Brokers[0] != "a:9092" || cfg.Brokers[1] != "b:9092" {
		t.Errorf("브로커 파싱이 다릅니다: %v", cfg.Brokers)
	}
	if cfg.GroupID != "bench" {
		t.Errorf("그룹이 다릅니다: %s", cfg.GroupID)
	}
	if cfg.Instances != 3 {
		t.Errorf("인스턴스가 다릅니다: %d", cfg.Instances)
	}
	if cfg.ProcessingDelay != 50*time.Millisecond {
		t.Errorf("처리 지연이 다릅니다: %s", cfg.ProcessingDelay)
	}
	if cfg.CommitMode != BeforeProcess {
		t.Errorf("커밋 방식이 다릅니다: %s", cfg.CommitMode)
	}
	if !cfg.FromBeginning {
		t.Error("처음부터 읽기가 켜져야 합니다")
	}
	if cfg.MaxMessages != 1000 {
		t.Errorf("최대 건수가 다릅니다: %d", cfg.MaxMessages)
	}
	if len(cfg.Topics) != 2 {
		t.Errorf("토픽 개수가 다릅니다: %v", cfg.Topics)
	}
}

func TestDescribeShowsConditions(t *testing.T) {
	cfg := Config{
		GroupID:         "bench",
		Instances:       2,
		CommitMode:      AfterProcess,
		ProcessingDelay: 10 * time.Millisecond,
		MaxMessages:     500,
	}

	got := cfg.Describe()
	for _, want := range []string{"bench", "인스턴스=2", "after-process", "처리지연", "500건에서정지"} {
		if !contains(got, want) {
			t.Errorf("설명에 %q 가 없습니다: %s", want, got)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
