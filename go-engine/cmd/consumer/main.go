// 주문 이벤트를 소비하는 Go 컨슈머.
//
// 실행:
//
//	go run ./cmd/consumer
//	GO_CONSUMER_COUNT=3 GO_CONSUMER_MAX_MESSAGES=1000 go run ./cmd/consumer
//
// Node 컨슈머(npm run consumer)와 같은 토픽을 다른 그룹으로 소비한다.
// 그룹이 다르면 둘 다 전량을 읽으므로 같은 입력에 대해 나란히 비교할 수 있다.
//
// 설정은 .env.example 의 GO_CONSUMER_* 항목 참고.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/consumer"
)

func main() {
	cfg, err := consumer.FromEnv()
	if err != nil {
		// 설정이 틀렸으면 시작하지 않는다. 잘못된 조건으로 측정하면
		// 그 숫자를 나중에 신뢰할 수 없다.
		log.Fatalf("[go-consumer] 설정 오류: %v", err)
	}

	log.Printf("[go-consumer] 시작 %s", cfg.Describe())
	log.Printf("[go-consumer] 브로커: %s", strings.Join(cfg.Brokers, ","))
	log.Printf("[go-consumer] 구독 토픽: %s", strings.Join(cfg.Topics, ", "))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	consumers := make([]*consumer.Consumer, 0, cfg.Instances)
	for i := 0; i < cfg.Instances; i++ {
		id := fmt.Sprintf("%d-%d", os.Getpid(), i)
		consumers = append(consumers, consumer.New(cfg, id))
	}

	startedAt := time.Now()
	var wg sync.WaitGroup
	for _, c := range consumers {
		wg.Add(1)
		go func(c *consumer.Consumer) {
			defer wg.Done()
			if err := c.Run(ctx); err != nil {
				log.Printf("[go-consumer] 중단: %v", err)
			}
		}(c)
	}

	// 처리 현황을 주기적으로 보여준다. 파티션 분배가 여기서 보인다.
	if cfg.ReportInterval > 0 {
		go reportLoop(ctx, consumers, cfg.ReportInterval)
	}

	wg.Wait()

	// 리더를 닫아야 그룹에서 정상적으로 빠진다.
	for _, c := range consumers {
		if err := c.Close(); err != nil {
			log.Printf("[go-consumer] 리더 종료 실패: %v", err)
		}
	}

	log.Printf("[go-consumer] 종료 (총 %s)", time.Since(startedAt).Round(time.Millisecond))
	printReport(consumers)
}

func reportLoop(ctx context.Context, consumers []*consumer.Consumer, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			printReport(consumers)
		}
	}
}

func printReport(consumers []*consumer.Consumer) {
	totalProcessed := 0
	totalFailed := 0
	for _, c := range consumers {
		snapshot := c.Stats().Snapshot()
		totalProcessed += snapshot.Processed
		totalFailed += snapshot.Failed
	}

	if totalProcessed == 0 && totalFailed == 0 {
		return
	}

	log.Printf("[go-consumer] 누적 처리 %d건 실패 %d건", totalProcessed, totalFailed)

	for _, c := range consumers {
		snapshot := c.Stats().Snapshot()
		log.Printf("  %s 처리=%d %s 평균=%s p95=%s %s",
			snapshot.ConsumerID,
			snapshot.Processed,
			formatPartitions(snapshot.PartitionCounts),
			consumer.FormatDuration(snapshot.AvgLatency),
			consumer.FormatDuration(snapshot.P95Latency),
			consumer.FormatThroughput(snapshot.Throughput),
		)
	}
}

// formatPartitions 는 파티션별 처리 건수를 보여준다.
// 하나도 없으면 이 컨슈머가 놀고 있다는 뜻이다(파티션보다 컨슈머가 많을 때).
func formatPartitions(counts map[int]int) string {
	if len(counts) == 0 {
		return "(할당된 파티션 없음)"
	}

	partitions := make([]int, 0, len(counts))
	for partition := range counts {
		partitions = append(partitions, partition)
	}
	sort.Ints(partitions)

	parts := make([]string, 0, len(partitions))
	for _, partition := range partitions {
		parts = append(parts, fmt.Sprintf("p%d:%d", partition, counts[partition]))
	}
	return strings.Join(parts, " ")
}
