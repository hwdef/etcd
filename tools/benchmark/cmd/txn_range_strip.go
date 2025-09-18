package cmd

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"

	v3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/pkg/v3/report"
)

var txnRangeStripCmd = &cobra.Command{
	Use:   "txn-range-strip",
	Short: "Benchmark txn performance with and without range stripping",
	Run:   txnRangeStripFunc,
}

var (
	txnRangeStripTotal    int
	txnRangeStripRate     int
	txnRangeStripRangeOps int
	txnRangeStripPutOps   int
)

func init() {
	RootCmd.AddCommand(txnRangeStripCmd)
	txnRangeStripCmd.Flags().IntVar(&keySize, "key-size", 8, "Key size for txn range strip benchmark")
	txnRangeStripCmd.Flags().IntVar(&valSize, "val-size", 8, "Value size for txn range strip benchmark")
	txnRangeStripCmd.Flags().IntVar(&txnRangeStripRangeOps, "range-ops", 1, "Number of range ops issued per txn before stripping")
	txnRangeStripCmd.Flags().IntVar(&txnRangeStripPutOps, "put-ops", 1, "Number of put ops issued per txn")
	txnRangeStripCmd.Flags().IntVar(&txnRangeStripRate, "rate", 0, "Maximum txns per second (0 is no limit)")
	txnRangeStripCmd.Flags().IntVar(&txnRangeStripTotal, "total", 10000, "Total number of txn requests per scenario")
	txnRangeStripCmd.Flags().IntVar(&keySpaceSize, "key-space-size", 1, "Maximum possible keys")
}

func txnRangeStripFunc(cmd *cobra.Command, _ []string) {
	if keySpaceSize <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --key-space-size, got (%v)", keySpaceSize)
		os.Exit(1)
	}
	if txnRangeStripRangeOps <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --range-ops, got (%v)", txnRangeStripRangeOps)
		os.Exit(1)
	}
	if txnRangeStripPutOps <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --put-ops, got (%v)", txnRangeStripPutOps)
		os.Exit(1)
	}

	clients := mustCreateClients(totalClients, totalConns)
	value := string(mustRandBytes(valSize))
	prefillTxnRangeStripKeys(clients[0], value)

	fmt.Println("== Running txn range strip benchmark ==")
	fmt.Println("Range operations are always included in the txn; server-side stripping determines performance.")

	result := runTxnRangeStripScenario(cmd, clients, value)
	fmt.Println(result.report)

	tps := throughputFromScenario(result.duration)
	fmt.Printf("Txn throughput: %.2f txn/sec\n", tps)
	fmt.Println("Run this benchmark before and after removing range stripping logic to compare results.")
}

type txnRangeStripScenarioResult struct {
	report   string
	duration time.Duration
}

func runTxnRangeStripScenario(cmd *cobra.Command, clients []*v3.Client, value string) txnRangeStripScenarioResult {
	requests := make(chan []v3.Op, int(totalClients))
	limit := txnRangeStripRate
	if limit == 0 {
		limit = math.MaxInt32
	}
	limiter := rate.NewLimiter(rate.Limit(limit), 1)

	bar = pb.New(txnRangeStripTotal)
	bar.Start()

	r := newReport(cmd.Name())
	for i := range clients {
		wg.Add(1)
		go func(c *v3.Client) {
			defer wg.Done()
			for ops := range requests {
				limiter.Wait(context.Background())
				st := time.Now()
				_, err := c.Txn(context.TODO()).Then(ops...).Commit()
				r.Results() <- report.Result{Err: err, Start: st, End: time.Now()}
				bar.Increment()
			}
		}(clients[i])
	}

	start := time.Now()
	go func() {
		for i := 0; i < txnRangeStripTotal; i++ {
			requests <- buildTxnRangeStripOps(i, value)
		}
		close(requests)
	}()

	rc := r.Run()
	wg.Wait()
	close(r.Results())
	bar.Finish()

	return txnRangeStripScenarioResult{report: <-rc, duration: time.Since(start)}
}

func buildTxnRangeStripOps(txnIndex int, value string) []v3.Op {
	totalOps := txnRangeStripPutOps + txnRangeStripRangeOps
	ops := make([]v3.Op, 0, totalOps)

	for i := 0; i < txnRangeStripRangeOps; i++ {
		key := buildTxnRangeStripKey((txnIndex*txnRangeStripRangeOps + i) % keySpaceSize)
		ops = append(ops, v3.OpGet(key))
	}

	for i := 0; i < txnRangeStripPutOps; i++ {
		key := buildTxnRangeStripKey((txnIndex*txnRangeStripPutOps + i) % keySpaceSize)
		ops = append(ops, v3.OpPut(key, value))
	}

	return ops
}

func buildTxnRangeStripKey(index int) string {
	keyBytes := make([]byte, keySize)
	binary.PutVarint(keyBytes, int64(index))
	return string(keyBytes)
}

func prefillTxnRangeStripKeys(c *v3.Client, value string) {
	ctx := context.Background()
	for i := 0; i < keySpaceSize; i++ {
		key := buildTxnRangeStripKey(i)
		if _, err := c.Put(ctx, key, value); err != nil {
			fmt.Fprintf(os.Stderr, "failed to prefill key %d: %v\n", i, err)
			os.Exit(1)
		}
	}
}

func throughputFromScenario(duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	return float64(txnRangeStripTotal) / duration.Seconds()
}