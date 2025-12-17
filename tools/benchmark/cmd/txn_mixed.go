// Copyright 2021 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"

	v3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/pkg/v3/report"
)

// mixeTxnCmd represents the mixedTxn command
var mixedTxnCmd = &cobra.Command{
	Use:   "txn-mixed key [end-range]",
	Short: "Benchmark a mixed load of txn-put & txn-range.",

	Run: mixedTxnFunc,
}

var (
	mixedTxnTotal          int
	mixedTxnRate           int
	mixedTxnReadWriteRatio float64
	mixedTxnRangeLimit     int64
	mixedTxnEndKey         string
	mixedTxnOpsPerTxn      int

	writeOpsTotal      uint64
	readOpsTotal       uint64
	mixedTxnMixedTotal uint64
)

func init() {
	RootCmd.AddCommand(mixedTxnCmd)
	mixedTxnCmd.Flags().IntVar(&keySize, "key-size", 8, "Key size of mixed txn")
	mixedTxnCmd.Flags().IntVar(&valSize, "val-size", 8, "Value size of mixed txn")
	mixedTxnCmd.Flags().IntVar(&mixedTxnRate, "rate", 0, "Maximum txns per second (0 is no limit)")
	mixedTxnCmd.Flags().IntVar(&mixedTxnTotal, "total", 10000, "Total number of txn requests")
	mixedTxnCmd.Flags().StringVar(&mixedTxnEndKey, "end-key", "",
		"Read operation range end key. By default, we do full range query with the default limit of 1000.")
	mixedTxnCmd.Flags().Int64Var(&mixedTxnRangeLimit, "limit", 1000, "Read operation range result limit")
	mixedTxnCmd.Flags().IntVar(&keySpaceSize, "key-space-size", 1, "Maximum possible keys")
	mixedTxnCmd.Flags().StringVar(&rangeConsistency, "consistency", "l", "Linearizable(l) or Serializable(s)")
	mixedTxnCmd.Flags().Float64Var(&mixedTxnReadWriteRatio, "rw-ratio", 1, "Read/write ops ratio")
	mixedTxnCmd.Flags().IntVar(&mixedTxnOpsPerTxn, "txn-ops", 1, "Number of operations per transaction")
}

type request struct {
	ops      []v3.Op
	readOps  int
	writeOps int
}

func mixedTxnFunc(cmd *cobra.Command, _ []string) {
	if keySpaceSize <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --key-space-size, got (%v)", keySpaceSize)
		os.Exit(1)
	}
	if mixedTxnOpsPerTxn <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --txn-ops, got (%v)", mixedTxnOpsPerTxn)
		os.Exit(1)
	}

	readOpsTotal = 0
	writeOpsTotal = 0
	mixedTxnMixedTotal = 0

	if rangeConsistency == "l" {
		fmt.Println("bench with linearizable range")
	} else if rangeConsistency == "s" {
		fmt.Println("bench with serializable range")
	} else {
		fmt.Fprintln(os.Stderr, cmd.Usage())
		os.Exit(1)
	}

	requests := make(chan request, totalClients)
	if mixedTxnRate == 0 {
		mixedTxnRate = math.MaxInt32
	}
	limit := rate.NewLimiter(rate.Limit(mixedTxnRate), 1)
	clients := mustCreateClients(totalClients, totalConns)
	k, v := make([]byte, keySize), string(mustRandBytes(valSize))

	bar = pb.New(mixedTxnTotal)
	bar.Start()

	reportRead := newReport(cmd.Name() + "-read")
	reportWrite := newReport(cmd.Name() + "-write")
	reportMixed := newReport(cmd.Name() + "-mixed-ops")
	for i := range clients {
		wg.Add(1)
		go func(c *v3.Client) {
			defer wg.Done()
			for req := range requests {
				limit.Wait(context.Background())
				st := time.Now()
				_, err := c.Txn(context.TODO()).Then(req.ops...).Commit()

				res := report.Result{Err: err, Start: st, End: time.Now()}
				switch {
				case req.readOps > 0 && req.writeOps == 0:
					reportRead.Results() <- res
				case req.writeOps > 0 && req.readOps == 0:
					reportWrite.Results() <- res
				default:
					reportMixed.Results() <- res
				}
				bar.Increment()
			}
		}(clients[i])
	}

	go func() {
		readProbability := mixedTxnReadWriteRatio / (1 + mixedTxnReadWriteRatio)
		for i := 0; i < mixedTxnTotal; i++ {
			req := request{ops: make([]v3.Op, 0, mixedTxnOpsPerTxn)}
			for j := 0; j < mixedTxnOpsPerTxn; j++ {
				if rand.Float64() < readProbability {
					opts := []v3.OpOption{v3.WithRange(mixedTxnEndKey)}
					if rangeConsistency == "s" {
						opts = append(opts, v3.WithSerializable())
					}
					opts = append(opts, v3.WithPrefix(), v3.WithLimit(mixedTxnRangeLimit))
					req.ops = append(req.ops, v3.OpGet("", opts...))
					req.readOps++
					continue
				}

				binary.PutVarint(k, int64(((i*mixedTxnOpsPerTxn)+j)%keySpaceSize))
				req.ops = append(req.ops, v3.OpPut(string(k), v))
				req.writeOps++
			}
			readOpsTotal += uint64(req.readOps)
			writeOpsTotal += uint64(req.writeOps)
			if req.readOps > 0 && req.writeOps > 0 {
				mixedTxnMixedTotal++
			}
			requests <- req
		}
		close(requests)
	}()

	rcRead := reportRead.Run()
	rcWrite := reportWrite.Run()
	rcMixed := reportMixed.Run()
	wg.Wait()
	close(reportRead.Results())
	close(reportWrite.Results())
	close(reportMixed.Results())
	bar.Finish()
	fmt.Printf("Total Read Ops: %d\nDetails:", readOpsTotal)
	fmt.Println(<-rcRead)
	fmt.Printf("Total Write Ops: %d\nDetails:", writeOpsTotal)
	fmt.Println(<-rcWrite)
	if mixedTxnMixedTotal > 0 {
		fmt.Printf("Transactions containing both read and write ops: %d\nDetails:", mixedTxnMixedTotal)
		fmt.Println(<-rcMixed)
	}
}
