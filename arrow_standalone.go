// main.go
package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type timingStats struct {
	reqCount uint64
	reqNanos uint64
}

func (s *timingStats) add(d time.Duration) {
	atomic.AddUint64(&s.reqCount, 1)
	atomic.AddUint64(&s.reqNanos, uint64(d))
}

func (s *timingStats) snapshot() (count uint64, total time.Duration) {
	count = atomic.LoadUint64(&s.reqCount)
	total = time.Duration(atomic.LoadUint64(&s.reqNanos))
	return
}

var globalTiming timingStats

func RecordUpstreamRequest(d time.Duration) {
	globalTiming.add(d)
}

func aiLLMBatch(
	texts []string, textValid []bool,
	prompts []string, promptValid []bool,
	parallel int,
) ([]string, []bool) {
	n := len(texts)
	out := make([]string, n)
	outValid := make([]bool, n)

	if fusedDispatcher == nil && singleClient == nil && dispatcher == nil {
		return out, outValid
	}

	if parallel <= 0 {
		parallel = runtime.GOMAXPROCS(0)
	}

	if fusedDispatcher != nil {
		type job struct {
			i      int
			text   string
			prompt string
		}
		jobCh := make(chan job, n)
		var wg sync.WaitGroup

		for w := 0; w < parallel; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range jobCh {
					ans, err := fusedDispatcher.GetResult(j.text, j.prompt)
					if err != nil || ans == "" {
						outValid[j.i] = false
						continue
					}
					out[j.i] = ans
					outValid[j.i] = true
				}
			}()
		}

		for i := 0; i < n; i++ {
			if !textValid[i] || !promptValid[i] {
				outValid[i] = false
				continue
			}
			jobCh <- job{i: i, text: texts[i], prompt: prompts[i]}
		}

		close(jobCh)
		wg.Wait()
		return out, outValid
	}

	if singleClient != nil {
		type job struct {
			i      int
			text   string
			prompt string
		}
		jobCh := make(chan job, n)
		var wg sync.WaitGroup

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		for w := 0; w < parallel; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range jobCh {
					t0 := time.Now()
					ans, err := singleClient.Run(ctx, j.text, j.prompt)
					RecordUpstreamRequest(time.Since(t0))

					if err != nil || ans == "" {
						outValid[j.i] = false
						continue
					}
					out[j.i] = ans
					outValid[j.i] = true
				}
			}()
		}

		for i := 0; i < n; i++ {
			if !textValid[i] || !promptValid[i] {
				outValid[i] = false
				continue
			}
			jobCh <- job{i: i, text: texts[i], prompt: prompts[i]}
		}

		close(jobCh)
		wg.Wait()
		return out, outValid
	}

	type rowRef struct {
		i   int
		cid string
	}
	refs := make([]rowRef, 0, n)

	type tp struct {
		text   string
		prompt string
	}
	uniq := make(map[string]tp, n)

	for i := 0; i < n; i++ {
		if !textValid[i] || !promptValid[i] {
			outValid[i] = false
			continue
		}
		cid := customID(texts[i], prompts[i])
		refs = append(refs, rowRef{i: i, cid: cid})
		if _, ok := uniq[cid]; !ok {
			uniq[cid] = tp{text: texts[i], prompt: prompts[i]}
		}
	}

	if len(refs) == 0 {
		return out, outValid
	}

	jobs := make([]llmJob, 0, len(uniq))
	for _, v := range uniq {
		jobs = append(jobs, llmJob{text: v.text, prompt: v.prompt})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	t0 := time.Now()
	resMap, err := dispatcher.Submit(ctx, jobs)
	RecordUpstreamRequest(time.Since(t0))

	if err != nil {
		for _, r := range refs {
			outValid[r.i] = false
		}
		return out, outValid
	}

	for _, r := range refs {
		ans := resMap[r.cid]
		if ans == "" {
			outValid[r.i] = false
			continue
		}
		out[r.i] = ans
		outValid[r.i] = true
	}
	return out, outValid
}

func main() {
	const inPath = "animals.arrow"

	promptList := []string{
		"What sound does this animal make?",
		"Reverse the name and capitalize it",
		"Is this animal typically a pet? Answer yes/no.",
		"Return the plural form.",
	}

	startWall := time.Now()

	f, err := os.Open(inPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open file:", err)
		os.Exit(1)
	}
	defer f.Close()

	mode := "none"
	switch {
	case fusedDispatcher != nil:
		mode = "fused"
	case singleClient != nil:
		mode = "single"
	case dispatcher != nil:
		mode = "batch" // unstable due to high API response times :(
	}
	fmt.Fprintln(os.Stderr, "MODE =", mode)

	if mode == "none" {
		fmt.Fprintln(os.Stderr, "no dispatcher init faild")
		os.Exit(1)
	}
	if len(promptList) == 0 {
		fmt.Fprintln(os.Stderr, "you need prompts for this")
		os.Exit(1)
	}

	pool := memory.NewGoAllocator()

	fr, err := ipc.NewFileReader(f, ipc.WithAllocator(pool))
	if err != nil {
		fmt.Fprintln(os.Stderr, "ipc file reader:", err)
		os.Exit(1)
	}
	defer fr.Close()

	printedHeader := false
	parallel := runtime.GOMAXPROCS(0)

	for bi := 0; bi < fr.NumRecords(); bi++ {
		rec, err := fr.Record(bi)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read record batch %d: %v\n", bi, err)
			os.Exit(1)
		}
		rec.Retain()

		schema := rec.Schema()
		idIdx := fieldIndex(schema, "id")
		textIdx := fieldIndex(schema, "text")

		n := int(rec.NumRows())
		if n == 0 {
			rec.Release()
			continue
		}

		ids, idValid, err := extractIntColumn(rec.Column(idIdx), n)
		if err != nil {
			fmt.Fprintln(os.Stderr, "read id column:", err)
			rec.Release()
			os.Exit(1)
		}

		textArr, ok := rec.Column(textIdx).(*array.String)
		if !ok {
			fmt.Fprintf(os.Stderr, "column %q is %T; expected string\n", "text", rec.Column(textIdx))
			rec.Release()
			os.Exit(1)
		}
		texts, textValid := extractStringColumn(textArr, n)

		results := make([][]string, len(promptList))
		valids := make([][]bool, len(promptList))
		for p := range promptList {
			results[p] = make([]string, n)
			valids[p] = make([]bool, n)
		}

		makePromptArray := func(p string) (arr []string, pValid []bool) {
			arr = make([]string, n)
			pValid = make([]bool, n)
			for i := 0; i < n; i++ {
				pValid[i] = textValid[i]
				arr[i] = p
			}
			return
		}

		if mode == "fused" {
			var wg sync.WaitGroup
			wg.Add(len(promptList))

			for pi := range promptList {
				pi := pi
				pArr, pValid := makePromptArray(promptList[pi])

				go func() {
					defer wg.Done()
					results[pi], valids[pi] = aiLLMBatch(texts, textValid, pArr, pValid, parallel)
				}()
			}

			wg.Wait()
		} else {
			for pi := range promptList {
				pArr, pValid := makePromptArray(promptList[pi])
				results[pi], valids[pi] = aiLLMBatch(texts, textValid, pArr, pValid, parallel)
			}
		}

		if !printedHeader {
			fmt.Print("id\tname")

			for pi := range promptList {
				fmt.Printf("\tout%d", pi)
			}
			fmt.Println()
			printedHeader = true
		}

		for i := 0; i < n; i++ {
			if !idValid[i] || !textValid[i] {
				// print NULLs
				fmt.Print("NULL\tNULL")
				for range promptList {
					fmt.Print("\tNULL")
				}
				fmt.Println()
				continue
			}

			fmt.Printf("%d\t%s", ids[i], texts[i])

			for pi := range promptList {
				val := "NULL"
				if valids[pi][i] && results[pi][i] != "" {
					val = oneLine(results[pi][i])
				}
				fmt.Printf("\t%s", val)
			}
			fmt.Println()
		}

		rec.Release()
	}

	wall := time.Since(startWall)
	reqCount, reqTotal := globalTiming.snapshot()

	fmt.Println()
	fmt.Printf("mode\t%s\n", mode)
	fmt.Printf("total_wall_time_sec\t%.6f\n", wall.Seconds())
	fmt.Printf("requests\t%d\n", reqCount)

	if wall > 0 {
		fmt.Printf("requests_per_sec\t%.6f\n", float64(reqCount)/wall.Seconds())
	} else {
		fmt.Printf("requests_per_sec\t0\n")
	}

	if reqCount > 0 {
		avg := time.Duration(int64(reqTotal) / int64(reqCount))
		fmt.Printf("avg_request_time_ms\t%.3f\n", float64(avg)/float64(time.Millisecond))
	} else {
		fmt.Printf("avg_request_time_ms\t0\n")
	}
}

func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	return strings.TrimSpace(s)
}

func fieldIndex(s *arrow.Schema, name string) int {
	for i, f := range s.Fields() {
		if f.Name == name {
			return i
		}
	}
	fmt.Fprintf(os.Stderr, "schema has no column %q\nschema: %s\n", name, s)
	os.Exit(1)
	return -1
}

func extractStringColumn(a *array.String, n int) ([]string, []bool) {
	out := make([]string, n)
	valid := make([]bool, n)
	for i := 0; i < n; i++ {
		if a.IsNull(i) {
			valid[i] = false
			out[i] = ""
		} else {
			valid[i] = true
			out[i] = a.Value(i)
		}
	}
	return out, valid
}

func extractIntColumn(col arrow.Array, n int) ([]int64, []bool, error) {
	out := make([]int64, n)
	valid := make([]bool, n)

	switch a := col.(type) {
	case *array.Int32:
		for i := 0; i < n; i++ {
			if a.IsNull(i) {
				valid[i] = false
			} else {
				valid[i] = true
				out[i] = int64(a.Value(i))
			}
		}
		return out, valid, nil
	case *array.Int64:
		for i := 0; i < n; i++ {
			if a.IsNull(i) {
				valid[i] = false
			} else {
				valid[i] = true
				out[i] = a.Value(i)
			}
		}
		return out, valid, nil
	default:
		return nil, nil, fmt.Errorf("unsupported type: %T", col)
	}
}
