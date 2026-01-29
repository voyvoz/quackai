package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go-bindings"
)

// Modes:
// - fusedDispatcher fuses multiple prompts per same text into one request, returns answers separated by ";"
// - singleClient one request per row+prompt
// - dispatcher batch across rows/prompts (dedup by text+prompt within chunk), unstable due to high API response times... :/
func aiLLM(info duckdb.FunctionInfo, input duckdb.DataChunk, output duckdb.Vector) {
	numRows := duckdb.DataChunkGetSize(input)
	if numRows == 0 {
		return
	}

	colVec := duckdb.DataChunkGetVector(input, 0)
	promptVec := duckdb.DataChunkGetVector(input, 1)

	colData := (*[1 << 28]duckdb.StringT)(duckdb.VectorGetData(colVec))
	colValidity := duckdb.VectorGetValidity(colVec)

	promptData := (*[1 << 28]duckdb.StringT)(duckdb.VectorGetData(promptVec))
	promptValidity := duckdb.VectorGetValidity(promptVec)

	duckdb.VectorEnsureValidityWritable(output)
	outValidity := duckdb.VectorGetValidity(output)

	if fusedDispatcher == nil && singleClient == nil && dispatcher == nil {
		for row := duckdb.IdxT(0); row < numRows; row++ {
			duckdb.ValiditySetRowInvalid(outValidity, row)
		}
		return
	}

	if fusedDispatcher != nil {
		workers := runtime.GOMAXPROCS(0)
		var wg sync.WaitGroup

		type job struct {
			row    duckdb.IdxT
			text   string
			prompt string
		}
		jobCh := make(chan job, numRows)

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range jobCh {
					ans, err := fusedDispatcher.GetResult(j.text, j.prompt)
					if err != nil || ans == "" {
						fmt.Println("Invalid")
						duckdb.ValiditySetRowInvalid(outValidity, j.row)
						continue
					}
					duckdb.VectorAssignStringElement(output, j.row, ans)
				}
			}()
		}

		for row := duckdb.IdxT(0); row < numRows; row++ {
			if !duckdb.ValidityRowIsValid(colValidity, row) || !duckdb.ValidityRowIsValid(promptValidity, row) {
				duckdb.ValiditySetRowInvalid(outValidity, row)
				continue
			}
			jobCh <- job{
				row:    row,
				text:   duckdb.StringTData(&colData[row]),
				prompt: duckdb.StringTData(&promptData[row]),
			}
		}

		close(jobCh)
		wg.Wait()
		return
	}

	if singleClient != nil {
		workers := runtime.GOMAXPROCS(0)
		var wg sync.WaitGroup

		type job struct {
			row    duckdb.IdxT
			text   string
			prompt string
		}
		jobCh := make(chan job, numRows)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range jobCh {
					ans, err := singleClient.Run(ctx, j.text, j.prompt)
					if err != nil || ans == "" {
						duckdb.ValiditySetRowInvalid(outValidity, j.row)
						continue
					}
					duckdb.VectorAssignStringElement(output, j.row, ans)
				}
			}()
		}

		for row := duckdb.IdxT(0); row < numRows; row++ {
			if !duckdb.ValidityRowIsValid(colValidity, row) || !duckdb.ValidityRowIsValid(promptValidity, row) {
				duckdb.ValiditySetRowInvalid(outValidity, row)
				continue
			}
			jobCh <- job{
				row:    row,
				text:   duckdb.StringTData(&colData[row]),
				prompt: duckdb.StringTData(&promptData[row]),
			}
		}

		close(jobCh)
		wg.Wait()
		return
	}

	type rowRef struct {
		row duckdb.IdxT
		cid string
	}

	refs := make([]rowRef, 0, int(numRows))

	// custom_id -> (text,prompt)
	type tp struct {
		text   string
		prompt string
	}
	uniq := make(map[string]tp, int(numRows))

	for row := duckdb.IdxT(0); row < numRows; row++ {
		if !duckdb.ValidityRowIsValid(colValidity, row) || !duckdb.ValidityRowIsValid(promptValidity, row) {
			duckdb.ValiditySetRowInvalid(outValidity, row)
			continue
		}

		text := duckdb.StringTData(&colData[row])
		prompt := duckdb.StringTData(&promptData[row])

		cid := customID(text, prompt)

		refs = append(refs, rowRef{row: row, cid: cid})
		if _, ok := uniq[cid]; !ok {
			uniq[cid] = tp{text: text, prompt: prompt}
		}
	}

	if len(refs) == 0 {
		return
	}

	jobs := make([]llmJob, 0, len(uniq))
	for _, v := range uniq {
		jobs = append(jobs, llmJob{
			text:   v.text,
			prompt: v.prompt,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	resMap, err := dispatcher.Submit(ctx, jobs)
	if err != nil {
		for _, r := range refs {
			duckdb.ValiditySetRowInvalid(outValidity, r.row)
		}
		return
	}

	for _, r := range refs {
		ans, ok := resMap[r.cid]
		if !ok || ans == "" {
			duckdb.ValiditySetRowInvalid(outValidity, r.row)
			continue
		}
		duckdb.VectorAssignStringElement(output, r.row, ans)
	}
}
