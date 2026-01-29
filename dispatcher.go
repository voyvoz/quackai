package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/liushuangls/go-anthropic/v2"
)

type llmJob struct {
	row    int
	text   string
	prompt string
}

type batchWaiter struct {
	done    chan struct{}
	results map[string]string // custom_id -> answer
	err     error
}

type LLMDispatcher struct {
	mu sync.Mutex

	pending []llmJob
	waiters []*batchWaiter

	flushScheduled bool

	client *AnthropicBatchClient

	flushDelay   time.Duration
	maxBatchSize int

	pollEvery   time.Duration
	pollTimeout time.Duration
}

func NewLLMDispatcher(client *AnthropicBatchClient) *LLMDispatcher {
	return &LLMDispatcher{
		client:         client,
		flushDelay:     5 * time.Millisecond,
		maxBatchSize:   200, // TODO
		pollEvery:      50 * time.Millisecond,
		pollTimeout:    120 * time.Second,
		flushScheduled: false,
	}
}

func (d *LLMDispatcher) Submit(ctx context.Context, jobs []llmJob) (map[string]string, error) {
	w := &batchWaiter{done: make(chan struct{})}

	d.mu.Lock()
	d.pending = append(d.pending, jobs...)
	d.waiters = append(d.waiters, w)

	if !d.flushScheduled {
		d.flushScheduled = true
		time.AfterFunc(d.flushDelay, func() { d.flush() })
	}
	d.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.done:
		return w.results, w.err
	}
}

func (d *LLMDispatcher) flush() {
	// pending jobs
	d.mu.Lock()
	d.flushScheduled = false

	jobs := d.pending
	waiters := d.waiters
	d.pending = nil
	d.waiters = nil

	client := d.client
	pollEvery := d.pollEvery
	pollTimeout := d.pollTimeout
	maxBatchSize := d.maxBatchSize
	d.mu.Unlock()

	wakeAll := func(results map[string]string, err error) {
		for _, w := range waiters {
			w.results = results
			w.err = err
			close(w.done)
		}
	}

	if len(waiters) == 0 {
		return
	}

	if len(jobs) == 0 {
		wakeAll(map[string]string{}, nil)
		return
	}

	if client == nil {
		wakeAll(nil, fmt.Errorf("anthropic client is nil"))
		return
	}

	allResults := make(map[string]string, len(jobs))
	var firstErr error

	for start := 0; start < len(jobs); start += maxBatchSize {
		end := start + maxBatchSize
		if end > len(jobs) {
			end = len(jobs)
		}
		chunk := jobs[start:end]

		reqs := make([]anthropic.InnerRequests, 0, len(chunk))
		for _, j := range chunk {
			cid := makeCustomID(j.row, j.text, j.prompt)
			reqs = append(reqs, buildAnthropicInnerRequest(cid, j.text, j.prompt, client.maxTokens))
		}

		runCtx, cancel := context.WithTimeout(context.Background(), pollTimeout+10*time.Second)
		res, err := client.RunMessageBatch(runCtx, reqs, pollEvery, pollTimeout)
		cancel()

		if err != nil && firstErr == nil {
			firstErr = err
		}
		for k, v := range res {
			allResults[k] = v
		}
	}

	wakeAll(allResults, firstErr)
}
