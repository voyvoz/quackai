package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type fusedBatch struct {
	sync.Mutex
	text string

	// prompt -> answer
	prompts map[string]string

	// insertion order of prompts (due to golang specific map ordering, same as rust :/ )
	order []string

	frozen bool

	err  error
	done chan struct{}
}

type fusedWorkItem struct {
	text        string
	promptList  []string
	fusedPrompt string
	b           *fusedBatch
}

type FusedDispatcher struct {
	mu sync.Mutex

	batches  map[string]*fusedBatch
	inflight map[string]*fusedBatch

	// cache: text -> prompt -> answer
	cache map[string]map[string]string

	client *AnthropicSingleClient
	sep    string

	fuseDelay  time.Duration
	fuseGrace  time.Duration
	maxWaitCtx time.Duration

	limiter *rate.Limiter // nil => disabled

	debug bool

	multiEnabled   bool
	multiMaxTexts  int
	multiBatchWait time.Duration
	workCh         chan fusedWorkItem
	startOnce      sync.Once
}

func NewFusedDispatcher(client *AnthropicSingleClient, sep string) *FusedDispatcher {
	fd := &FusedDispatcher{
		batches:    make(map[string]*fusedBatch),
		inflight:   make(map[string]*fusedBatch),
		cache:      make(map[string]map[string]string),
		client:     client,
		sep:        sep,
		fuseDelay:  10 * time.Millisecond,
		fuseGrace:  0 * time.Millisecond,
		maxWaitCtx: 30 * time.Second,
		limiter:    nil,
		debug:      os.Getenv("QUACK_LLM_DEBUG") == "1",

		multiEnabled:   os.Getenv("QUACK_FUSED_MULTI") == "1",
		multiMaxTexts:  16,
		multiBatchWait: 5 * time.Millisecond,
		workCh:         make(chan fusedWorkItem, 4096),
	}

	if ms, ok := envInt("QUACK_FUSE_DELAY_MS"); ok && ms >= 0 {
		fd.fuseDelay = time.Duration(ms) * time.Millisecond
	}
	if ms, ok := envInt("QUACK_FUSE_GRACE_MS"); ok && ms >= 0 {
		fd.fuseGrace = time.Duration(ms) * time.Millisecond
	}

	if n, ok := envInt("QUACK_FUSED_MAX_TEXTS"); ok && n > 0 {
		fd.multiMaxTexts = n
	}
	if ms, ok := envInt("QUACK_FUSED_BATCH_MS"); ok && ms >= 0 {
		fd.multiBatchWait = time.Duration(ms) * time.Millisecond
	}

	// have sometimes to limited call :/
	if rps, ok := envInt("QUACK_FUSED_RPS"); ok && rps > 0 {
		fd.limiter = rate.NewLimiter(rate.Limit(rps), 1)
	}

	// Start worker if enabled
	if fd.multiEnabled {
		fd.startOnce.Do(func() { go fd.multiWorker() })
	}

	return fd
}

func envInt(key string) (int, bool) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, false
	}
	return n, true
}

/*
GetResult returns the answer for (text,prompt).
1. Cache
2. Reuse inflight
3. Add to collecting batch
*/
func (d *FusedDispatcher) GetResult(text, prompt string) (string, error) {
	for {
		// 1) cache
		d.mu.Lock()
		if mp := d.cache[text]; mp != nil {
			if ans := mp[prompt]; ans != "" {
				d.mu.Unlock()
				return ans, nil
			}
		}

		// 2) inflight
		if in := d.inflight[text]; in != nil {
			in.Lock()
			_, included := in.prompts[prompt]
			in.Unlock()

			d.mu.Unlock()

			if included {
				<-in.done
				in.Lock()
				ans := in.prompts[prompt]
				err := in.err
				in.Unlock()
				return ans, err
			}

			<-in.done
			continue
		}

		// 3) collecting
		b := d.batches[text]
		if b == nil {
			b = &fusedBatch{
				text:    text,
				prompts: make(map[string]string, 4),
				order:   make([]string, 0, 4),
				done:    make(chan struct{}),
			}
			d.batches[text] = b
			time.AfterFunc(d.fuseDelay, func() { d.flushText(text) })
		}

		b.Lock()
		if !b.frozen {
			if _, exists := b.prompts[prompt]; !exists {
				b.prompts[prompt] = ""
				b.order = append(b.order, prompt)
			}
			b.Unlock()
			d.mu.Unlock()

			<-b.done
			b.Lock()
			ans := b.prompts[prompt]
			err := b.err
			b.Unlock()
			return ans, err
		}
		b.Unlock()
		d.mu.Unlock()

		<-b.done
	}
}

func (d *FusedDispatcher) flushText(text string) {

	d.mu.Lock()
	b := d.batches[text]
	delete(d.batches, text)
	if b != nil {
		d.inflight[text] = b
	}
	d.mu.Unlock()

	if b == nil {
		return
	}

	if d.client == nil {
		setAll(b, "ERR:client_nil", d.debug, fmt.Errorf("single client is nil"))
		d.mu.Lock()
		delete(d.inflight, text)
		d.mu.Unlock()
		close(b.done)
		return
	}

	// Freeze
	b.Lock()
	b.frozen = true
	promptList := make([]string, 0, len(b.order))
	for _, p := range b.order {
		if _, ok := b.prompts[p]; ok {
			promptList = append(promptList, p)
		}
	}
	b.Unlock()

	if len(promptList) == 0 {
		d.mu.Lock()
		delete(d.inflight, text)
		d.mu.Unlock()
		close(b.done)
		return
	}

	fusedPrompt := strings.Join(promptList, d.sep)

	if d.multiEnabled {
		d.workCh <- fusedWorkItem{
			text:        text,
			promptList:  promptList,
			fusedPrompt: fusedPrompt,
			b:           b,
		}
		return
	}

	if d.debug {
		fmt.Println(fusedPrompt)
	}

	t0 := time.Now()
	raw, err := d.runSingleFusedRequest(text, fusedPrompt)
	RecordUpstreamRequest(time.Since(t0))

	d.finishSingle(text, b, promptList, raw, err)
}

func (d *FusedDispatcher) finishSingle(text string, b *fusedBatch, promptList []string, raw string, err error) {
	if err != nil {
		setAll(b, "ERR:"+err.Error(), d.debug, err)
		d.mu.Lock()
		delete(d.inflight, text)
		d.mu.Unlock()
		close(b.done)
		return
	}

	parts := splitFused(raw, d.sep)
	if len(parts) != len(promptList) {
		if d.debug {
			b.Lock()
			for p := range b.prompts {
				b.prompts[p] = "PARSE_MISMATCH want=" + strconv.Itoa(len(promptList)) +
					" got=" + strconv.Itoa(len(parts)) + " raw=" + raw
			}
			b.err = nil
			b.Unlock()
		} else {
			b.Lock()
			b.err = fmt.Errorf("got %d parts, want %d", len(parts), len(promptList))
			b.Unlock()
		}
		d.mu.Lock()
		delete(d.inflight, text)
		d.mu.Unlock()
		close(b.done)
		return
	}

	b.Lock()
	for i, p := range promptList {
		b.prompts[p] = parts[i]
	}
	b.err = nil
	b.Unlock()

	d.mu.Lock()
	if d.cache[text] == nil {
		d.cache[text] = make(map[string]string, len(promptList))
	}
	for p, a := range b.prompts {
		if a != "" {
			d.cache[text][p] = a
		}
	}
	delete(d.inflight, text)
	d.mu.Unlock()

	close(b.done)
}

func setAll(b *fusedBatch, msg string, debug bool, err error) {
	b.Lock()
	defer b.Unlock()
	if debug {
		for p := range b.prompts {
			b.prompts[p] = msg
		}
		b.err = nil
	} else {
		b.err = err
	}
}

func (d *FusedDispatcher) runSingleFusedRequest(text, fusedPrompt string) (string, error) {
	if d.limiter != nil {
		if err := d.limiter.Wait(context.Background()); err != nil {
			return "", err
		}
	}

	// Keep this short to reduce token overhead
	system := `Return machine-parseable output.`
	user := fmt.Sprintf(
		`TEXT:%s
INSTRUCTIONS:%s
Return answers ONLY, in order, separated by "%s". No newlines.`,
		text, fusedPrompt, d.sep,
	)

	reqCtx, cancel := context.WithTimeout(context.Background(), d.maxWaitCtx)
	defer cancel()

	return d.client.Run(reqCtx, "", system+"\n"+user)
}

func (d *FusedDispatcher) multiWorker() {
	for {
		// block until we have at least one job
		first := <-d.workCh
		batch := make([]fusedWorkItem, 0, d.multiMaxTexts)
		batch = append(batch, first)

		deadline := time.NewTimer(d.multiBatchWait)

	collect:
		for len(batch) < d.multiMaxTexts {
			select {
			case it := <-d.workCh:
				batch = append(batch, it)
			case <-deadline.C:
				break collect
			}
		}
		_ = deadline.Stop()

		d.runMultiBatch(batch)
	}
}

func (d *FusedDispatcher) runMultiBatch(items []fusedWorkItem) {
	if len(items) == 0 {
		return
	}

	wantN := len(items[0].promptList)
	for _, it := range items {
		if len(it.promptList) != wantN {
			// fallback
			for _, x := range items {
				t0 := time.Now()
				raw, err := d.runSingleFusedRequest(x.text, x.fusedPrompt)
				RecordUpstreamRequest(time.Since(t0))
				d.finishSingle(x.text, x.b, x.promptList, raw, err)
			}
			return
		}
	}

	if d.debug {
		fmt.Fprintf(os.Stderr, " batch size=%d prompts=%d\n", len(items), wantN)
	}

	// built single prompt:
	// exactly one line per TEXT, with ordering
	// line_i = ans1;ans2;... (no labels)
	var sb strings.Builder
	sb.Grow(256 * len(items))

	sb.WriteString("Return machine-parseable output.\n")
	sb.WriteString("For each item below, output EXACTLY ONE line.\n")
	sb.WriteString("Each line must contain exactly ")
	sb.WriteString(strconv.Itoa(wantN))
	sb.WriteString(" answers separated by '")
	sb.WriteString(d.sep)
	sb.WriteString("'. No extra text. No blank lines.\n\n")

	for i, it := range items {
		sb.WriteString("ITEM ")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString("\nTEXT:")
		sb.WriteString(it.text)
		sb.WriteString("\nINSTRUCTIONS:")
		sb.WriteString(it.fusedPrompt)
		sb.WriteString("\n\n")
	}

	reqCtx, cancel := context.WithTimeout(context.Background(), d.maxWaitCtx)
	defer cancel()

	t0 := time.Now()
	raw, err := d.client.Run(reqCtx, "", sb.String())
	RecordUpstreamRequest(time.Since(t0))

	if err != nil || strings.TrimSpace(raw) == "" {
		// fail
		for _, it := range items {
			setAll(it.b, "ERR:"+fmt.Sprint(err), d.debug, err)
			d.mu.Lock()
			delete(d.inflight, it.text)
			d.mu.Unlock()
			close(it.b.done)
		}
		return
	}

	lines := splitLines(raw)
	if len(lines) != len(items) {
		// fail all (debug shows raw)
		for _, it := range items {
			if d.debug {
				it.b.Lock()
				for p := range it.b.prompts {
					it.b.prompts[p] = "lines=" + strconv.Itoa(len(lines)) + " raw=" + raw
				}
				it.b.err = nil
				it.b.Unlock()
			} else {
				it.b.Lock()
				it.b.err = fmt.Errorf("got %d lines, want %d", len(lines), len(items))
				it.b.Unlock()
			}
			d.mu.Lock()
			delete(d.inflight, it.text)
			d.mu.Unlock()
			close(it.b.done)
		}
		return
	}

	// assign each line to each item
	for i, it := range items {
		parts := splitFused(lines[i], d.sep)
		if len(parts) != len(it.promptList) {

			if d.debug {
				it.b.Lock()
				for p := range it.b.prompts {
					it.b.prompts[p] = "got=" + strconv.Itoa(len(parts)) + " raw=" + lines[i]
				}
				it.b.err = nil
				it.b.Unlock()
			} else {
				it.b.Lock()
				it.b.err = fmt.Errorf("mgot %d, want %d", len(parts), len(it.promptList))
				it.b.Unlock()
			}
			d.mu.Lock()
			delete(d.inflight, it.text)
			d.mu.Unlock()
			close(it.b.done)
			continue
		}

		it.b.Lock()
		for j, p := range it.promptList {
			it.b.prompts[p] = parts[j]
		}
		it.b.err = nil
		it.b.Unlock()

		// cache + cleanup
		d.mu.Lock()
		if d.cache[it.text] == nil {
			d.cache[it.text] = make(map[string]string, len(it.promptList))
		}
		for p, a := range it.b.prompts {
			if a != "" {
				d.cache[it.text][p] = a
			}
		}
		delete(d.inflight, it.text)
		d.mu.Unlock()

		close(it.b.done)
	}
}

// splits one line into parts.
func splitFused(s, sep string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	raw := strings.Split(s, sep)
	out := make([]string, 0, len(raw))
	for _, x := range raw {
		x = strings.TrimSpace(x)
		if x == "" {
			continue
		}
		out = append(out, x)
	}
	return out
}

// splitLines trims and returns non-empty lines.
func splitLines(s string) []string {
	s = strings.ReplaceAll(s, "\r", "")
	raw := strings.Split(s, "\n")
	out := make([]string, 0, len(raw))
	for _, ln := range raw {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		out = append(out, ln)
	}
	return out
}
