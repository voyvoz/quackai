package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

func callBundledWithRetry(
	parent context.Context,
	text string,
	prompts []string,
	retries int,
	timeout time.Duration,
	backoff time.Duration,
) (map[string]string, error) {
	var lastErr error

	for attempt := 0; attempt <= retries; attempt++ {
		ctx, cancel := context.WithTimeout(parent, timeout)
		res, err := callBundledLLMAPI(ctx, text, prompts)
		cancel()

		if err == nil {
			return res, nil
		}
		lastErr = err

		if attempt < retries {
			sleepTime := backoff << attempt
			if sleepTime > 500*time.Millisecond {
				sleepTime = 500 * time.Millisecond
			}
			time.Sleep(sleepTime)
		}
	}

	return nil, lastErr
}

// Mock
func callBundledLLMAPI(ctx context.Context, text string, prompts []string) (map[string]string, error) {
	select {
	case <-time.After(40 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	h := sha1.Sum([]byte(text + fmt.Sprint(prompts)))
	if (h[0] & 0x0F) == 0 {
		return nil, errors.New("api: error")
	}

	res := make(map[string]string, len(prompts))
	id := hex.EncodeToString(h[:4])
	for _, p := range prompts {
		res[p] = fmt.Sprintf("ai_llm[%s]: %s | prompt=%s", id, text, p)
	}
	return res, nil
}
