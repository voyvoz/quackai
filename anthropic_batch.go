package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/liushuangls/go-anthropic/v2"
)

type AnthropicBatchClient struct {
	client    *anthropic.Client
	model     anthropic.Model
	maxTokens int
}

func NewAnthropicBatchClientFromEnv() (*AnthropicBatchClient, error) {
	key := os.Getenv("ANTHROPIC_API_KEY")

	if key == "" {
		return nil, fmt.Errorf("ANTHROPIC_API_KEY is not set")
	}

	c := anthropic.NewClient(
		key,
		anthropic.WithBetaVersion(anthropic.BetaMessageBatches20240924),
	)

	return &AnthropicBatchClient{
		client:    c,
		model:     anthropic.ModelClaude3Haiku20240307,
		maxTokens: 256,
	}, nil
}

func (a *AnthropicBatchClient) RunMessageBatch(
	ctx context.Context,
	reqs []anthropic.InnerRequests,
	pollEvery time.Duration,
	pollTimeout time.Duration,
) (map[string]string, error) {
	if len(reqs) == 0 {
		return map[string]string{}, nil
	}

	createResp, err := a.client.CreateBatch(ctx, anthropic.BatchRequest{Requests: reqs})
	if err != nil {
		fmt.Println(err)
		return nil, wrapAnthropicErr("CreateBatch", err)
	}
	batchID := createResp.Id

	ticker := time.NewTicker(pollEvery)
	defer ticker.Stop()

	timeout := time.NewTimer(pollTimeout)
	defer timeout.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-timeout.C:
			{
				fmt.Println("Time out")
				return nil, fmt.Errorf("batch %s timed out after %s", batchID, pollTimeout)
			}
		case <-ticker.C:
			status, err := a.client.RetrieveBatch(ctx, batchID)
			if err != nil {
				return nil, wrapAnthropicErr("RetrieveBatch", err)
			}

			switch status.ProcessingStatus {
			case "ended", "completed", "finished":
				goto DONE
			case "failed", "canceled":
				return nil, fmt.Errorf("batch %s ended with status=%s", batchID, status.ProcessingStatus)
			}
		}
	}

	// TODO: fix jump
DONE:
	resultsResp, err := a.client.RetrieveBatchResults(ctx, batchID)
	if err != nil {
		return nil, wrapAnthropicErr("RetrieveBatchResults", err)
	}

	out := make(map[string]string, len(reqs))

	for _, br := range resultsResp.Responses {
		customID := br.CustomId

		if br.Result.Type != anthropic.ResultTypeSucceeded {
			out[customID] = ""
			continue
		}

		msg := br.Result.Result

		answer := ""
		for _, block := range msg.Content {
			t := block.GetText()
			if t != "" {
				answer += t
			}
		}

		out[customID] = answer
	}

	return out, nil
}

func wrapAnthropicErr(where string, err error) error {
	var apiErr *anthropic.APIError
	if errors.As(err, &apiErr) {
		return fmt.Errorf("%s: anthropic error type=%s message=%s", where, apiErr.Type, apiErr.Message)
	}
	return fmt.Errorf("%s: %w", where, err)
}
