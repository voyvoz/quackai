package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/liushuangls/go-anthropic/v2"
)

type AnthropicSingleClient struct {
	client    *anthropic.Client
	model     anthropic.Model
	maxTokens int
	timeout   time.Duration
}

func NewAnthropicSingleClientFromEnv() (*AnthropicSingleClient, error) {
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("ANTHROPIC_API_KEY is not set")
	}

	c := anthropic.NewClient(key)

	return &AnthropicSingleClient{
		client:    c,
		model:     anthropic.ModelClaude3Haiku20240307,
		maxTokens: 256,
		timeout:   20 * time.Second,
	}, nil
}

func (a *AnthropicSingleClient) Run(ctx context.Context, text, prompt string) (string, error) {
	reqCtx := ctx

	var cancel context.CancelFunc
	if a.timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, a.timeout)
		defer cancel()
	}

	user := fmt.Sprintf(
		"TEXT:\n%s\n\nINSTRUCTION:\n%s\n\nReturn only the answer text.",
		text, prompt,
	)

	resp, err := a.client.CreateMessages(reqCtx, anthropic.MessagesRequest{
		Model: a.model,
		MultiSystem: anthropic.NewMultiSystemMessages(
			"you are a precise assistant",
			"follow the instruction and respond with only the answer",
		),
		Messages: []anthropic.Message{
			anthropic.NewUserTextMessage(user),
		},
		MaxTokens: a.maxTokens,
	})

	if err != nil {
		return "", wrapAnthropicErr("CreateMessages", err)
	}

	out := ""
	for _, block := range resp.Content {
		t := block.GetText()
		if t != "" {
			out += t
		}
	}
	return out, nil
}
