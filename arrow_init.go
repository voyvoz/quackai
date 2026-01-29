package main

import (
	"fmt"
	"os"
)

var (
	dispatcher      *LLMDispatcher
	singleClient    *AnthropicSingleClient
	fusedDispatcher *FusedDispatcher
)

func init() {
	mode := os.Getenv("QUACK_LLM_MODE")

	dispatcher = nil
	singleClient = nil
	fusedDispatcher = nil

	switch mode {
	case "single":
		c, err := NewAnthropicSingleClientFromEnv()
		if err != nil {
			panic(fmt.Sprintf("Failed to init single client: %v", err))
		}
		singleClient = c

	case "fused":
		c, err := NewAnthropicSingleClientFromEnv()
		if err != nil {
			panic(fmt.Sprintf("Failed to init single client (for fused mode): %v", err))
		}
		fusedDispatcher = NewFusedDispatcher(c, ";")

	default:
		c, err := NewAnthropicBatchClientFromEnv()
		if err != nil {
			panic(fmt.Sprintf("Failed to init batch client: %v", err))
		}
		dispatcher = NewLLMDispatcher(c)
	}
}
