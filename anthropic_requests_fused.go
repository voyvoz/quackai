package main

import (
	"fmt"

	"github.com/liushuangls/go-anthropic/v2"
)

func buildAnthropicInnerRequestFused(customID, text, fusedPrompt, sep string, maxTokens int) anthropic.InnerRequests {
	user := fmt.Sprintf(
		`TEXT:
%s

INSTRUCTIONS:
You will receive multiple instructions separated by "%s".
Return ONLY the answers, in the SAME ORDER, separated by "%s".
Do not add numbering, quotes, extra text, or newlines. If an answer is empty, still keep separators.

INSTRUCTIONS_STRING:
%s`,
		text, sep, sep, fusedPrompt,
	)

	return anthropic.InnerRequests{
		CustomId: customID,
		Params: anthropic.MessagesRequest{
			Model:     anthropic.ModelClaude3Haiku20240307,
			MaxTokens: maxTokens,
			MultiSystem: anthropic.NewMultiSystemMessages(
				"you are a precise assistant",
				"output must be machine-parseable and strictly follow the delimiter rule",
			),
			Messages: []anthropic.Message{
				anthropic.NewUserTextMessage(user),
			},
		},
	}
}
