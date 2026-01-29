package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"

	"github.com/liushuangls/go-anthropic/v2"
)

// creates a valid id, considering claude limitations
func makeCustomID(row int, text, prompt string) string {
	sum := sha1.Sum([]byte(text + "\x00" + prompt))
	hash := hex.EncodeToString(sum[:6])
	return fmt.Sprintf("r%d_%s", row, hash)
}

func buildAnthropicInnerRequest(customID, text, prompt string, maxTokens int) anthropic.InnerRequests {
	user := fmt.Sprintf(
		"TEXT:\n%s\n\nINSTRUCTION:\n%s\n\nReturn only the answer text.",
		text, prompt,
	)

	return anthropic.InnerRequests{
		CustomId: customID,
		Params: anthropic.MessagesRequest{
			Model:     anthropic.ModelClaude3Haiku20240307,
			MaxTokens: maxTokens,
			MultiSystem: anthropic.NewMultiSystemMessages(
				"you are a precise assistant",
				"follow the instruction and respond with only the answer",
			),
			Messages: []anthropic.Message{
				anthropic.NewUserTextMessage(user),
			},
		},
	}
}
