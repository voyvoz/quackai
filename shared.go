package main

// shared between duckdb extension and arrow standalone

import (
	"crypto/sha1"
	"encoding/hex"
)

func customID(text, prompt string) string {
	sum := sha1.Sum([]byte(text + "\x00" + prompt))
	return hex.EncodeToString(sum[:8])
}
