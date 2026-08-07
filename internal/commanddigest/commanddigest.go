// Package commanddigest provides the minimal canonical digest used to bind an
// executor launch to its durable command. It has no workflow state authority.
package commanddigest

import (
	"crypto/sha256"
	"encoding/hex"
)

func CommandDigest(executable string, args []string) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(executable))
	for _, arg := range args {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(arg))
	}
	return hex.EncodeToString(hash.Sum(nil))
}
