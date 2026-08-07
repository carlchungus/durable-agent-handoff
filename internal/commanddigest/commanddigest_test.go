package commanddigest

import "testing"

func TestCommandDigestIncludesArgumentBoundaries(t *testing.T) {
	if CommandDigest("tool", []string{"ab", "c"}) == CommandDigest("tool", []string{"a", "bc"}) {
		t.Fatal("argument boundaries must affect the digest")
	}
}
