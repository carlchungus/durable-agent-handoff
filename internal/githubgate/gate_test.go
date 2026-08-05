package githubgate

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type fakeRunner struct {
	responses [][]byte
	errs      []error
	calls     [][]string
}

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.calls = append(f.calls, append([]string{name}, args...))
	i := len(f.calls) - 1
	var err error
	if i < len(f.errs) {
		err = f.errs[i]
	}
	return f.responses[i], err
}

func TestMergeUsesExactGateAndHeadSHA(t *testing.T) {
	f := &fakeRunner{responses: [][]byte{[]byte(`{"number":7,"url":"https://example/pr/7","headRefOid":"abc123","mergeStateStatus":"CLEAN","statusCheckRollup":[{"name":"verify (24)","status":"COMPLETED","conclusion":"SUCCESS"}]}`), []byte("ok")}}
	p, err := Merge(context.Background(), f, "o/r", "7", []string{"verify (24)"}, "squash")
	if err != nil {
		t.Fatal(err)
	}
	if p.HeadOID != "abc123" {
		t.Fatal(p.HeadOID)
	}
	want := []string{"gh", "pr", "merge", "7", "--repo", "o/r", "--match-head-commit", "abc123", "--squash"}
	if !reflect.DeepEqual(f.calls[1], want) {
		t.Fatalf("call=%v", f.calls[1])
	}
}
func TestVerifyRejectsMissingOrPendingGate(t *testing.T) {
	p := PR{HeadOID: "abc", MergeState: "CLEAN", Checks: []Check{{Name: "test", Status: "IN_PROGRESS"}}}
	if err := Verify(p, []string{"test"}); err == nil {
		t.Fatal("expected pending gate rejection")
	}
	if err := Verify(p, []string{"other"}); err == nil {
		t.Fatal("expected missing gate rejection")
	}
}
func TestInspectPreservesCommandFailure(t *testing.T) {
	f := &fakeRunner{responses: [][]byte{[]byte("nope")}, errs: []error{errors.New("exit")}}
	if _, err := Inspect(context.Background(), f, "o/r", "1"); err == nil {
		t.Fatal("expected error")
	}
}
