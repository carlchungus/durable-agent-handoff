package secureledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func testLedger(t *testing.T) *Ledger {
	t.Helper()
	ledger, err := Open(t.TempDir(), Options{
		Namespace: "records",
		ValidateID: func(id string) error {
			if len(id) < 4 || id[:4] != "rec_" {
				return errors.New("invalid record id")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ledger
}

func TestAppendReplayAndSnapshotUseOneDurableRecord(t *testing.T) {
	ledger := testLedger(t)
	if err := ledger.Update("rec_alpha", func(tx *Txn) error {
		for _, kind := range []string{"created", "updated"} {
			kind := kind
			if _, err := tx.Append(func(next uint64) ([]byte, error) {
				return json.Marshal(map[string]any{"sequence": next, "kind": kind})
			}); err != nil {
				return err
			}
		}
		return tx.ReplaceSnapshot([]byte("{\"state\":\"updated\"}\n"))
	}); err != nil {
		t.Fatal(err)
	}

	var kinds []string
	if err := ledger.View("rec_alpha", func(sequence uint64, raw []byte) error {
		var event struct {
			Sequence uint64 `json:"sequence"`
			Kind     string `json:"kind"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return err
		}
		if event.Sequence != sequence {
			t.Fatalf("event sequence=%d callback=%d", event.Sequence, sequence)
		}
		kinds = append(kinds, event.Kind)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(kinds) != "[created updated]" {
		t.Fatalf("kinds=%v", kinds)
	}
	if got, err := os.ReadFile(filepath.Join(ledger.rootPath, "records", "rec_alpha", "state.json")); err != nil || string(got) != "{\"state\":\"updated\"}\n" {
		t.Fatalf("snapshot=%q err=%v", got, err)
	}
}

func TestAppendRepairsOnlyATornTail(t *testing.T) {
	ledger := testLedger(t)
	if err := ledger.Update("rec_torn", func(tx *Txn) error {
		_, err := tx.Append(func(next uint64) ([]byte, error) {
			return []byte(fmt.Sprintf(`{"sequence":%d,"kind":"created"}`, next)), nil
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(ledger.rootPath, "records", "rec_torn", "events.jsonl")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteString(`{"sequence":2`); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = ledger.Update("rec_torn", func(tx *Txn) error {
		sequence, appendErr := tx.Append(func(next uint64) ([]byte, error) {
			return []byte(fmt.Sprintf(`{"sequence":%d,"kind":"repaired"}`, next)), nil
		})
		if sequence != 2 {
			t.Fatalf("sequence=%d", sequence)
		}
		return appendErr
	}); err != nil {
		t.Fatal(err)
	}
}

func TestUnsafeIdentifiersAndRedirectsFailClosed(t *testing.T) {
	ledger := testLedger(t)
	for _, id := range []string{"../outside", "bad", "rec_ok/../../outside"} {
		if err := ledger.Update(id, func(*Txn) error { return nil }); err == nil {
			t.Fatalf("accepted unsafe id %q", id)
		}
	}
	outside := t.TempDir()
	link := filepath.Join(ledger.rootPath, "records", "rec_link")
	if err := os.Symlink(outside, link); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink requires Windows Developer Mode: %v", err)
		}
		t.Fatal(err)
	}
	if err := ledger.Update("rec_link", func(*Txn) error { return nil }); err == nil {
		t.Fatal("accepted a linked record directory")
	}
}

func TestSeparateLedgersSerializeWriters(t *testing.T) {
	root := t.TempDir()
	options := Options{Namespace: "records", ValidateID: func(string) error { return nil }}
	left, _ := Open(root, options)
	right, _ := Open(root, options)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- left.Update("rec_lock", func(*Txn) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	right.lockTimeout = 25 * time.Millisecond
	right.lockRetry = time.Millisecond
	if err := right.Update("rec_lock", func(*Txn) error { return nil }); err == nil {
		t.Fatal("second writer acquired a held kernel lock")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := right.Update("rec_lock", func(*Txn) error { return nil }); err != nil {
		t.Fatalf("released lock not reusable: %v", err)
	}
}

func TestConcurrentAppendsKeepEverySequence(t *testing.T) {
	root := t.TempDir()
	options := Options{Namespace: "records", ValidateID: func(string) error { return nil }}
	left, _ := Open(root, options)
	right, _ := Open(root, options)
	if err := left.Update("rec_concurrent", func(*Txn) error { return nil }); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			writer := left
			if i%2 == 1 {
				writer = right
			}
			if err := writer.Update("rec_concurrent", func(tx *Txn) error {
				_, err := tx.Append(func(next uint64) ([]byte, error) {
					return []byte(fmt.Sprintf(`{"sequence":%d,"value":%d}`, next, i)), nil
				})
				return err
			}); err != nil {
				t.Errorf("append %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	count := 0
	if err := left.View("rec_concurrent", func(sequence uint64, _ []byte) error {
		count++
		if sequence != uint64(count) {
			t.Fatalf("sequence=%d count=%d", sequence, count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 20 {
		t.Fatalf("events=%d", count)
	}
}

func TestPreOpenIdentitySwapsFailClosed(t *testing.T) {
	t.Run("root", func(t *testing.T) {
		base := t.TempDir()
		root := filepath.Join(base, "state")
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		ledger, err := Open(root, Options{Namespace: "records", ValidateID: func(string) error { return nil }})
		if err != nil {
			t.Fatal(err)
		}
		var once sync.Once
		ledger.safetyHooks.afterRootPrecheck = func() {
			once.Do(func() {
				if err := os.Rename(root, filepath.Join(base, "old-state")); err != nil {
					t.Error(err)
					return
				}
				if err := os.Mkdir(root, 0o700); err != nil {
					t.Error(err)
				}
			})
		}
		if _, err := ledger.openRoot(); err == nil {
			t.Fatal("accepted root replacement between precheck and open")
		}
	})

	t.Run("record", func(t *testing.T) {
		ledger := testLedger(t)
		record := filepath.Join(ledger.rootPath, "records", "rec_swap")
		if err := os.Mkdir(record, 0o700); err != nil {
			t.Fatal(err)
		}
		old := record + "-old"
		var once sync.Once
		ledger.safetyHooks.afterChildPrecheck = func(name string) {
			if name != "rec_swap" {
				return
			}
			once.Do(func() {
				if err := os.Rename(record, old); err != nil {
					t.Error(err)
					return
				}
				if err := os.Mkdir(record, 0o700); err != nil {
					t.Error(err)
				}
			})
		}
		if err := ledger.Update("rec_swap", func(*Txn) error { return nil }); err == nil {
			t.Fatal("accepted record replacement between precheck and open")
		}
	})
}

func TestPostLockReplacementFailsBeforeLedgerWrite(t *testing.T) {
	ledger := testLedger(t)
	record := filepath.Join(ledger.rootPath, "records", "rec_replace")
	old := record + "-old"
	var once sync.Once
	ledger.safetyHooks.afterLock = func() {
		once.Do(func() {
			if err := os.Rename(record, old); err != nil {
				t.Error(err)
				return
			}
			if err := os.Mkdir(record, 0o700); err != nil {
				t.Error(err)
			}
		})
	}
	err := ledger.Update("rec_replace", func(tx *Txn) error {
		_, appendErr := tx.Append(func(next uint64) ([]byte, error) {
			return []byte(fmt.Sprintf(`{"sequence":%d}`, next)), nil
		})
		return appendErr
	})
	if err == nil {
		t.Fatal("accepted record replacement after locking")
	}
	if _, statErr := os.Stat(filepath.Join(record, "events.jsonl")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("replacement record was mutated: %v", statErr)
	}
}

func TestLockAndLedgerCannotHardLinkOutsideRecord(t *testing.T) {
	for _, name := range []string{".write.lock", "events.jsonl"} {
		t.Run(name, func(t *testing.T) {
			ledger := testLedger(t)
			record := filepath.Join(ledger.rootPath, "records", "rec_hardlink")
			if err := os.Mkdir(record, 0o700); err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(t.TempDir(), "sentinel")
			if err := os.WriteFile(sentinel, []byte("unchanged"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(sentinel, filepath.Join(record, name)); err != nil {
				t.Skipf("hard links unavailable: %v", err)
			}
			if err := ledger.Update("rec_hardlink", func(tx *Txn) error {
				_, appendErr := tx.Append(func(next uint64) ([]byte, error) {
					return []byte(fmt.Sprintf(`{"sequence":%d}`, next)), nil
				})
				return appendErr
			}); err == nil {
				t.Fatalf("accepted hard-linked %s", name)
			}
			if got, err := os.ReadFile(sentinel); err != nil || string(got) != "unchanged" {
				t.Fatalf("sentinel=%q err=%v", got, err)
			}
		})
	}
}

func TestSnapshotReplacesDestinationSymlinkButRejectsPlantedTemporaryLink(t *testing.T) {
	ledger := testLedger(t)
	if err := ledger.Update("rec_snapshot", func(tx *Txn) error {
		_, err := tx.Append(func(next uint64) ([]byte, error) { return []byte(fmt.Sprintf(`{"sequence":%d}`, next)), nil })
		return err
	}); err != nil {
		t.Fatal(err)
	}
	record := filepath.Join(ledger.rootPath, "records", "rec_snapshot")
	sentinel := filepath.Join(t.TempDir(), "sentinel")
	if err := os.WriteFile(sentinel, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sentinel, filepath.Join(record, "state.json")); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink requires Windows Developer Mode: %v", err)
		}
		t.Fatal(err)
	}
	if err := ledger.Update("rec_snapshot", func(tx *Txn) error { return tx.ReplaceSnapshot([]byte("new\n")) }); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(sentinel); string(got) != "unchanged" {
		t.Fatalf("snapshot followed destination link: %q", got)
	}

	fixed := time.Unix(123, 456)
	ledger.now = func() time.Time { return fixed }
	tmp := filepath.Join(record, fmt.Sprintf(".state.json.tmp-%d-%d", os.Getpid(), fixed.UnixNano()))
	if err := os.Symlink(sentinel, tmp); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Update("rec_snapshot", func(tx *Txn) error { return tx.ReplaceSnapshot([]byte("bad\n")) }); err == nil {
		t.Fatal("accepted planted snapshot temporary link")
	}
}

func TestAppendRejectsEventFileReplacementAfterOpen(t *testing.T) {
	ledger := testLedger(t)
	appendOne := func() error {
		return ledger.Update("rec_event_swap", func(tx *Txn) error {
			_, err := tx.Append(func(next uint64) ([]byte, error) {
				return []byte(fmt.Sprintf(`{"sequence":%d}`, next)), nil
			})
			return err
		})
	}
	if err := appendOne(); err != nil {
		t.Fatal(err)
	}
	record := filepath.Join(ledger.rootPath, "records", "rec_event_swap")
	swapped := false
	ledger.safetyHooks.beforeValidation = func(boundary string) {
		if boundary != "ledger append" || swapped {
			return
		}
		swapped = true
		if err := os.Rename(filepath.Join(record, "events.jsonl"), filepath.Join(record, "displaced.jsonl")); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(record, "events.jsonl"), []byte("replacement\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := appendOne(); err == nil {
		t.Fatal("accepted event file replacement after opening the pinned file")
	}
	if got, err := os.ReadFile(filepath.Join(record, "events.jsonl")); err != nil || string(got) != "replacement\n" {
		t.Fatalf("replacement was mutated: %q err=%v", got, err)
	}
}

func TestSnapshotRejectsTemporaryFileReplacementBeforeRename(t *testing.T) {
	ledger := testLedger(t)
	if err := ledger.Update("rec_snapshot_swap", func(tx *Txn) error { return tx.ReplaceSnapshot([]byte("old\n")) }); err != nil {
		t.Fatal(err)
	}
	record := filepath.Join(ledger.rootPath, "records", "rec_snapshot_swap")
	swapped := false
	ledger.safetyHooks.beforeValidation = func(boundary string) {
		if boundary != "snapshot rename" || swapped {
			return
		}
		swapped = true
		matches, err := filepath.Glob(filepath.Join(record, ".state.json.tmp-*"))
		if err != nil || len(matches) != 1 {
			t.Fatalf("temporary snapshots=%v err=%v", matches, err)
		}
		if err = os.Rename(matches[0], matches[0]+".displaced"); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(matches[0], []byte("attacker\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := ledger.Update("rec_snapshot_swap", func(tx *Txn) error { return tx.ReplaceSnapshot([]byte("new\n")) }); err == nil {
		t.Fatal("accepted snapshot temporary-file replacement before rename")
	}
	if got, err := os.ReadFile(filepath.Join(record, "state.json")); err != nil || string(got) != "old\n" {
		t.Fatalf("canonical snapshot changed: %q err=%v", got, err)
	}
}

func TestRejectsGroupWritableRootOnUnix(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission contract")
	}
	root := t.TempDir()
	if err := os.Chmod(root, 0o770); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, Options{Namespace: "records", ValidateID: func(string) error { return nil }}); err == nil {
		t.Fatal("accepted group-writable root")
	}
}

func TestBlobCursorReadsStableRecordLocalOutput(t *testing.T) {
	ledger := testLedger(t)
	var output *os.File
	if err := ledger.Update("rec_output", func(tx *Txn) error {
		var err error
		output, err = tx.CreateBlob("attempt_1_stdout.log")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := output.WriteString("one\ntwo\n"); err == nil {
		err = output.Sync()
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	first, err := ledger.ReadBlob("rec_output", "attempt_1_stdout.log", 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Data) != "one\n" || first.Start != 0 || first.End != 4 || first.Size != 8 {
		t.Fatalf("first=%+v", first)
	}
	second, err := ledger.ReadBlob("rec_output", "attempt_1_stdout.log", first.End, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(second.Data) != "two\n" || second.Start != 4 || second.End != 8 {
		t.Fatalf("second=%+v", second)
	}
	if _, err := ledger.ReadBlob("rec_output", "../outside", 0, 10); err == nil {
		t.Fatal("accepted unsafe blob name")
	}
}
