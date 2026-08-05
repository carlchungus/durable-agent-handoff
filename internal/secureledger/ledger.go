package secureledger

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
)

const (
	defaultMaxRecordBytes = 4 << 20
	defaultLockTimeout    = 10 * time.Second
	defaultLockRetry      = 10 * time.Millisecond
)

var namespacePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
var blobNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,190}$`)

type Options struct {
	Namespace      string
	ValidateID     func(string) error
	MaxRecordBytes int
}

type Ledger struct {
	rootPath       string
	namespace      string
	validateID     func(string) error
	maxRecordBytes int
	mu             sync.Mutex
	newFileLock    func(*os.File) fileLock
	lockTimeout    time.Duration
	lockRetry      time.Duration
	now            func() time.Time
	sleep          func(time.Duration)
	safetyHooks    safetyHooks
}

type safetyHooks struct {
	afterRootPrecheck  func()
	afterChildPrecheck func(string)
	afterFilePrecheck  func(string)
	afterLock          func()
	beforeValidation   func(string)
}

type fileLock interface {
	TryLock() (bool, error)
	Unlock() error
}

func Open(root string, options Options) (*Ledger, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("secure ledger root is required")
	}
	if !namespacePattern.MatchString(options.Namespace) {
		return nil, fmt.Errorf("invalid secure ledger namespace %q", options.Namespace)
	}
	if options.ValidateID == nil {
		return nil, errors.New("secure ledger id validator is required")
	}
	if options.MaxRecordBytes <= 0 {
		options.MaxRecordBytes = defaultMaxRecordBytes
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	ledger := &Ledger{
		rootPath:       root,
		namespace:      options.Namespace,
		validateID:     options.ValidateID,
		maxRecordBytes: options.MaxRecordBytes,
		newFileLock:    newPlatformFileLock,
		lockTimeout:    defaultLockTimeout,
		lockRetry:      defaultLockRetry,
		now:            time.Now,
		sleep:          time.Sleep,
	}
	rootHandle, err := ledger.openRoot()
	if err != nil {
		return nil, err
	}
	namespace, err := ledger.openChildRoot(rootHandle, ledger.namespace, true)
	_ = rootHandle.Close()
	if err != nil {
		return nil, err
	}
	_ = namespace.Close()
	return ledger, nil
}

func (l *Ledger) View(id string, visit func(sequence uint64, raw []byte) error) error {
	if err := l.validateID(id); err != nil {
		return err
	}
	root, err := l.openRecordRoot(id, false)
	if err != nil {
		return err
	}
	defer root.Close()
	file, err := l.openRegular(root, "events.jsonl", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = l.walk(file, true, visit)
	return err
}

func (l *Ledger) Update(id string, update func(*Txn) error) error {
	if err := l.validateID(id); err != nil {
		return err
	}
	if update == nil {
		return errors.New("secure ledger update callback is required")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	tx, err := l.acquire(id)
	if err != nil {
		return err
	}
	defer tx.release()
	return update(tx)
}

func (l *Ledger) IDs() ([]string, error) {
	namespace, err := l.openNamespaceRoot(false)
	if err != nil {
		return nil, err
	}
	defer namespace.Close()
	directory, err := namespace.Open(".")
	if err != nil {
		return nil, err
	}
	entries, err := directory.ReadDir(-1)
	_ = directory.Close()
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || l.validateID(entry.Name()) != nil {
			continue
		}
		ids = append(ids, entry.Name())
	}
	sort.Strings(ids)
	return ids, nil
}

type Txn struct {
	ledger *Ledger
	id     string
	root   *os.Root
	file   *os.File
	lock   fileLock
	owner  lockOwner
	once   sync.Once
}

type BlobChunk struct {
	Data  []byte
	Start int64
	End   int64
	Size  int64
}

func (tx *Txn) Replay(visit func(sequence uint64, raw []byte) error) error {
	if err := tx.validate("ledger replay"); err != nil {
		return err
	}
	file, err := tx.ledger.openRegular(tx.root, "events.jsonl", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = tx.ledger.walk(file, true, visit)
	return err
}

func (tx *Txn) Append(encode func(nextSequence uint64) ([]byte, error)) (_ uint64, err error) {
	if encode == nil {
		return 0, errors.New("secure ledger encoder is required")
	}
	if err = tx.validate("ledger open"); err != nil {
		return 0, err
	}
	file, err := tx.ledger.openRegular(tx.root, "events.jsonl", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
	}()
	if err = tx.validate("ledger repair"); err != nil {
		return 0, err
	}
	sequence, err := tx.ledger.repairTail(file)
	if err != nil {
		return 0, err
	}
	next := sequence + 1
	raw, err := encode(next)
	if err != nil {
		return 0, err
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || !json.Valid(raw) {
		return 0, errors.New("secure ledger event is not valid JSON")
	}
	parsed, err := parseSequence(raw)
	if err != nil {
		return 0, err
	}
	if parsed != next {
		return 0, fmt.Errorf("encoded event sequence %d does not match allocated sequence %d", parsed, next)
	}
	if len(raw)+1 > tx.ledger.maxRecordBytes {
		return 0, fmt.Errorf("secure ledger event exceeds %d bytes", tx.ledger.maxRecordBytes)
	}
	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		return 0, err
	}
	if err = tx.validate("ledger append"); err != nil {
		return 0, err
	}
	if _, err = file.Write(append(raw, '\n')); err == nil {
		err = file.Sync()
	}
	return next, err
}

func (tx *Txn) ReplaceSnapshot(data []byte) error {
	if err := tx.validate("snapshot temp"); err != nil {
		return err
	}
	tmp := fmt.Sprintf(".state.json.tmp-%d-%d", os.Getpid(), tx.ledger.now().UnixNano())
	file, err := tx.ledger.openRegular(tx.root, tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer tx.root.Remove(tmp)
	if err = tx.validate("snapshot write"); err != nil {
		_ = file.Close()
		return err
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = tx.validate("snapshot rename"); err != nil {
		return err
	}
	if err = tx.root.Rename(tmp, "state.json"); err != nil {
		return err
	}
	if directory, openErr := tx.root.Open("."); openErr == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}

func (tx *Txn) CreateBlob(name string) (*os.File, error) {
	if !validBlobName(name) {
		return nil, fmt.Errorf("invalid secure ledger blob name %q", name)
	}
	if err := tx.validate("blob create"); err != nil {
		return nil, err
	}
	return tx.ledger.openRegular(tx.root, name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
}

func (l *Ledger) ReadBlob(id, name string, after int64, maxBytes int) (BlobChunk, error) {
	if err := l.validateID(id); err != nil {
		return BlobChunk{}, err
	}
	if !validBlobName(name) {
		return BlobChunk{}, fmt.Errorf("invalid secure ledger blob name %q", name)
	}
	if after < 0 {
		return BlobChunk{}, errors.New("blob cursor cannot be negative")
	}
	if maxBytes <= 0 {
		maxBytes = 64 << 10
	}
	if maxBytes > 1<<20 {
		return BlobChunk{}, errors.New("blob read exceeds 1 MiB")
	}
	root, err := l.openRecordRoot(id, false)
	if err != nil {
		return BlobChunk{}, err
	}
	defer root.Close()
	file, err := l.openRegular(root, name, os.O_RDONLY, 0)
	if err != nil {
		return BlobChunk{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return BlobChunk{}, err
	}
	if after > info.Size() {
		return BlobChunk{}, fmt.Errorf("blob cursor %d exceeds size %d", after, info.Size())
	}
	data := make([]byte, min(int64(maxBytes), info.Size()-after))
	n, err := file.ReadAt(data, after)
	if err != nil && !errors.Is(err, io.EOF) {
		return BlobChunk{}, err
	}
	data = data[:n]
	return BlobChunk{Data: data, Start: after, End: after + int64(n), Size: info.Size()}, nil
}

func (l *Ledger) BlobPath(id, name string) (string, error) {
	if err := l.validateID(id); err != nil {
		return "", err
	}
	if !validBlobName(name) {
		return "", fmt.Errorf("invalid secure ledger blob name %q", name)
	}
	return fmt.Sprintf("%s%c%s%c%s%c%s", l.rootPath, os.PathSeparator, l.namespace, os.PathSeparator, id, os.PathSeparator, name), nil
}

func validBlobName(name string) bool {
	return blobNamePattern.MatchString(name) && name != "events.jsonl" && name != "state.json" && name != ".write.lock"
}

func (l *Ledger) openRoot() (*os.Root, error) {
	before, err := os.Lstat(l.rootPath)
	if err != nil {
		return nil, err
	}
	if !actualDirectory(before) {
		return nil, fmt.Errorf("secure ledger root %q is not an actual directory", l.rootPath)
	}
	if err = validateTrustedDirectory(before); err != nil {
		return nil, fmt.Errorf("unsafe secure ledger root %q: %w", l.rootPath, err)
	}
	beforeIdentity, err := identifyRootPath(l.rootPath)
	if err != nil {
		return nil, err
	}
	if l.safetyHooks.afterRootPrecheck != nil {
		l.safetyHooks.afterRootPrecheck()
	}
	root, err := os.OpenRoot(l.rootPath)
	if err != nil {
		return nil, err
	}
	after, pathErr := os.Lstat(l.rootPath)
	if pathErr != nil || !actualDirectory(after) {
		_ = root.Close()
		if pathErr != nil {
			return nil, pathErr
		}
		return nil, fmt.Errorf("secure ledger root %q is not an actual directory", l.rootPath)
	}
	if err = validateTrustedDirectory(after); err != nil {
		_ = root.Close()
		return nil, err
	}
	openedIdentity, openedErr := identifyRoot(root)
	afterIdentity, afterErr := identifyRootPath(l.rootPath)
	if openedErr != nil || afterErr != nil || !sameStorageIdentity(beforeIdentity, openedIdentity) || !sameStorageIdentity(beforeIdentity, afterIdentity) {
		_ = root.Close()
		if openedErr != nil {
			return nil, openedErr
		}
		if afterErr != nil {
			return nil, afterErr
		}
		return nil, fmt.Errorf("secure ledger root %q changed while opening", l.rootPath)
	}
	return root, nil
}

func (l *Ledger) openNamespaceRoot(create bool) (*os.Root, error) {
	root, err := l.openRoot()
	if err != nil {
		return nil, err
	}
	namespace, err := l.openChildRoot(root, l.namespace, create)
	_ = root.Close()
	return namespace, err
}

func (l *Ledger) openRecordRoot(id string, create bool) (*os.Root, error) {
	if err := l.validateID(id); err != nil {
		return nil, err
	}
	namespace, err := l.openNamespaceRoot(create)
	if err != nil {
		return nil, err
	}
	record, err := l.openChildRoot(namespace, id, create)
	_ = namespace.Close()
	return record, err
}

func (l *Ledger) openChildRoot(parent *os.Root, name string, create bool) (*os.Root, error) {
	if create {
		if err := parent.Mkdir(name, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	before, err := parent.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !actualDirectory(before) {
		return nil, fmt.Errorf("secure ledger component %q is not an actual directory", name)
	}
	if err = validateTrustedDirectory(before); err != nil {
		return nil, fmt.Errorf("unsafe secure ledger component %q: %w", name, err)
	}
	beforeIdentity, err := identifyChildRoot(parent, name)
	if err != nil {
		return nil, err
	}
	if l.safetyHooks.afterChildPrecheck != nil {
		l.safetyHooks.afterChildPrecheck(name)
	}
	child, err := parent.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	after, pathErr := parent.Lstat(name)
	if pathErr != nil || !actualDirectory(after) {
		_ = child.Close()
		if pathErr != nil {
			return nil, pathErr
		}
		return nil, fmt.Errorf("secure ledger component %q is not an actual directory", name)
	}
	if err = validateTrustedDirectory(after); err != nil {
		_ = child.Close()
		return nil, err
	}
	openedIdentity, openedErr := identifyRoot(child)
	afterIdentity, afterErr := identifyChildRoot(parent, name)
	if openedErr != nil || afterErr != nil || !sameStorageIdentity(beforeIdentity, openedIdentity) || !sameStorageIdentity(beforeIdentity, afterIdentity) {
		_ = child.Close()
		if openedErr != nil {
			return nil, openedErr
		}
		if afterErr != nil {
			return nil, afterErr
		}
		return nil, fmt.Errorf("secure ledger component %q changed while opening", name)
	}
	return child, nil
}

func actualDirectory(info os.FileInfo) bool { return info != nil && info.Mode().Type() == os.ModeDir }

func identifyRootPath(path string) (storageIdentity, error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return storageIdentity{}, err
	}
	defer root.Close()
	return identifyRoot(root)
}

func identifyChildRoot(parent *os.Root, name string) (storageIdentity, error) {
	root, err := parent.OpenRoot(name)
	if err != nil {
		return storageIdentity{}, err
	}
	defer root.Close()
	return identifyRoot(root)
}

func identifyRoot(root *os.Root) (storageIdentity, error) {
	file, err := root.Open(".")
	if err != nil {
		return storageIdentity{}, err
	}
	defer file.Close()
	return identifyStorageFile(file)
}

func identifyRegularPath(root *os.Root, name string) (storageIdentity, error) {
	file, err := root.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return storageIdentity{}, err
	}
	defer file.Close()
	if err = validateRegularFile(file); err != nil {
		return storageIdentity{}, err
	}
	return identifyStorageFile(file)
}

func (l *Ledger) openRegular(root *os.Root, name string, flag int, perm os.FileMode) (*os.File, error) {
	before, err := root.Lstat(name)
	existed := err == nil
	var beforeIdentity storageIdentity
	if err == nil {
		if !before.Mode().IsRegular() {
			return nil, fmt.Errorf("secure ledger file %q is not a regular file", name)
		}
		beforeIdentity, err = identifyRegularPath(root, name)
		if err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) || flag&os.O_CREATE == 0 {
		return nil, err
	}
	if l.safetyHooks.afterFilePrecheck != nil {
		l.safetyHooks.afterFilePrecheck(name)
	}
	file, err := root.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	after, pathErr := root.Lstat(name)
	safetyErr := validateRegularFile(file)
	if pathErr != nil || safetyErr != nil || !after.Mode().IsRegular() {
		_ = file.Close()
		if pathErr != nil {
			return nil, pathErr
		}
		if safetyErr != nil {
			return nil, fmt.Errorf("unsafe secure ledger file %q: %w", name, safetyErr)
		}
		return nil, fmt.Errorf("secure ledger file %q is not a regular file", name)
	}
	openedIdentity, openedErr := identifyStorageFile(file)
	afterIdentity, afterErr := identifyRegularPath(root, name)
	if openedErr != nil || afterErr != nil || !sameStorageIdentity(openedIdentity, afterIdentity) || existed && !sameStorageIdentity(beforeIdentity, openedIdentity) {
		_ = file.Close()
		if openedErr != nil {
			return nil, openedErr
		}
		if afterErr != nil {
			return nil, afterErr
		}
		return nil, fmt.Errorf("secure ledger file %q changed while opening", name)
	}
	return file, nil
}

func (l *Ledger) acquire(id string) (*Txn, error) {
	root, err := l.openRecordRoot(id, true)
	if err != nil {
		return nil, err
	}
	file, err := l.openRegular(root, ".write.lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	lock := l.newFileLock(file)
	deadline := l.now().Add(l.lockTimeout)
	first := true
	for {
		if !first && !l.now().Before(deadline) {
			_ = file.Close()
			_ = root.Close()
			return nil, fmt.Errorf("timed out waiting for secure ledger record %s lock", id)
		}
		first = false
		locked, lockErr := lock.TryLock()
		if lockErr != nil {
			_ = file.Close()
			_ = root.Close()
			return nil, fmt.Errorf("lock secure ledger record %s: %w", id, lockErr)
		}
		if locked {
			owner := lockOwner{PID: os.Getpid(), StartToken: runstate.ProcessStartToken(os.Getpid()), LeaseID: fmt.Sprintf("%d-%d", os.Getpid(), l.now().UnixNano()), State: lockActive, UpdatedAt: l.now().UTC()}
			tx := &Txn{ledger: l, id: id, root: root, file: file, lock: lock, owner: owner}
			if l.safetyHooks.afterLock != nil {
				l.safetyHooks.afterLock()
			}
			if err = tx.validate("acquire"); err != nil {
				_ = lock.Unlock()
				_ = file.Close()
				_ = root.Close()
				return nil, err
			}
			if err = writeLockOwner(file, owner); err != nil {
				_ = lock.Unlock()
				_ = file.Close()
				_ = root.Close()
				return nil, err
			}
			return tx, nil
		}
		remaining := deadline.Sub(l.now())
		if remaining <= 0 {
			continue
		}
		delay := l.lockRetry
		if delay > remaining {
			delay = remaining
		}
		l.sleep(delay)
	}
}

func (tx *Txn) validate(boundary string) error {
	if tx.ledger.safetyHooks.beforeValidation != nil {
		tx.ledger.safetyHooks.beforeValidation(boundary)
	}
	current, err := tx.ledger.openRecordRoot(tx.id, false)
	if err != nil {
		return fmt.Errorf("validate secure ledger at %s: %w", boundary, err)
	}
	currentIdentity, currentErr := identifyRoot(current)
	pinnedIdentity, pinnedErr := identifyRoot(tx.root)
	_ = current.Close()
	if currentErr != nil || pinnedErr != nil || !sameStorageIdentity(currentIdentity, pinnedIdentity) {
		if currentErr != nil {
			return currentErr
		}
		if pinnedErr != nil {
			return pinnedErr
		}
		return fmt.Errorf("secure ledger record identity changed at %s", boundary)
	}
	if err = validatePinnedRegular(tx.root, ".write.lock", tx.file); err != nil {
		return fmt.Errorf("secure ledger lock identity changed at %s: %w", boundary, err)
	}
	return nil
}

func (tx *Txn) release() {
	tx.once.Do(func() {
		if tx.validate("release") == nil {
			tx.owner.State = lockReleased
			tx.owner.UpdatedAt = tx.ledger.now().UTC()
			_ = writeLockOwner(tx.file, tx.owner)
		}
		_ = tx.lock.Unlock()
		_ = tx.file.Close()
		_ = tx.root.Close()
	})
}

func validatePinnedRegular(root *os.Root, name string, pinned *os.File) error {
	current, err := root.Lstat(name)
	if err != nil {
		return err
	}
	if !current.Mode().IsRegular() {
		return errors.New("public entry is not a regular file")
	}
	currentIdentity, err := identifyRegularPath(root, name)
	if err != nil {
		return err
	}
	pinnedIdentity, err := identifyStorageFile(pinned)
	if err != nil {
		return err
	}
	if !sameStorageIdentity(currentIdentity, pinnedIdentity) {
		return errors.New("public entry no longer names the pinned regular file")
	}
	return validateRegularFile(pinned)
}

const (
	lockActive   = "active"
	lockReleased = "released"
)

type lockOwner struct {
	PID        int       `json:"pid"`
	StartToken string    `json:"start_token,omitempty"`
	LeaseID    string    `json:"lease_id"`
	State      string    `json:"state"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func writeLockOwner(file *os.File, owner lockOwner) error {
	raw, err := json.Marshal(owner)
	if err != nil {
		return err
	}
	if err = file.Truncate(0); err != nil {
		return err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err = file.Write(append(raw, '\n')); err != nil {
		return err
	}
	return file.Sync()
}

func (l *Ledger) walk(file *os.File, toleratePartialTail bool, visit func(uint64, []byte) error) (uint64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	reader := bufio.NewReaderSize(file, 64<<10)
	var sequence uint64
	for {
		line, readErr := l.readLine(reader)
		if len(line) > 0 {
			complete := line[len(line)-1] == '\n'
			raw := bytes.TrimSpace(line)
			if len(raw) > 0 {
				next, parseErr := parseSequence(raw)
				if parseErr != nil && !complete && errors.Is(readErr, io.EOF) && toleratePartialTail {
					return sequence, nil
				}
				if parseErr != nil {
					return 0, parseErr
				}
				if next != sequence+1 {
					return 0, fmt.Errorf("event sequence %d followed %d", next, sequence)
				}
				if visit != nil {
					if err := visit(next, append([]byte(nil), raw...)); err != nil {
						return 0, err
					}
				}
				sequence = next
			}
		}
		if errors.Is(readErr, io.EOF) {
			return sequence, nil
		}
		if readErr != nil {
			return 0, readErr
		}
	}
}

func (l *Ledger) repairTail(file *os.File) (uint64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	reader := bufio.NewReaderSize(file, 64<<10)
	var sequence uint64
	var goodOffset int64
	for {
		line, readErr := l.readLine(reader)
		if len(line) > 0 {
			complete := line[len(line)-1] == '\n'
			next, parseErr := parseSequence(bytes.TrimSpace(line))
			if parseErr != nil && !complete && errors.Is(readErr, io.EOF) {
				truncateErr := file.Truncate(goodOffset)
				if truncateErr == nil {
					truncateErr = file.Sync()
				}
				return sequence, truncateErr
			}
			if parseErr != nil {
				return 0, parseErr
			}
			if next != sequence+1 {
				return 0, fmt.Errorf("event sequence %d followed %d", next, sequence)
			}
			sequence = next
			goodOffset += int64(len(line))
			if !complete && errors.Is(readErr, io.EOF) {
				_, writeErr := file.WriteAt([]byte{'\n'}, goodOffset)
				if writeErr == nil {
					writeErr = file.Sync()
				}
				return sequence, writeErr
			}
		}
		if errors.Is(readErr, io.EOF) {
			return sequence, nil
		}
		if readErr != nil {
			return 0, readErr
		}
	}
}

func (l *Ledger) readLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		line = append(line, part...)
		if len(line) > l.maxRecordBytes {
			return nil, fmt.Errorf("secure ledger event exceeds %d bytes", l.maxRecordBytes)
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, err
	}
}

func parseSequence(raw []byte) (uint64, error) {
	var envelope struct {
		Sequence uint64 `json:"sequence"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return 0, err
	}
	if envelope.Sequence == 0 {
		return 0, errors.New("secure ledger event sequence must be positive")
	}
	return envelope.Sequence, nil
}
