package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	quickjs "github.com/aperturerobotics/go-quickjs-wasi-reactor/wazero-quickjs"
	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/experimental"
)

const (
	resultMarker = "\x1eHANDOFF_WORKFLOW_RESULT:"
	wasmPageSize = uint64(64 << 10)
)

var (
	forbiddenModuleSyntax = regexp.MustCompile(`\b(?:import|export)\b`)
	vmCompilationCache    = wazero.NewCompilationCache()
)

type VMInput struct {
	Filename     string
	Source       string
	WorkflowJSON json.RawMessage
	ArgsJSON     json.RawMessage
	MaxMutations int
	Limits       VMLimits
}

type VMOutput struct {
	Mutations []core.Mutation `json:"mutations"`
	Rationale string          `json:"rationale,omitempty"`
	FuelUsed  uint64          `json:"-"`
}

type VMError struct {
	ErrorKind string
	Filename  string
	Detail    string
	Cause     error
}

func (e *VMError) Error() string {
	message := "workflow VM " + e.ErrorKind
	if e.Filename != "" {
		message += " in " + e.Filename
	}
	if e.Detail != "" {
		message += ": " + e.Detail
	}
	return message
}

func (e *VMError) Unwrap() error { return e.Cause }
func (e *VMError) Kind() string  { return e.ErrorKind }

type QuickJSRuntime struct{}

func (QuickJSRuntime) Identity() string {
	return "quickjs-ng-wasi-reactor-v0.15.1+wazero-v1.12.0"
}

func (QuickJSRuntime) Evaluate(ctx context.Context, input VMInput) (VMOutput, error) {
	if forbiddenModuleSyntax.MatchString(input.Source) {
		return VMOutput{}, &VMError{
			ErrorKind: "capability", Filename: input.Filename,
			Detail: "import/export syntax is disabled; workflow scripts receive only the handoff capability",
		}
	}
	meter := &fuelMeter{limit: input.Limits.InstructionFuel}
	ctx = context.WithValue(ctx, fuelMeterContextKey{}, meter)
	listenerCtx := experimental.WithFunctionListenerFactory(ctx, fuelListenerFactory{})
	pages := uint32((input.Limits.MemoryBytes + wasmPageSize - 1) / wasmPageSize)
	runtimeConfig := wazero.NewRuntimeConfig().
		WithCloseOnContextDone(true).
		WithMemoryLimitPages(pages).
		WithCompilationCache(vmCompilationCache)
	wasmRuntime := wazero.NewRuntimeWithConfig(listenerCtx, runtimeConfig)
	defer wasmRuntime.Close(context.Background())
	out := &boundedOutput{limit: input.Limits.MaxOutputBytes}
	config := wazero.NewModuleConfig().WithStartFunctions().WithStdout(out).WithStderr(out)
	qjs, err := quickjs.NewQuickJS(listenerCtx, wasmRuntime, config)
	if err != nil {
		return VMOutput{}, classifyVMError(input.Filename, input.Limits, meter, out, ctx, "initialize QuickJS", err)
	}
	defer qjs.Close(context.Background())
	memoryLimit := fmt.Sprintf("%d", input.Limits.MemoryBytes)
	stackLimit := fmt.Sprintf("%d", input.Limits.MaxStackBytes)
	if err = qjs.Init(listenerCtx, []string{"qjs", "--memory-limit", memoryLimit, "--stack-size", stackLimit}); err != nil {
		return VMOutput{}, classifyVMError(input.Filename, input.Limits, meter, out, ctx, "initialize JavaScript context", err)
	}
	source, err := buildWrapper(input)
	if err != nil {
		return VMOutput{}, &VMError{ErrorKind: "input", Filename: input.Filename, Detail: err.Error(), Cause: err}
	}
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, input.Limits.Timeout)
	defer cancelTimeout()
	evalCtx, cancelFuel := context.WithCancel(timeoutCtx)
	meter.arm(cancelFuel)
	defer cancelFuel()
	if err = qjs.EvalWithFilename(evalCtx, source, input.Filename, false); err == nil {
		err = qjs.RunLoop(evalCtx)
	}
	meter.disarm()
	if err != nil {
		return VMOutput{}, classifyVMError(input.Filename, input.Limits, meter, out, timeoutCtx, "evaluate script", err)
	}
	if out.overflowed() {
		return VMOutput{}, &VMError{
			ErrorKind: "output_limit", Filename: input.Filename,
			Detail: fmt.Sprintf("output exceeded %d bytes; reduce proposal or diagnostics", input.Limits.MaxOutputBytes),
		}
	}
	result, err := decodeVMResult(out.Bytes())
	if err != nil {
		kind := "result"
		if indicatesMemoryLimit(err.Error()) {
			kind = "memory_limit"
		}
		return VMOutput{}, &VMError{ErrorKind: kind, Filename: input.Filename, Detail: err.Error(), Cause: err}
	}
	result.FuelUsed = meter.used.Load()
	return result, nil
}

func buildWrapper(input VMInput) (string, error) {
	source, err := json.Marshal(input.Source)
	if err != nil {
		return "", err
	}
	filename, err := json.Marshal(input.Filename)
	if err != nil {
		return "", err
	}
	marker, _ := json.Marshal(resultMarker)
	return fmt.Sprintf(`
(function () {
  "use strict";
  const __emit = globalThis.print;
  const __parse = JSON.parse.bind(JSON);
  const __stringify = JSON.stringify.bind(JSON);
	const __Function = globalThis.Function;
  const __AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
	const __GeneratorFunction = Object.getPrototypeOf(function* () {}).constructor;
	const __AsyncGeneratorFunction = Object.getPrototypeOf(async function* () {}).constructor;
  const __source = %s;
  let __user;
  try {
    __user = new __AsyncFunction("handoff", __source + "\n//# sourceURL=" + %s);
  } catch (error) {
    throw new SyntaxError("workflow script could not be compiled: " + String(error && error.message || error));
  }
  const __disabled = function () { throw new Error("dynamic code generation is disabled in workflow scripts"); };
	try { Object.defineProperty(globalThis, "eval", {value: undefined, writable: false, configurable: false}); } catch (_) {}
	try { Object.defineProperty(globalThis, "Function", {value: undefined, writable: false, configurable: false}); } catch (_) {}
	try { Object.defineProperty(__Function.prototype, "constructor", {value: __disabled, writable: false, configurable: false}); } catch (_) {}
	try { Object.defineProperty(__AsyncFunction.prototype, "constructor", {value: __disabled, writable: false, configurable: false}); } catch (_) {}
	try { Object.defineProperty(__GeneratorFunction.prototype, "constructor", {value: __disabled, writable: false, configurable: false}); } catch (_) {}
	try { Object.defineProperty(__AsyncGeneratorFunction.prototype, "constructor", {value: __disabled, writable: false, configurable: false}); } catch (_) {}
	try { Object.defineProperty(globalThis, "Date", {value: undefined, writable: false, configurable: false}); } catch (_) {}
	try { Object.defineProperty(globalThis, "performance", {value: undefined, writable: false, configurable: false}); } catch (_) {}
	try { Object.defineProperty(Math, "random", {value: __disabled, writable: false, configurable: false}); } catch (_) {}
	try { Object.defineProperty(globalThis, "console", {value: undefined, writable: false, configurable: false}); } catch (_) {}
	try { Object.defineProperty(globalThis, "print", {value: undefined, writable: false, configurable: false}); } catch (_) {}

  const __clone = value => __parse(__stringify(value));
  const __deepFreeze = value => {
    if (value && typeof value === "object" && !Object.isFrozen(value)) {
      Object.freeze(value);
      for (const key of Object.keys(value)) __deepFreeze(value[key]);
    }
    return value;
  };
  const __workflow = __deepFreeze(__parse(%s));
  const __args = __deepFreeze(__parse(%s));
  let __proposal;
  const handoff = Object.freeze({
    workflow: __workflow,
    args: __args,
    node(id) {
      if (typeof id !== "string") throw new TypeError("handoff.node(id) requires a string id");
      return __workflow.nodes[id] || null;
    },
    evidence(nodeId) {
      if (nodeId !== undefined && typeof nodeId !== "string") throw new TypeError("handoff.evidence(nodeId) requires a string id");
      const values = (__workflow.evidence || []).filter(item => nodeId === undefined || item.node_id === nodeId);
      return __deepFreeze(__clone(values));
    },
    propose(mutations, rationale = "") {
      if (__proposal !== undefined) throw new Error("handoff.propose() may be called only once");
      if (!Array.isArray(mutations)) throw new TypeError("handoff.propose() requires a mutation array");
      if (mutations.length > %d) throw new RangeError("workflow mutation limit exceeded");
      if (typeof rationale !== "string") throw new TypeError("workflow proposal rationale must be a string");
      __proposal = __clone({mutations, rationale});
      return __deepFreeze(__clone(__proposal));
    }
  });

  (async function () {
    try {
      await __user(handoff);
      if (__proposal === undefined) throw new Error("workflow script completed without calling handoff.propose()");
      __emit(%s + __stringify({ok: true, output: __proposal}));
    } catch (error) {
      const name = String(error && error.name || "Error");
      const message = String(error && error.message || error);
      const stack = String(error && error.stack || "");
      __emit(%s + __stringify({ok: false, error: name + ": " + message + (stack ? "\n" + stack : "")}));
    }
  })();
})();`, source, filename, quotedJSON(input.WorkflowJSON), quotedJSON(input.ArgsJSON), input.MaxMutations, marker, marker), nil
}

func quotedJSON(raw json.RawMessage) string {
	b, _ := json.Marshal(string(raw))
	return string(b)
}

type vmWireResult struct {
	OK     bool      `json:"ok"`
	Output *VMOutput `json:"output,omitempty"`
	Error  string    `json:"error,omitempty"`
}

func decodeVMResult(output []byte) (VMOutput, error) {
	index := bytes.LastIndex(output, []byte(resultMarker))
	if index < 0 {
		return VMOutput{}, errors.New("script ended without a result; unresolved promises and direct process exits are not supported")
	}
	payload := bytes.TrimSpace(output[index+len(resultMarker):])
	var result vmWireResult
	dec := json.NewDecoder(bytes.NewReader(payload))
	if err := dec.Decode(&result); err != nil {
		return VMOutput{}, fmt.Errorf("decode script result: %w", err)
	}
	if !result.OK {
		if result.Error == "" {
			result.Error = "script failed without an error message"
		}
		return VMOutput{}, errors.New(result.Error)
	}
	if result.Output == nil {
		return VMOutput{}, errors.New("script returned no proposal")
	}
	return *result.Output, nil
}

func classifyVMError(filename string, limits VMLimits, meter *fuelMeter, output *boundedOutput, ctx context.Context, action string, err error) error {
	switch {
	case meter.exceeded.Load():
		return &VMError{
			ErrorKind: "instruction_limit", Filename: filename,
			Detail: fmt.Sprintf("execution consumed more than %d deterministic QuickJS/WASM function-entry ticks", limits.InstructionFuel), Cause: err,
		}
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return &VMError{
			ErrorKind: "timeout", Filename: filename,
			Detail: fmt.Sprintf("execution exceeded %s", limits.Timeout), Cause: err,
		}
	case output.overflowed():
		return &VMError{
			ErrorKind: "output_limit", Filename: filename,
			Detail: fmt.Sprintf("output exceeded %d bytes", limits.MaxOutputBytes), Cause: err,
		}
	case indicatesMemoryLimit(err.Error()):
		return &VMError{
			ErrorKind: "memory_limit", Filename: filename,
			Detail: fmt.Sprintf("execution exceeded the %d-byte memory limit", limits.MemoryBytes), Cause: err,
		}
	default:
		detail := action + ": " + err.Error()
		if text := strings.TrimSpace(string(output.Bytes())); text != "" {
			detail += "; " + text
		}
		return &VMError{ErrorKind: "runtime", Filename: filename, Detail: detail, Cause: err}
	}
}

func indicatesMemoryLimit(message string) bool {
	message = strings.ToLower(message)
	return strings.Contains(message, "out of memory") ||
		strings.Contains(message, "memory limit") ||
		strings.Contains(message, "memory out of bounds")
}

type fuelMeterContextKey struct{}

type fuelMeter struct {
	limit    uint64
	used     atomic.Uint64
	exceeded atomic.Bool
	armed    atomic.Bool
	mu       sync.Mutex
	cancel   context.CancelFunc
}

func (m *fuelMeter) arm(cancel context.CancelFunc) {
	m.used.Store(0)
	m.exceeded.Store(false)
	m.mu.Lock()
	m.cancel = cancel
	m.mu.Unlock()
	m.armed.Store(true)
}

func (m *fuelMeter) disarm() {
	m.armed.Store(false)
	m.mu.Lock()
	m.cancel = nil
	m.mu.Unlock()
}

func (m *fuelMeter) consume() {
	if !m.armed.Load() || m.limit == 0 || m.exceeded.Load() {
		return
	}
	if m.used.Add(1) <= m.limit {
		return
	}
	if !m.exceeded.CompareAndSwap(false, true) {
		return
	}
	m.mu.Lock()
	cancel := m.cancel
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

type fuelListenerFactory struct{}

func (fuelListenerFactory) NewFunctionListener(api.FunctionDefinition) experimental.FunctionListener {
	return experimental.FunctionListenerFunc(func(ctx context.Context, _ api.Module, _ api.FunctionDefinition, _ []uint64, _ experimental.StackIterator) {
		if meter, ok := ctx.Value(fuelMeterContextKey{}).(*fuelMeter); ok {
			meter.consume()
		}
	})
}

type boundedOutput struct {
	mu       sync.Mutex
	buffer   bytes.Buffer
	limit    int
	overflow bool
}

func (w *boundedOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	remaining := w.limit - w.buffer.Len()
	if remaining > 0 {
		write := len(p)
		if write > remaining {
			write = remaining
		}
		_, _ = w.buffer.Write(p[:write])
	}
	if len(p) > remaining {
		w.overflow = true
	}
	return len(p), nil
}

func (w *boundedOutput) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.buffer.Bytes()...)
}

func (w *boundedOutput) overflowed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.overflow
}

var _ ScriptRuntime = QuickJSRuntime{}
