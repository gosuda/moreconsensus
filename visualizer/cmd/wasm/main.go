//go:build js && wasm

package main

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"syscall/js"

	"gosuda.org/moreconsensus/visualizer/internal/sim"
)

type request struct {
	Op       string     `json:"op"`
	Scenario string     `json:"scenario"`
	Size     int        `json:"size"`
	Index    int        `json:"index"`
	Action   sim.Action `json:"action"`
}

type response struct {
	OK         bool                  `json:"ok"`
	Scenarios  []sim.ScenarioMeta    `json:"scenarios,omitempty"`
	Throughput []sim.ThroughputPoint `json:"throughput,omitempty"`
	Trace      *sim.ScenarioTrace    `json:"trace,omitempty"`
	Frame      *sim.Frame            `json:"frame,omitempty"`
	CanBack    *bool                 `json:"canBack,omitempty"`
	CanForward *bool                 `json:"canForward,omitempty"`
	Error      *errorResponse        `json:"error,omitempty"`
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type bridge struct {
	lab           *sim.Session
	labCursor     int
	labLength     int
	finance       *sim.Session
	financeCursor int
	financeLength int
	throughput    []sim.ThroughputPoint
}

var dispatchFunction js.Func

func main() {
	state := &bridge{}
	dispatchFunction = js.FuncOf(state.dispatch)
	js.Global().Set("epaxosVizDispatch", dispatchFunction)
	document := js.Global().Get("document")
	event := js.Global().Get("CustomEvent").New("epaxos-viz-ready")
	document.Call("dispatchEvent", event)
	select {}
}

func (b *bridge) dispatch(_ js.Value, args []js.Value) (result any) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = marshalResponse(failure(sim.CodeInternal, "The EPaxos core rejected this request."))
		}
	}()
	if len(args) != 1 || args[0].Type() != js.TypeString {
		return marshalResponse(failure(sim.CodeInvalidRequest, "Pass one JSON request string."))
	}
	decoded, err := decodeRequest(args[0].String())
	if err != nil {
		return marshalResponse(failure(sim.CodeInvalidRequest, "The request is not valid JSON."))
	}
	return marshalResponse(b.handle(decoded))
}

func decodeRequest(raw string) (request, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var decoded request
	if err := decoder.Decode(&decoded); err != nil {
		return request{}, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return request{}, errors.New("request has trailing JSON")
	}
	return decoded, nil
}

func (b *bridge) handle(req request) response {
	switch req.Op {
	case "catalog":
		if req.Scenario != "" || req.Size != 0 || req.Index != 0 || req.Action != (sim.Action{}) {
			return failure(sim.CodeInvalidRequest, "Catalog does not accept additional fields.")
		}
		return response{OK: true, Scenarios: sim.Catalog()}
	case "performance":
		if req.Scenario != "" || req.Size != 0 || req.Index != 0 || req.Action != (sim.Action{}) {
			return failure(sim.CodeInvalidRequest, "Performance does not accept additional fields.")
		}
		if b.throughput == nil {
			profile, err := sim.FaultThroughputProfile()
			if err != nil {
				return fromError(err)
			}
			b.throughput = profile
		}
		return response{OK: true, Throughput: b.throughput}
	case "scenario":
		if req.Scenario == "" || req.Size != 0 || req.Index != 0 || req.Action != (sim.Action{}) {
			return failure(sim.CodeInvalidRequest, "Choose one guided scenario.")
		}
		trace, err := sim.BuildScenario(req.Scenario)
		if err != nil {
			return fromError(err)
		}
		return response{OK: true, Trace: &trace}
	case "lab.reset":
		if req.Scenario != "" || req.Index != 0 || req.Action != (sim.Action{}) || (req.Size != 3 && req.Size != 5) {
			return failure(sim.CodeInvalidRequest, "Lab reset accepts only a cluster size.")
		}
		session, err := sim.NewSession(req.Size)
		if err != nil {
			return fromError(err)
		}
		frame, err := session.Seek(0)
		if err != nil {
			return fromError(err)
		}
		b.lab = session
		b.labCursor = 0
		b.labLength = 0
		return frameResponse(frame, false, false)
	case "lab.action":
		if req.Scenario != "" || req.Size != 0 || req.Index != 0 || b.lab == nil {
			return failure(sim.CodeInvalidRequest, "Reset the lab before dispatching an action.")
		}
		frame, err := b.lab.Dispatch(req.Action)
		if err != nil {
			return fromError(err)
		}
		b.labCursor = frame.Index
		b.labLength = frame.Index
		return frameResponse(frame, b.labCursor > 0, false)
	case "lab.seek":
		if req.Scenario != "" || req.Size != 0 || req.Action != (sim.Action{}) || b.lab == nil {
			return failure(sim.CodeInvalidRequest, "Reset the lab before seeking its history.")
		}
		frame, err := b.lab.Seek(req.Index)
		if err != nil {
			return fromError(err)
		}
		b.labCursor = frame.Index
		return frameResponse(frame, b.labCursor > 0, b.labCursor < b.labLength)
	case "finance.reset":
		if req.Scenario != "" || req.Size != 0 || req.Index != 0 || req.Action != (sim.Action{}) {
			return failure(sim.CodeInvalidRequest, "Financial reset does not accept additional fields.")
		}
		session, err := sim.NewFinancialSession()
		if err != nil {
			return fromError(err)
		}
		frame, err := session.Seek(0)
		if err != nil {
			return fromError(err)
		}
		b.finance = session
		b.financeCursor = 0
		b.financeLength = 0
		return frameResponse(frame, false, false)
	case "finance.action":
		if req.Scenario != "" || req.Size != 0 || req.Index != 0 || b.finance == nil {
			return failure(sim.CodeInvalidRequest, "Reset the financial simulation before dispatching an action.")
		}
		frame, err := b.finance.Dispatch(req.Action)
		if err != nil {
			return fromError(err)
		}
		b.financeCursor = frame.Index
		b.financeLength = frame.Index
		return frameResponse(frame, b.financeCursor > 0, false)
	case "finance.seek":
		if req.Scenario != "" || req.Size != 0 || req.Action != (sim.Action{}) || b.finance == nil {
			return failure(sim.CodeInvalidRequest, "Reset the financial simulation before seeking its history.")
		}
		frame, err := b.finance.Seek(req.Index)
		if err != nil {
			return fromError(err)
		}
		b.financeCursor = frame.Index
		return frameResponse(frame, b.financeCursor > 0, b.financeCursor < b.financeLength)
	default:
		return failure(sim.CodeInvalidRequest, "That operation is not supported.")
	}
}

func frameResponse(frame sim.Frame, canBack, canForward bool) response {
	return response{OK: true, Frame: &frame, CanBack: &canBack, CanForward: &canForward}
}

func fromError(err error) response {
	code := sim.CodeInternal
	var coded interface{ Code() string }
	if errors.As(err, &coded) {
		code = coded.Code()
	}
	return failure(code, err.Error())
}

func failure(code, message string) response {
	return response{OK: false, Error: &errorResponse{Code: code, Message: message}}
}

func marshalResponse(value response) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return `{"ok":false,"error":{"code":"internal","message":"The EPaxos response could not be encoded."}}`
	}
	return string(encoded)
}
