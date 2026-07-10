package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"sync"

	"gosuda.org/moreconsensus/epaxos"
)

const maxProxyBodyBytes = int64(2 << 20)

type ProxyAction struct {
	ID   uint64 `json:"id"`
	Kind string `json:"kind"`
	From uint64 `json:"from,omitempty"`
	To   uint64 `json:"to,omitempty"`
}

type ProxyReceipt struct {
	ID                 uint64   `json:"id"`
	ActionID           uint64   `json:"action_id,omitempty"`
	MessageID          uint64   `json:"message_id,omitempty"`
	Kind               string   `json:"kind"`
	From               uint64   `json:"from,omitempty"`
	To                 uint64   `json:"to,omitempty"`
	HTTPStatus         int      `json:"http_status,omitempty"`
	Copies             int      `json:"copies,omitempty"`
	ReleasedMessageIDs []uint64 `json:"released_message_ids,omitempty"`
	Applicable         bool     `json:"applicable"`
	Reason             string   `json:"reason,omitempty"`
}

type heldRequest struct {
	id     uint64
	method string
	path   string
	header http.Header
	body   []byte
	from   uint64
	to     uint64
}

type FaultProxy struct {
	listener net.Listener
	upstream *url.URL
	client   *http.Client
	log      io.Writer
	server   *http.Server

	mu            sync.Mutex
	started       bool
	closed        bool
	actions       []ProxyAction
	actionIDs     map[uint64]struct{}
	held          map[uint64]heldRequest
	receipts      []ProxyReceipt
	nextMessageID uint64
	nextReceiptID uint64
}

func newFaultProxy(listener net.Listener, upstream *url.URL, logWriter io.Writer) *FaultProxy {
	proxy := &FaultProxy{
		listener:  listener,
		upstream:  cloneURL(upstream),
		client:    &http.Client{},
		log:       logWriter,
		actionIDs: make(map[uint64]struct{}),
		held:      make(map[uint64]heldRequest),
	}
	proxy.server = &http.Server{Handler: proxy}
	return proxy
}

func (p *FaultProxy) URL() string {
	if p == nil || p.listener == nil {
		return ""
	}
	return "http://" + p.listener.Addr().String()
}

func (p *FaultProxy) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.listener == nil || p.upstream == nil {
		return fmt.Errorf("proxy requires listener and upstream")
	}
	if p.started {
		return fmt.Errorf("proxy already started")
	}
	if p.closed {
		return fmt.Errorf("proxy is closed")
	}
	p.started = true
	go func() {
		err := p.server.Serve(p.listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			p.mu.Lock()
			p.appendReceiptLocked(ProxyReceipt{Kind: "proxy-server-error", Applicable: true, Reason: err.Error()})
			p.mu.Unlock()
		}
	}()
	return nil
}

func (p *FaultProxy) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	started := p.started
	p.mu.Unlock()
	if !started {
		if p.listener != nil {
			return p.listener.Close()
		}
		return nil
	}
	return p.server.Close()
}

func (p *FaultProxy) Schedule(actions ...ProxyAction) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	local := make(map[uint64]struct{}, len(actions))
	for _, action := range actions {
		if action.ID == 0 {
			return fmt.Errorf("proxy action ID must be nonzero")
		}
		if _, exists := p.actionIDs[action.ID]; exists {
			return fmt.Errorf("duplicate proxy action ID %d", action.ID)
		}
		if _, exists := local[action.ID]; exists {
			return fmt.Errorf("duplicate proxy action ID %d", action.ID)
		}
		switch action.Kind {
		case "pass", "drop", "duplicate", "delay":
		default:
			return fmt.Errorf("unsupported proxy action %q", action.Kind)
		}
		local[action.ID] = struct{}{}
	}
	for _, action := range actions {
		p.actionIDs[action.ID] = struct{}{}
		p.actions = append(p.actions, action)
	}
	return nil
}

func (p *FaultProxy) Receipts() []ProxyReceipt {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := append([]ProxyReceipt(nil), p.receipts...)
	for i := range out {
		out[i].ReleasedMessageIDs = append([]uint64(nil), p.receipts[i].ReleasedMessageIDs...)
	}
	return out
}

func (p *FaultProxy) HeldIDs() []uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]uint64, 0, len(p.held))
	for id := range p.held {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (p *FaultProxy) Release(ids []uint64) ([]ProxyReceipt, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("release requires at least one held message ID")
	}
	p.mu.Lock()
	requests := make([]heldRequest, 0, len(ids))
	seen := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		if _, duplicate := seen[id]; duplicate {
			p.mu.Unlock()
			return nil, fmt.Errorf("duplicate release message ID %d", id)
		}
		seen[id] = struct{}{}
		request, ok := p.held[id]
		if !ok {
			p.mu.Unlock()
			return nil, fmt.Errorf("held message %d does not exist", id)
		}
		requests = append(requests, request)
	}
	for _, id := range ids {
		delete(p.held, id)
	}
	p.mu.Unlock()

	status := http.StatusNoContent
	copies := 0
	var releaseErr error
	for _, request := range requests {
		gotStatus, _, err := p.forward(request.method, request.path, request.header, request.body)
		if err != nil {
			status = http.StatusBadGateway
			releaseErr = errors.Join(releaseErr, err)
			continue
		}
		copies++
		if gotStatus < 200 || gotStatus >= 300 {
			status = gotStatus
			releaseErr = errors.Join(releaseErr, fmt.Errorf("release message %d upstream status %d", request.id, gotStatus))
		}
	}
	p.mu.Lock()
	receipt := p.appendReceiptLocked(ProxyReceipt{Kind: "release", HTTPStatus: status, Copies: copies, ReleasedMessageIDs: append([]uint64(nil), ids...), Applicable: true})
	p.mu.Unlock()
	if releaseErr != nil {
		return []ProxyReceipt{receipt}, releaseErr
	}
	return []ProxyReceipt{receipt}, nil
}

func (p *FaultProxy) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(io.LimitReader(request.Body, maxProxyBodyBytes+1))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if int64(len(body)) > maxProxyBodyBytes {
		http.Error(w, "proxy request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	var message epaxos.Message
	decoded := epaxos.DecodeMessage(body, &message) == nil
	var from, to uint64
	if decoded {
		from = uint64(message.From)
		to = uint64(message.To)
	}

	p.mu.Lock()
	p.nextMessageID++
	messageID := p.nextMessageID
	action, hasAction := p.takeMatchingActionLocked(decoded, from, to)
	if hasAction && action.Kind == "delay" {
		p.held[messageID] = heldRequest{id: messageID, method: request.Method, path: request.URL.RequestURI(), header: request.Header.Clone(), body: append([]byte(nil), body...), from: from, to: to}
		p.appendReceiptLocked(ProxyReceipt{ActionID: action.ID, MessageID: messageID, Kind: action.Kind, From: from, To: to, HTTPStatus: http.StatusNoContent, Applicable: true})
		p.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if hasAction && action.Kind == "drop" {
		p.appendReceiptLocked(ProxyReceipt{ActionID: action.ID, MessageID: messageID, Kind: action.Kind, From: from, To: to, HTTPStatus: http.StatusNoContent, Applicable: true})
		p.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	p.mu.Unlock()

	copies := 1
	if hasAction && action.Kind == "duplicate" {
		copies = 2
	}
	var firstStatus int
	var firstHeader http.Header
	var firstBody []byte
	completed := 0
	allSucceeded := true
	var failureBody []byte
	for copyIndex := range copies {
		status, header, responseBody, forwardErr := p.forwardWithHeader(request.Method, request.URL.RequestURI(), request.Header, body)
		if copyIndex == 0 {
			firstStatus, firstHeader, firstBody = status, header, responseBody
		}
		if forwardErr != nil {
			allSucceeded = false
			failureBody = []byte(forwardErr.Error())
			continue
		}
		if status < http.StatusOK || status >= http.StatusMultipleChoices {
			allSucceeded = false
			failureBody = []byte(fmt.Sprintf("upstream returned status %d", status))
			continue
		}
		completed++
	}
	if firstStatus == 0 || !allSucceeded {
		firstStatus = http.StatusBadGateway
		firstHeader = nil
		if len(failureBody) != 0 {
			firstBody = failureBody
		}
	}
	if hasAction {
		p.mu.Lock()
		p.appendReceiptLocked(ProxyReceipt{ActionID: action.ID, MessageID: messageID, Kind: action.Kind, From: from, To: to, HTTPStatus: firstStatus, Copies: completed, Applicable: true})
		p.mu.Unlock()
	}
	copyHTTPHeader(w.Header(), firstHeader)
	w.WriteHeader(firstStatus)
	_, _ = w.Write(firstBody)
}

func (p *FaultProxy) takeMatchingActionLocked(decoded bool, from, to uint64) (ProxyAction, bool) {
	if !decoded {
		return ProxyAction{}, false
	}
	for i, action := range p.actions {
		if (action.From == 0 || action.From == from) && (action.To == 0 || action.To == to) {
			p.actions = append(p.actions[:i], p.actions[i+1:]...)
			return action, true
		}
	}
	return ProxyAction{}, false
}

func (p *FaultProxy) forward(method, requestURI string, header http.Header, body []byte) (int, []byte, error) {
	status, _, responseBody, err := p.forwardWithHeader(method, requestURI, header, body)
	return status, responseBody, err
}

func (p *FaultProxy) forwardWithHeader(method, requestURI string, header http.Header, body []byte) (int, http.Header, []byte, error) {
	target := *p.upstream
	relative, err := url.Parse(requestURI)
	if err != nil {
		return 0, nil, nil, err
	}
	target.Path = relative.Path
	target.RawPath = relative.RawPath
	target.RawQuery = relative.RawQuery
	forwardRequest, err := http.NewRequest(method, target.String(), bytes.NewReader(body))
	if err != nil {
		return 0, nil, nil, err
	}
	copyHTTPHeader(forwardRequest.Header, header)
	response, err := p.client.Do(forwardRequest)
	if err != nil {
		return 0, nil, nil, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return response.StatusCode, response.Header.Clone(), nil, err
	}
	return response.StatusCode, response.Header.Clone(), responseBody, nil
}

func (p *FaultProxy) appendReceiptLocked(receipt ProxyReceipt) ProxyReceipt {
	p.nextReceiptID++
	receipt.ID = p.nextReceiptID
	receipt.ReleasedMessageIDs = append([]uint64(nil), receipt.ReleasedMessageIDs...)
	p.receipts = append(p.receipts, receipt)
	if p.log != nil {
		_ = json.NewEncoder(p.log).Encode(receipt)
	}
	return cloneProxyReceipt(receipt)
}

func cloneProxyReceipt(receipt ProxyReceipt) ProxyReceipt {
	receipt.ReleasedMessageIDs = append([]uint64(nil), receipt.ReleasedMessageIDs...)
	return receipt
}

func cloneURL(value *url.URL) *url.URL {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}

func copyHTTPHeader(destination, source http.Header) {
	for key, values := range source {
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}
