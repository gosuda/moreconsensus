//go:build kvnode

// Command kvnode runs the Pebble-backed EPaxos key-value example.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	"gosuda.org/moreconsensus/epaxos"
	"gosuda.org/moreconsensus/examples/kv"
)

type readyApplier interface {
	ApplyReady(epaxos.Ready) error
}

type service struct {
	mu             sync.Mutex
	faultMu        sync.RWMutex
	id             epaxos.ReplicaID
	node           *epaxos.RawNode
	ready          readyApplier
	db             *kv.DB
	peers          map[epaxos.ReplicaID]string
	client         *http.Client
	sendq          chan epaxos.Message
	nextSeq        uint64
	transportDrops map[transportLink]struct{}
	storageFailed  bool
}

type transportLink struct {
	from epaxos.ReplicaID
	to   epaxos.ReplicaID
}

type transportFaultRequest struct {
	From epaxos.ReplicaID `json:"from"`
	To   epaxos.ReplicaID `json:"to"`
	Drop bool             `json:"drop"`
}

type storageFaultRequest struct {
	Fail bool `json:"fail"`
}

func main() {
	idFlag := flag.Uint64("id", 1, "replica id")
	listen := flag.String("listen", ":8080", "HTTP listen address")
	data := flag.String("data", "kvnode-data", "Pebble data directory")
	peersFlag := flag.String("peers", "1=http://127.0.0.1:8080", "comma-separated id=url peer list")
	flag.Parse()
	peers, voters, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatal(err)
	}
	db, err := kv.Open(*data)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := db.EPaxosStorage()
	node, err := epaxos.NewRawNode(epaxos.Config{ID: epaxos.ReplicaID(*idFlag), Voters: voters, Storage: store, RetryTicks: 2, RecoveryTicks: 5, TimeOptimization: true, TimeOptimizationTicks: 1})
	if err != nil {
		log.Fatal(err)
	}
	s := &service{id: epaxos.ReplicaID(*idFlag), node: node, ready: db, db: db, peers: peers, client: &http.Client{}, sendq: make(chan epaxos.Message, 1024), nextSeq: 1, transportDrops: make(map[transportLink]struct{})}
	for range 8 {
		go s.transportWorker()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/txn", s.handleTxn)
	mux.HandleFunc("/scan", s.handleScan)
	mux.HandleFunc("/epaxos/message", s.handleMessage)
	mux.HandleFunc("/faults/transport", s.handleTransportFault)
	mux.HandleFunc("/faults/storage", s.handleStorageFault)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	log.Printf("kvnode %d listening on %s", s.id, *listen)
	log.Fatal(http.ListenAndServe(*listen, mux))
}

func parsePeers(raw string) (map[epaxos.ReplicaID]string, []epaxos.ReplicaID, error) {
	peers := make(map[epaxos.ReplicaID]string)
	var voters []epaxos.ReplicaID
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.SplitN(part, "=", 2)
		if len(fields) != 2 {
			return nil, nil, fmt.Errorf("bad peer %q", part)
		}
		id, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		peers[epaxos.ReplicaID(id)] = strings.TrimRight(fields[1], "/")
		voters = append(voters, epaxos.ReplicaID(id))
	}
	return peers, voters, nil
}

func parseReadBounds(q url.Values) (kv.TimestampBounds, bool, error) {
	at, hasAt, err := parseReadBoundUint(q, "at")
	if err != nil {
		return kv.TimestampBounds{}, false, err
	}
	minTime, hasMinTime, err := parseReadBoundUint(q, "min-time")
	if err != nil {
		return kv.TimestampBounds{}, false, err
	}
	maxTime, hasMaxTime, err := parseReadBoundUint(q, "max-time")
	if err != nil {
		return kv.TimestampBounds{}, false, err
	}
	exactTime, hasExactTime, err := parseReadBoundUint(q, "exact-time")
	if err != nil {
		return kv.TimestampBounds{}, false, err
	}
	referenceTime, hasReferenceTime, err := parseReadBoundUint(q, "reference-time")
	if err != nil {
		return kv.TimestampBounds{}, false, err
	}
	maxStaleness, hasMaxStaleness, err := parseReadBoundUint(q, "max-staleness")
	if err != nil {
		return kv.TimestampBounds{}, false, err
	}
	exactStaleness, hasExactStaleness, err := parseReadBoundUint(q, "exact-staleness")
	if err != nil {
		return kv.TimestampBounds{}, false, err
	}

	interval := hasMinTime || hasMaxTime
	if interval && !(hasMinTime && hasMaxTime) {
		return kv.TimestampBounds{}, false, fmt.Errorf("bad timestamp selector")
	}
	relative := hasReferenceTime || hasMaxStaleness || hasExactStaleness
	if relative && (!hasReferenceTime || (hasMaxStaleness == hasExactStaleness)) {
		return kv.TimestampBounds{}, false, fmt.Errorf("bad timestamp selector")
	}
	groups := 0
	for _, set := range []bool{hasAt, interval, hasExactTime, relative} {
		if set {
			groups++
		}
	}
	if groups == 0 {
		return kv.TimestampBounds{}, false, nil
	}
	if groups != 1 {
		return kv.TimestampBounds{}, false, fmt.Errorf("bad timestamp selector")
	}
	var bounds kv.TimestampBounds
	switch {
	case hasAt:
		bounds = kv.TimestampAtOrBefore(at)
	case interval:
		bounds = kv.TimestampWithinBounds(minTime, maxTime)
	case hasExactTime:
		bounds = kv.ExactTimestamp(exactTime)
	case hasMaxStaleness:
		bounds = kv.BoundedStaleness(referenceTime, maxStaleness)
	case hasExactStaleness:
		bounds = kv.ExactStaleness(referenceTime, exactStaleness)
	}
	if err := bounds.Validate(); err != nil {
		return kv.TimestampBounds{}, false, err
	}
	return bounds, true, nil
}

func parseReadBoundUint(q url.Values, name string) (uint64, bool, error) {
	values, ok := q[name]
	if !ok {
		return 0, false, nil
	}
	if len(values) != 1 || values[0] == "" {
		return 0, true, fmt.Errorf("bad timestamp selector")
	}
	value, err := strconv.ParseUint(values[0], 10, 64)
	if err != nil {
		return 0, true, err
	}
	return value, true, nil
}

func (s *service) handleKV(w http.ResponseWriter, r *http.Request) {
	keyText, err := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/kv/"))
	if err != nil {
		http.Error(w, "bad key", http.StatusBadRequest)
		return
	}
	key := []byte(keyText)
	if err := kv.ValidateKey(key); err != nil {
		http.Error(w, "bad key", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		bounds, hasBounds, err := parseReadBounds(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if s.storageFaultActive() {
			http.Error(w, "storage fault active", http.StatusServiceUnavailable)
			return
		}
		var value []byte
		var ok bool
		if hasBounds {
			value, ok, err = s.db.GetWithBounds(key, bounds)
		} else {
			if err := s.waitForKeys(r.Context(), key); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			value, ok, err = s.db.Get(key)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_, _ = w.Write(value)
	case http.MethodPut:
		if s.storageFaultActive() {
			http.Error(w, "storage fault active", http.StatusServiceUnavailable)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.proposeAndWait(r.Context(), kv.CommandForPut(uint64(s.id), s.next(), key, body)); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if s.storageFaultActive() {
			http.Error(w, "storage fault active", http.StatusServiceUnavailable)
			return
		}
		if err := s.proposeAndWait(r.Context(), kv.CommandForDelete(uint64(s.id), s.next(), key)); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

type txnRequestOp struct {
	Delete bool   `json:"delete"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

func (s *service) handleTxn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request []txnRequestOp
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(request) == 0 {
		http.Error(w, "empty transaction", http.StatusBadRequest)
		return
	}
	ops := make([]kv.TxnOp, 0, len(request))
	for _, op := range request {
		key := []byte(op.Key)
		if err := kv.ValidateKey(key); err != nil {
			http.Error(w, "bad key", http.StatusBadRequest)
			return
		}
		ops = append(ops, kv.TxnOp{Delete: op.Delete, Key: key, Value: []byte(op.Value)})
	}
	if s.storageFaultActive() {
		http.Error(w, "storage fault active", http.StatusServiceUnavailable)
		return
	}
	if err := s.proposeAndWait(r.Context(), kv.CommandForTxn(uint64(s.id), s.next(), ops)); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type scanResponseKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Time  uint64 `json:"time"`
}

func (s *service) handleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	limit := 0
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if n < 0 {
			http.Error(w, "bad limit", http.StatusBadRequest)
			return
		}
		limit = n
	}
	reverse := false
	if raw := q.Get("reverse"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reverse = v
	}
	bounds, hasBounds, err := parseReadBounds(q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if hasBounds && q.Get("barrier") != "" {
		http.Error(w, "bad timestamp selector", http.StatusBadRequest)
		return
	}
	if s.storageFaultActive() {
		http.Error(w, "storage fault active", http.StatusServiceUnavailable)
		return
	}
	if raw := q.Get("barrier"); raw != "" {
		parts := strings.Split(raw, ",")
		keys := make([][]byte, 0, len(parts))
		for _, part := range parts {
			key := []byte(part)
			if err := kv.ValidateKey(key); err != nil {
				http.Error(w, "bad barrier key", http.StatusBadRequest)
				return
			}
			keys = append(keys, key)
		}
		if err := s.waitForKeys(r.Context(), keys...); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	rows, err := s.db.Scan(kv.ScanOptions{
		Start:   []byte(q.Get("start")),
		End:     []byte(q.Get("end")),
		Prefix:  []byte(q.Get("prefix")),
		Limit:   limit,
		Reverse: reverse,
		Bounds:  bounds,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]scanResponseKV, len(rows))
	for i, row := range rows {
		out[i] = scanResponseKV{Key: string(row.Key), Value: string(row.Value), Time: row.Time}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *service) handleTransportFault(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		drops := s.transportDropSnapshot()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(drops)
	case http.MethodPost:
		var req transportFaultRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.From == 0 || req.To == 0 {
			http.Error(w, "bad transport link", http.StatusBadRequest)
			return
		}
		s.setTransportDrop(req.From, req.To, req.Drop)
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		s.clearTransportDrops()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *service) handleStorageFault(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(storageFaultRequest{Fail: s.storageFaultActive()})
	case http.MethodPost:
		var req storageFaultRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.setStorageFault(req.Fail)
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		s.setStorageFault(false)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *service) setStorageFault(failed bool) {
	s.faultMu.Lock()
	defer s.faultMu.Unlock()
	s.storageFailed = failed
}

func (s *service) storageFaultActive() bool {
	s.faultMu.RLock()
	defer s.faultMu.RUnlock()
	return s.storageFailed
}

func (s *service) setTransportDrop(from, to epaxos.ReplicaID, drop bool) {
	s.faultMu.Lock()
	defer s.faultMu.Unlock()
	if s.transportDrops == nil {
		s.transportDrops = make(map[transportLink]struct{})
	}
	link := transportLink{from: from, to: to}
	if drop {
		s.transportDrops[link] = struct{}{}
		return
	}
	delete(s.transportDrops, link)
}

func (s *service) clearTransportDrops() {
	s.faultMu.Lock()
	defer s.faultMu.Unlock()
	for link := range s.transportDrops {
		delete(s.transportDrops, link)
	}
}

func (s *service) transportDropSnapshot() []transportFaultRequest {
	s.faultMu.RLock()
	defer s.faultMu.RUnlock()
	out := make([]transportFaultRequest, 0, len(s.transportDrops))
	for link := range s.transportDrops {
		out = append(out, transportFaultRequest{From: link.from, To: link.to, Drop: true})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		return out[i].To < out[j].To
	})
	return out
}

func (s *service) transportDropped(from, to epaxos.ReplicaID) bool {
	s.faultMu.RLock()
	defer s.faultMu.RUnlock()
	_, ok := s.transportDrops[transportLink{from: from, to: to}]
	return ok
}

func (s *service) handleMessage(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var msg epaxos.Message
	if err := epaxos.DecodeMessage(body, &msg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if s.transportDropped(msg.From, s.id) {
		http.Error(w, "transport link dropped", http.StatusConflict)
		return
	}
	if s.storageFaultActive() {
		http.Error(w, "storage fault active", http.StatusServiceUnavailable)
		return
	}
	s.mu.Lock()
	err = s.node.Step(msg)
	out, drainErr := s.drainLocked()
	s.mu.Unlock()
	if drainErr != nil {
		http.Error(w, errorsJoin(err, drainErr).Error(), http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.send(out)
	w.WriteHeader(http.StatusNoContent)
}

func (s *service) next() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.nextSeq
	s.nextSeq++
	return seq
}

func (s *service) waitForKeys(ctx context.Context, keys ...[]byte) error {
	conflicts := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		conflicts = append(conflicts, append([]byte(nil), key...))
	}
	if len(conflicts) == 0 {
		return nil
	}
	return s.proposeAndWait(ctx, epaxos.Command{ID: epaxos.CommandID{Client: uint64(s.id), Sequence: s.next()}, ConflictKeys: conflicts})
}

func (s *service) proposeAndWait(ctx context.Context, cmd epaxos.Command) error {
	if s.storageFaultActive() {
		return fmt.Errorf("storage fault active")
	}
	s.mu.Lock()
	ref, err := s.node.Propose(cmd)
	out, drainErr := s.drainLocked()
	done := err == nil && drainErr == nil && s.executedLocked(ref)
	s.mu.Unlock()
	if err != nil || drainErr != nil {
		return errorsJoin(err, drainErr)
	}
	s.send(out)
	if done {
		return nil
	}
	for range 512 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		s.mu.Lock()
		s.node.Tick()
		out, drainErr := s.drainLocked()
		done := s.executedLocked(ref)
		s.mu.Unlock()
		if drainErr != nil {
			return drainErr
		}
		s.send(out)
		if done {
			return nil
		}
	}
	return fmt.Errorf("proposal was not applied after logical tick budget")
}

func (s *service) executedLocked(ref epaxos.InstanceRef) bool {
	for _, got := range s.node.Status().Executed {
		if got == ref {
			return true
		}
	}
	return false
}

func (s *service) drainLocked() ([]epaxos.Message, error) {
	var out []epaxos.Message
	for round := 0; round < 1000; round++ {
		if !s.node.HasReady() {
			return out, nil
		}
		rd := s.node.Ready()
		if err := s.ready.ApplyReady(rd); err != nil {
			return nil, err
		}
		out = append(out, rd.Messages...)
		if err := s.node.Advance(rd); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("ready processing did not quiesce")
}

func (s *service) send(messages []epaxos.Message) {
	for _, msg := range messages {
		if msg.To == s.id || s.peers[msg.To] == "" {
			continue
		}
		if s.postMessage(msg) {
			continue
		}
		select {
		case s.sendq <- msg:
		default:
		}
	}
}

func (s *service) postMessage(msg epaxos.Message) bool {
	base := s.peers[msg.To]
	if base == "" {
		return true
	}
	if s.transportDropped(s.id, msg.To) {
		return true
	}
	buf, err := epaxos.EncodeMessage(nil, msg)
	if err != nil {
		return false
	}
	resp, err := s.client.Post(base+"/epaxos/message", "application/octet-stream", bytes.NewReader(buf))
	if err != nil {
		return false
	}
	if resp.Body != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	return resp.StatusCode < http.StatusInternalServerError
}

func (s *service) transportWorker() {
	for msg := range s.sendq {
		_ = s.postMessage(msg)
	}
}

func errorsJoin(a, b error) error {
	if a != nil {
		if b != nil {
			return fmt.Errorf("%v; %w", a, b)
		}
		return a
	}
	return b
}
