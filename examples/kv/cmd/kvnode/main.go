//go:build kvnode

// Command kvnode runs the Pebble-backed EPaxos key-value example.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"gosuda.org/moreconsensus/epaxos"
	"gosuda.org/moreconsensus/examples/kv"
)

type service struct {
	mu      sync.Mutex
	id      epaxos.ReplicaID
	node    *epaxos.RawNode
	store   *epaxos.MemoryStorage
	db      *kv.DB
	peers   map[epaxos.ReplicaID]string
	client  *http.Client
	sendq   chan epaxos.Message
	nextSeq uint64
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
	store := epaxos.NewMemoryStorage()
	node, err := epaxos.NewRawNode(epaxos.Config{ID: epaxos.ReplicaID(*idFlag), Voters: voters, Storage: store, RetryTicks: 2, RecoveryTicks: 5, TimeOptimization: true, TimeOptimizationTicks: 1})
	if err != nil {
		log.Fatal(err)
	}
	s := &service{id: epaxos.ReplicaID(*idFlag), node: node, store: store, db: db, peers: peers, client: &http.Client{}, sendq: make(chan epaxos.Message, 1024), nextSeq: 1}
	for range 8 {
		go s.transportWorker()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/epaxos/message", s.handleMessage)
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

func (s *service) handleKV(w http.ResponseWriter, r *http.Request) {
	keyText, err := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/kv/"))
	if err != nil || keyText == "" {
		http.Error(w, "bad key", http.StatusBadRequest)
		return
	}
	key := []byte(keyText)
	switch r.Method {
	case http.MethodGet:
		value, ok, err := s.db.Get(key)
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
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.proposeAndWait(r.Context(), kv.CommandForPut(uint64(s.id), s.next(), key, body), key, body); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := s.proposeAndWait(r.Context(), kv.CommandForDelete(uint64(s.id), s.next(), key), key, nil); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
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
	s.mu.Lock()
	err = s.node.Step(msg)
	out, drainErr := s.drainLocked()
	s.mu.Unlock()
	if err != nil || drainErr != nil {
		http.Error(w, errorsJoin(err, drainErr).Error(), http.StatusBadRequest)
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

func (s *service) proposeAndWait(ctx context.Context, cmd epaxos.Command, key, value []byte) error {
	s.mu.Lock()
	_, err := s.node.Propose(cmd)
	out, drainErr := s.drainLocked()
	s.mu.Unlock()
	if err != nil || drainErr != nil {
		return errorsJoin(err, drainErr)
	}
	s.send(out)
	for range 512 {
		current, ok, err := s.db.Get(key)
		if err == nil {
			if value == nil && !ok {
				return nil
			}
			if value != nil && ok && bytes.Equal(current, value) {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		s.mu.Lock()
		s.node.Tick()
		out, drainErr := s.drainLocked()
		s.mu.Unlock()
		if drainErr != nil {
			return drainErr
		}
		s.send(out)
	}
	return fmt.Errorf("proposal was not applied after logical tick budget")
}

func (s *service) drainLocked() ([]epaxos.Message, error) {
	var out []epaxos.Message
	for round := 0; round < 1000; round++ {
		if !s.node.HasReady() {
			return out, nil
		}
		rd := s.node.Ready()
		if err := s.store.ApplyReady(rd); err != nil {
			return nil, err
		}
		for _, committed := range rd.Committed {
			if err := s.db.ApplyCommitted(committed); err != nil {
				return nil, err
			}
		}
		out = append(out, rd.Messages...)
		s.node.Advance(rd)
	}
	return nil, fmt.Errorf("ready processing did not quiesce")
}

func (s *service) send(messages []epaxos.Message) {
	for _, msg := range messages {
		if msg.To == s.id || s.peers[msg.To] == "" {
			continue
		}
		select {
		case s.sendq <- msg:
		default:
		}
	}
}

func (s *service) transportWorker() {
	for msg := range s.sendq {
		base := s.peers[msg.To]
		if base == "" {
			continue
		}
		buf, err := epaxos.EncodeMessage(nil, msg)
		if err != nil {
			continue
		}
		resp, err := s.client.Post(base+"/epaxos/message", "application/octet-stream", bytes.NewReader(buf))
		if err == nil && resp.Body != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
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
