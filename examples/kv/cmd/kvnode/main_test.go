//go:build kvnode

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"gosuda.org/moreconsensus/epaxos"
	"gosuda.org/moreconsensus/examples/kv"
)

func TestParsePeersTrimsPeerURLsAndKeepsVoterOrder(t *testing.T) {
	peers, voters, err := parsePeers(" 2=http://b.example/ , ,1=http://a.example///")
	if err != nil {
		t.Fatal(err)
	}
	if len(voters) != 2 || voters[0] != 2 || voters[1] != 1 {
		t.Fatalf("voters=%v", voters)
	}
	if peers[2] != "http://b.example" || peers[1] != "http://a.example" {
		t.Fatalf("peers=%v", peers)
	}
}

func TestParsePeersRejectsMalformedEntries(t *testing.T) {
	for _, raw := range []string{"1", "x=http://example"} {
		if _, _, err := parsePeers(raw); err == nil {
			t.Fatalf("parsePeers(%q) succeeded", raw)
		}
	}
}

func TestHandleKVAppliesPutGetAndDeleteThroughConsensus(t *testing.T) {
	s := newTestService(t)

	put := httptest.NewRecorder()
	s.handleKV(put, httptest.NewRequest(http.MethodPut, "/kv/alpha", bytes.NewReader([]byte("one"))))
	if put.Code != http.StatusNoContent {
		t.Fatalf("put status=%d body=%q", put.Code, put.Body.String())
	}

	get := httptest.NewRecorder()
	s.handleKV(get, httptest.NewRequest(http.MethodGet, "/kv/alpha", nil))
	if get.Code != http.StatusOK || get.Body.String() != "one" {
		t.Fatalf("get status=%d body=%q", get.Code, get.Body.String())
	}

	deleted := httptest.NewRecorder()
	s.handleKV(deleted, httptest.NewRequest(http.MethodDelete, "/kv/alpha", nil))
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%q", deleted.Code, deleted.Body.String())
	}

	missing := httptest.NewRecorder()
	s.handleKV(missing, httptest.NewRequest(http.MethodGet, "/kv/alpha", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing status=%d body=%q", missing.Code, missing.Body.String())
	}
}

func TestHandleTxnAppliesMultiplePutsAndDeleteThroughConsensus(t *testing.T) {
	s := newTestService(t)

	seed := httptest.NewRecorder()
	s.handleKV(seed, httptest.NewRequest(http.MethodPut, "/kv/gone", bytes.NewReader([]byte("old"))))
	if seed.Code != http.StatusNoContent {
		t.Fatalf("seed status=%d body=%q", seed.Code, seed.Body.String())
	}

	body := []byte(`[{"key":"alpha","value":"one"},{"key":"beta","value":"two"},{"key":"gone","delete":true}]`)
	rr := httptest.NewRecorder()
	s.handleTxn(rr, httptest.NewRequest(http.MethodPost, "/txn", bytes.NewReader(body)))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("txn status=%d body=%q", rr.Code, rr.Body.String())
	}

	requireKVValue(t, s, "alpha", "one")
	requireKVValue(t, s, "beta", "two")
	requireKVMissing(t, s, "gone")

	ref := epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}
	if !hasExecutedRef(s.node.Status().Executed, ref) {
		t.Fatalf("executed refs=%v, want %s", s.node.Status().Executed, ref)
	}
}

func TestHandleTxnRejectsMalformedJSONAndInvalidRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   []byte
		want   int
	}{
		{name: "wrong method", method: http.MethodGet, want: http.StatusMethodNotAllowed},
		{name: "malformed json", method: http.MethodPost, body: []byte(`{"key"`), want: http.StatusBadRequest},
		{name: "empty transaction", method: http.MethodPost, body: []byte(`[]`), want: http.StatusBadRequest},
		{name: "empty key", method: http.MethodPost, body: []byte(`[{"key":"","value":"x"}]`), want: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			rr := httptest.NewRecorder()
			s.handleTxn(rr, httptest.NewRequest(tc.method, "/txn", bytes.NewReader(tc.body)))
			if rr.Code != tc.want {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			status := s.node.Status()
			if len(status.Instances) != 0 || len(status.Executed) != 0 {
				t.Fatalf("node status after rejected request: instances=%v executed=%v", status.Instances, status.Executed)
			}
		})
	}
}

func TestHandleKVRepeatedPutWaitsForNewAppliedRef(t *testing.T) {
	s := newTestService(t)
	for i := 1; i <= 2; i++ {
		rr := httptest.NewRecorder()
		s.handleKV(rr, httptest.NewRequest(http.MethodPut, "/kv/repeat", bytes.NewReader([]byte("same"))))
		if rr.Code != http.StatusNoContent {
			t.Fatalf("put %d status=%d body=%q", i, rr.Code, rr.Body.String())
		}
	}

	scan, err := s.db.Scan(kv.ScanOptions{Prefix: []byte("repeat")})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 1 || string(scan[0].Value) != "same" {
		t.Fatalf("scan=%v", scan)
	}
	wantTime := uint64(2)
	if scan[0].Time != wantTime {
		t.Fatalf("applied version=%d, want %d", scan[0].Time, wantTime)
	}
	ref := epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}
	if !hasExecutedRef(s.node.Status().Executed, ref) {
		t.Fatalf("executed refs=%v, want %s", s.node.Status().Executed, ref)
	}
}

func TestHandleScanReturnsPrefixRows(t *testing.T) {
	s := newTestService(t)

	body := []byte(`[{"key":"tx-a","value":"one"},{"key":"tx-b","value":"two"},{"key":"other","value":"three"}]`)
	rr := httptest.NewRecorder()
	s.handleTxn(rr, httptest.NewRequest(http.MethodPost, "/txn", bytes.NewReader(body)))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("txn status=%d body=%q", rr.Code, rr.Body.String())
	}

	scan := httptest.NewRecorder()
	s.handleScan(scan, httptest.NewRequest(http.MethodGet, "/scan?prefix=tx", nil))
	if scan.Code != http.StatusOK {
		t.Fatalf("scan status=%d body=%q", scan.Code, scan.Body.String())
	}
	var rows []scanResponseKV
	if err := json.Unmarshal(scan.Body.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	wantVersion := uint64(1)
	want := []scanResponseKV{
		{Key: "tx-a", Value: "one", Time: wantVersion},
		{Key: "tx-b", Value: "two", Time: wantVersion},
	}
	if len(rows) != len(want) {
		t.Fatalf("rows=%v, want %v", rows, want)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Fatalf("rows=%v, want %v", rows, want)
		}
	}
}

func TestHandleScanRejectsBadQueryAndMethod(t *testing.T) {
	tests := []struct {
		name   string
		method string
		target string
		want   int
	}{
		{name: "wrong method", method: http.MethodPost, target: "/scan", want: http.StatusMethodNotAllowed},
		{name: "bad limit", method: http.MethodGet, target: "/scan?limit=many", want: http.StatusBadRequest},
		{name: "bad reverse", method: http.MethodGet, target: "/scan?reverse=sideways", want: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			rr := httptest.NewRecorder()
			s.handleScan(rr, httptest.NewRequest(tc.method, tc.target, nil))
			if rr.Code != tc.want {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
		})
	}
}

func TestHandleKVRejectsBadKeysAndMethods(t *testing.T) {
	s := newTestService(t)

	badKey := httptest.NewRecorder()
	s.handleKV(badKey, httptest.NewRequest(http.MethodGet, "/kv/", nil))
	if badKey.Code != http.StatusBadRequest {
		t.Fatalf("bad-key status=%d body=%q", badKey.Code, badKey.Body.String())
	}

	badEscape := httptest.NewRecorder()
	s.handleKV(badEscape, &http.Request{Method: http.MethodGet, URL: &url.URL{Path: "/kv/%zz"}, Body: http.NoBody})
	if badEscape.Code != http.StatusBadRequest {
		t.Fatalf("bad-escape status=%d body=%q", badEscape.Code, badEscape.Body.String())
	}

	method := httptest.NewRecorder()
	s.handleKV(method, httptest.NewRequest(http.MethodPost, "/kv/alpha", nil))
	if method.Code != http.StatusMethodNotAllowed {
		t.Fatalf("method status=%d body=%q", method.Code, method.Body.String())
	}
}

func TestHandleMessageRejectsMalformedTransportPayload(t *testing.T) {
	s := newTestService(t)
	rr := httptest.NewRecorder()
	s.handleMessage(rr, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader([]byte("not an epaxos message"))))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
}

func requireKVValue(t *testing.T, s *service, key, want string) {
	t.Helper()
	rr := httptest.NewRecorder()
	s.handleKV(rr, httptest.NewRequest(http.MethodGet, "/kv/"+key, nil))
	if rr.Code != http.StatusOK || rr.Body.String() != want {
		t.Fatalf("get %q status=%d body=%q", key, rr.Code, rr.Body.String())
	}
}

func requireKVMissing(t *testing.T, s *service, key string) {
	t.Helper()
	rr := httptest.NewRecorder()
	s.handleKV(rr, httptest.NewRequest(http.MethodGet, "/kv/"+key, nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("get %q status=%d body=%q", key, rr.Code, rr.Body.String())
	}
}

func hasExecutedRef(refs []epaxos.InstanceRef, want epaxos.InstanceRef) bool {
	for _, ref := range refs {
		if ref == want {
			return true
		}
	}
	return false
}

func newTestService(t *testing.T) *service {
	t.Helper()
	db, err := kv.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := epaxos.NewMemoryStorage()
	node, err := epaxos.NewRawNode(epaxos.Config{ID: 1, Voters: []epaxos.ReplicaID{1}, Storage: store, RetryTicks: 2, RecoveryTicks: 5, TimeOptimization: true, TimeOptimizationTicks: 1})
	if err != nil {
		t.Fatal(err)
	}
	return &service{id: 1, node: node, store: store, db: db, peers: map[epaxos.ReplicaID]string{1: "http://127.0.0.1"}, client: &http.Client{}, sendq: make(chan epaxos.Message, 8), nextSeq: 1}
}

func TestSendPostsOnlyConfiguredRemotePeers(t *testing.T) {
	s := newTestService(t)
	s.peers[2] = "http://peer-2"
	var posted []string
	s.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		posted = append(posted, r.URL.String())
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	s.send([]epaxos.Message{{To: 1}, {Type: epaxos.MsgCommit, From: 1, To: 2, Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Deps: []epaxos.InstanceNum{0}}, {To: 3}})
	if len(posted) != 1 || posted[0] != "http://peer-2/epaxos/message" {
		t.Fatalf("posted=%v", posted)
	}
	if len(s.sendq) != 0 {
		t.Fatalf("queued messages=%d", len(s.sendq))
	}
}

func TestSendQueuesFailedRemotePosts(t *testing.T) {
	s := newTestService(t)
	s.peers[2] = "http://peer-2"
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("down")
	})}
	msg := epaxos.Message{Type: epaxos.MsgCommit, From: 1, To: 2, Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Deps: []epaxos.InstanceNum{0}}
	s.send([]epaxos.Message{msg})
	if len(s.sendq) != 1 {
		t.Fatalf("queued messages=%d", len(s.sendq))
	}
	got := <-s.sendq
	if got.To != 2 {
		t.Fatalf("queued message to replica %d", got.To)
	}
}

func TestHandleKVMapsUnreadablePutBodyToBadRequest(t *testing.T) {
	s := newTestService(t)
	rr := httptest.NewRecorder()
	s.handleKV(rr, httptest.NewRequest(http.MethodPut, "/kv/alpha", errReader{}))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
