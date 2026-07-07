//go:build kvnode

package main

import (
	"bytes"
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
