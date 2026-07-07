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

func TestHandleTxnAcceptsDuplicateKeysAndAppliesInPayloadOrder(t *testing.T) {
	s := newTestService(t)
	seed := httptest.NewRecorder()
	s.handleKV(seed, httptest.NewRequest(http.MethodPut, "/kv/alpha", bytes.NewReader([]byte("before"))))
	if seed.Code != http.StatusNoContent {
		t.Fatalf("seed status=%d body=%q", seed.Code, seed.Body.String())
	}

	rr := httptest.NewRecorder()
	body := []byte(`[{"key":"alpha","value":"after"},{"key":"alpha","delete":true},{"key":"alpha","value":"final"}]`)
	s.handleTxn(rr, httptest.NewRequest(http.MethodPost, "/txn", bytes.NewReader(body)))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}

	requireKVValue(t, s, "alpha", "final")
	scan, err := s.db.Scan(kv.ScanOptions{Prefix: []byte("alpha")})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 1 || string(scan[0].Value) != "final" || scan[0].Time != 2 {
		t.Fatalf("scan=%#v", scan)
	}
	ref := epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}
	if !hasExecutedRef(s.node.Status().Executed, ref) {
		t.Fatalf("executed refs=%v, want %s", s.node.Status().Executed, ref)
	}
}

func TestHandleTxnAcceptsDuplicateKeysWithFinalDelete(t *testing.T) {
	s := newTestService(t)
	seed := httptest.NewRecorder()
	s.handleKV(seed, httptest.NewRequest(http.MethodPut, "/kv/alpha", bytes.NewReader([]byte("before"))))
	if seed.Code != http.StatusNoContent {
		t.Fatalf("seed status=%d body=%q", seed.Code, seed.Body.String())
	}

	rr := httptest.NewRecorder()
	body := []byte(`[{"key":"alpha","value":"after"},{"key":"alpha","delete":true}]`)
	s.handleTxn(rr, httptest.NewRequest(http.MethodPost, "/txn", bytes.NewReader(body)))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}

	requireKVMissing(t, s, "alpha")
	ref := epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}
	if !hasExecutedRef(s.node.Status().Executed, ref) {
		t.Fatalf("executed refs=%v, want %s", s.node.Status().Executed, ref)
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

func TestHandleScanRejectsNegativeLimit(t *testing.T) {
	s := newTestService(t)
	body := []byte(`[{"key":"scan-a","value":"one"},{"key":"scan-b","value":"two"}]`)
	seed := httptest.NewRecorder()
	s.handleTxn(seed, httptest.NewRequest(http.MethodPost, "/txn", bytes.NewReader(body)))
	if seed.Code != http.StatusNoContent {
		t.Fatalf("seed status=%d body=%q", seed.Code, seed.Body.String())
	}

	rr := httptest.NewRecorder()
	s.handleScan(rr, httptest.NewRequest(http.MethodGet, "/scan?prefix=scan-&limit=-1", nil))
	if rr.Code != http.StatusBadRequest || rr.Body.String() != "bad limit\n" {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
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

func TestHandleStorageFaultSetsListsAndClearsFailure(t *testing.T) {
	s := newTestService(t)

	requireStorageFault(t, s, false)

	set := httptest.NewRecorder()
	s.handleStorageFault(set, httptest.NewRequest(http.MethodPost, "/faults/storage", bytes.NewReader([]byte(`{"fail":true}`))))
	if set.Code != http.StatusNoContent {
		t.Fatalf("set status=%d body=%q", set.Code, set.Body.String())
	}
	requireStorageFault(t, s, true)

	clearByPost := httptest.NewRecorder()
	s.handleStorageFault(clearByPost, httptest.NewRequest(http.MethodPost, "/faults/storage", bytes.NewReader([]byte(`{"fail":false}`))))
	if clearByPost.Code != http.StatusNoContent {
		t.Fatalf("clear post status=%d body=%q", clearByPost.Code, clearByPost.Body.String())
	}
	requireStorageFault(t, s, false)

	setAgain := httptest.NewRecorder()
	s.handleStorageFault(setAgain, httptest.NewRequest(http.MethodPost, "/faults/storage", bytes.NewReader([]byte(`{"fail":true}`))))
	if setAgain.Code != http.StatusNoContent {
		t.Fatalf("set again status=%d body=%q", setAgain.Code, setAgain.Body.String())
	}

	clearByDelete := httptest.NewRecorder()
	s.handleStorageFault(clearByDelete, httptest.NewRequest(http.MethodDelete, "/faults/storage", nil))
	if clearByDelete.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%q", clearByDelete.Code, clearByDelete.Body.String())
	}
	requireStorageFault(t, s, false)
}

func TestHandleStorageFaultRejectsMalformedRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   []byte
		want   int
	}{
		{name: "wrong method", method: http.MethodPut, want: http.StatusMethodNotAllowed},
		{name: "malformed json", method: http.MethodPost, body: []byte(`{"fail":`), want: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			s.setStorageFault(true)
			rr := httptest.NewRecorder()
			s.handleStorageFault(rr, httptest.NewRequest(tc.method, "/faults/storage", bytes.NewReader(tc.body)))
			if rr.Code != tc.want {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			requireStorageFault(t, s, true)
		})
	}
}

func TestStorageFaultRejectsClientRequestsBeforeConsensusProgress(t *testing.T) {
	tests := []struct {
		name   string
		method string
		target string
		body   []byte
		handle func(*service, http.ResponseWriter, *http.Request)
	}{
		{name: "put", method: http.MethodPut, target: "/kv/alpha", body: []byte("one"), handle: (*service).handleKV},
		{name: "delete", method: http.MethodDelete, target: "/kv/alpha", handle: (*service).handleKV},
		{name: "get", method: http.MethodGet, target: "/kv/alpha", handle: (*service).handleKV},
		{name: "txn", method: http.MethodPost, target: "/txn", body: []byte(`[{"key":"alpha","value":"one"}]`), handle: (*service).handleTxn},
		{name: "scan barrier", method: http.MethodGet, target: "/scan?barrier=alpha", handle: (*service).handleScan},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			set := httptest.NewRecorder()
			s.handleStorageFault(set, httptest.NewRequest(http.MethodPost, "/faults/storage", bytes.NewReader([]byte(`{"fail":true}`))))
			if set.Code != http.StatusNoContent {
				t.Fatalf("set fault status=%d body=%q", set.Code, set.Body.String())
			}

			rr := httptest.NewRecorder()
			tc.handle(s, rr, httptest.NewRequest(tc.method, tc.target, bytes.NewReader(tc.body)))
			if rr.Code != http.StatusServiceUnavailable || rr.Body.String() != "storage fault active\n" {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestStorageFaultRejectsInboundMessageBeforeSteppingNode(t *testing.T) {
	s := newTestClusterService(t, []epaxos.ReplicaID{1, 2})
	s.setStorageFault(true)
	msg := epaxos.Message{
		Type:    epaxos.MsgPreAccept,
		From:    2,
		To:      1,
		Ref:     epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		Ballot:  epaxos.Ballot{Replica: 2},
		Seq:     1,
		Deps:    []epaxos.InstanceNum{0, 0},
		Command: epaxos.Command{ID: epaxos.CommandID{Client: 2, Sequence: 1}, ConflictKeys: [][]byte{[]byte("blocked")}},
	}
	buf, err := epaxos.EncodeMessage(nil, msg)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	s.handleMessage(rr, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(buf)))
	if rr.Code != http.StatusServiceUnavailable || rr.Body.String() != "storage fault active\n" {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	requireNoConsensusProgress(t, s)
}

func TestHandleTransportFaultSetsListsAndClearsDroppedLinks(t *testing.T) {
	s := newTestService(t)

	setFirst := httptest.NewRecorder()
	s.handleTransportFault(setFirst, httptest.NewRequest(http.MethodPost, "/faults/transport", bytes.NewReader([]byte(`{"from":2,"to":1,"drop":true}`))))
	if setFirst.Code != http.StatusNoContent {
		t.Fatalf("set first status=%d body=%q", setFirst.Code, setFirst.Body.String())
	}
	setSecond := httptest.NewRecorder()
	s.handleTransportFault(setSecond, httptest.NewRequest(http.MethodPost, "/faults/transport", bytes.NewReader([]byte(`{"from":1,"to":3,"drop":true}`))))
	if setSecond.Code != http.StatusNoContent {
		t.Fatalf("set second status=%d body=%q", setSecond.Code, setSecond.Body.String())
	}

	list := httptest.NewRecorder()
	s.handleTransportFault(list, httptest.NewRequest(http.MethodGet, "/faults/transport", nil))
	if list.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%q", list.Code, list.Body.String())
	}
	requireTransportDrops(t, list.Body.Bytes(), []transportFaultRequest{
		{From: 1, To: 3, Drop: true},
		{From: 2, To: 1, Drop: true},
	})

	clearOne := httptest.NewRecorder()
	s.handleTransportFault(clearOne, httptest.NewRequest(http.MethodPost, "/faults/transport", bytes.NewReader([]byte(`{"from":1,"to":3,"drop":false}`))))
	if clearOne.Code != http.StatusNoContent {
		t.Fatalf("clear one status=%d body=%q", clearOne.Code, clearOne.Body.String())
	}
	listAfterClearOne := httptest.NewRecorder()
	s.handleTransportFault(listAfterClearOne, httptest.NewRequest(http.MethodGet, "/faults/transport", nil))
	requireTransportDrops(t, listAfterClearOne.Body.Bytes(), []transportFaultRequest{{From: 2, To: 1, Drop: true}})

	clearAll := httptest.NewRecorder()
	s.handleTransportFault(clearAll, httptest.NewRequest(http.MethodDelete, "/faults/transport", nil))
	if clearAll.Code != http.StatusNoContent {
		t.Fatalf("clear all status=%d body=%q", clearAll.Code, clearAll.Body.String())
	}
	listAfterClearAll := httptest.NewRecorder()
	s.handleTransportFault(listAfterClearAll, httptest.NewRequest(http.MethodGet, "/faults/transport", nil))
	requireTransportDrops(t, listAfterClearAll.Body.Bytes(), nil)
}

func TestHandleTransportFaultRejectsMalformedRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   []byte
		want   int
	}{
		{name: "wrong method", method: http.MethodPut, want: http.StatusMethodNotAllowed},
		{name: "malformed json", method: http.MethodPost, body: []byte(`{"from":`), want: http.StatusBadRequest},
		{name: "zero source", method: http.MethodPost, body: []byte(`{"from":0,"to":1,"drop":true}`), want: http.StatusBadRequest},
		{name: "zero destination", method: http.MethodPost, body: []byte(`{"from":1,"to":0,"drop":true}`), want: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			rr := httptest.NewRecorder()
			s.handleTransportFault(rr, httptest.NewRequest(tc.method, "/faults/transport", bytes.NewReader(tc.body)))
			if rr.Code != tc.want {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			list := httptest.NewRecorder()
			s.handleTransportFault(list, httptest.NewRequest(http.MethodGet, "/faults/transport", nil))
			requireTransportDrops(t, list.Body.Bytes(), nil)
		})
	}
}

func TestSendDropsConfiguredOutgoingTransportLinkWithoutPostingOrQueueing(t *testing.T) {
	s := newTestService(t)
	s.peers[2] = "http://peer-2"
	posts := 0
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		posts++
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	s.setTransportDrop(1, 2, true)

	msg := epaxos.Message{Type: epaxos.MsgCommit, From: 1, To: 2, Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Deps: []epaxos.InstanceNum{0}}
	s.send([]epaxos.Message{msg})

	if posts != 0 {
		t.Fatalf("posts=%d", posts)
	}
	if len(s.sendq) != 0 {
		t.Fatalf("queued messages=%d", len(s.sendq))
	}
}

func TestHandleMessageDropsConfiguredInboundTransportLinkBeforeSteppingNode(t *testing.T) {
	s := newTestClusterService(t, []epaxos.ReplicaID{1, 2})
	s.setTransportDrop(2, 1, true)
	msg := epaxos.Message{
		Type:    epaxos.MsgPreAccept,
		From:    2,
		To:      1,
		Ref:     epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		Ballot:  epaxos.Ballot{Replica: 2},
		Seq:     1,
		Deps:    []epaxos.InstanceNum{0, 0},
		Command: epaxos.Command{ID: epaxos.CommandID{Client: 2, Sequence: 1}, ConflictKeys: [][]byte{[]byte("blocked")}},
	}
	buf, err := epaxos.EncodeMessage(nil, msg)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	s.handleMessage(rr, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(buf)))
	if rr.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	status := s.node.Status()
	if len(status.Instances) != 0 || len(status.Executed) != 0 {
		t.Fatalf("node status after dropped message: instances=%v executed=%v", status.Instances, status.Executed)
	}
}

func requireStorageFault(t *testing.T, s *service, want bool) {
	t.Helper()
	rr := httptest.NewRecorder()
	s.handleStorageFault(rr, httptest.NewRequest(http.MethodGet, "/faults/storage", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%q", rr.Code, rr.Body.String())
	}
	var got storageFaultRequest
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Fail != want {
		t.Fatalf("storage fault active=%t, want %t", got.Fail, want)
	}
}

func requireNoConsensusProgress(t *testing.T, s *service) {
	t.Helper()
	status := s.node.Status()
	if len(status.Instances) != 0 || len(status.Executed) != 0 {
		t.Fatalf("node status after storage fault: instances=%v executed=%v", status.Instances, status.Executed)
	}
}

func requireTransportDrops(t *testing.T, body []byte, want []transportFaultRequest) {
	t.Helper()
	var got []transportFaultRequest
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("drops=%v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("drops=%v, want %v", got, want)
		}
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
	store := db.EPaxosStorage()
	node, err := epaxos.NewRawNode(epaxos.Config{ID: 1, Voters: []epaxos.ReplicaID{1}, Storage: store, RetryTicks: 2, RecoveryTicks: 5, TimeOptimization: true, TimeOptimizationTicks: 1})
	if err != nil {
		t.Fatal(err)
	}
	return &service{id: 1, node: node, ready: db, db: db, peers: map[epaxos.ReplicaID]string{1: "http://127.0.0.1"}, client: &http.Client{}, sendq: make(chan epaxos.Message, 8), nextSeq: 1}
}

func newTestClusterService(t *testing.T, voters []epaxos.ReplicaID) *service {
	t.Helper()
	db, err := kv.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := db.EPaxosStorage()
	node, err := epaxos.NewRawNode(epaxos.Config{ID: 1, Voters: voters, Storage: store, RetryTicks: 2, RecoveryTicks: 5, TimeOptimization: true, TimeOptimizationTicks: 1})
	if err != nil {
		t.Fatal(err)
	}
	peers := make(map[epaxos.ReplicaID]string, len(voters))
	for _, voter := range voters {
		peers[voter] = "http://127.0.0.1"
	}
	return &service{id: 1, node: node, ready: db, db: db, peers: peers, client: &http.Client{}, sendq: make(chan epaxos.Message, 8), nextSeq: 1}
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
