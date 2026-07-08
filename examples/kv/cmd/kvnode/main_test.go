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
		{name: "embedded separator key", method: http.MethodPost, body: []byte(`[{"key":"alpha\u0000beta","value":"x"}]`), want: http.StatusBadRequest},
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

func TestHandleKVHistoricalReadSelectorsReturnVisibleVersion(t *testing.T) {
	tests := []struct {
		name   string
		target string
		want   string
	}{
		{name: "at or before returns newest visible version", target: "/kv/hist?at=12", want: "mid"},
		{name: "interval returns newest version inside bounds", target: "/kv/hist?min-time=6&max-time=15", want: "mid"},
		{name: "exact timestamp returns only that version", target: "/kv/hist?exact-time=5", want: "old"},
		{name: "bounded staleness returns newest version in reference window", target: "/kv/hist?reference-time=15&max-staleness=7", want: "mid"},
		{name: "exact staleness returns version at reference offset", target: "/kv/hist?reference-time=15&exact-staleness=10", want: "old"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			seedHistoricalKV(t, s)

			rr := httptest.NewRecorder()
			s.handleKV(rr, httptest.NewRequest(http.MethodGet, tc.target, nil))
			if rr.Code != http.StatusOK || rr.Body.String() != tc.want {
				t.Fatalf("status=%d body=%q, want status=%d body=%q", rr.Code, rr.Body.String(), http.StatusOK, tc.want)
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestHandleKVHistoricalReadSelectorsReturnNotFoundForInvisibleVersion(t *testing.T) {
	tests := []struct {
		name   string
		target string
	}{
		{name: "at before first version", target: "/kv/hist?at=4"},
		{name: "exact timestamp without version", target: "/kv/hist?exact-time=6"},
		{name: "tombstone at selected timestamp", target: "/kv/hist?at=25"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			seedHistoricalKV(t, s)
			if err := s.db.DeleteVersion([]byte("hist"), 25); err != nil {
				t.Fatal(err)
			}

			rr := httptest.NewRecorder()
			s.handleKV(rr, httptest.NewRequest(http.MethodGet, tc.target, nil))
			if rr.Code != http.StatusNotFound {
				t.Fatalf("status=%d body=%q, want status=%d", rr.Code, rr.Body.String(), http.StatusNotFound)
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestHandleScanHistoricalReadSelectorsReturnVersionTimes(t *testing.T) {
	tests := []struct {
		name   string
		target string
		want   []scanResponseKV
	}{
		{
			name:   "at or before returns newest visible row per key",
			target: "/scan?prefix=scan-&at=8",
			want: []scanResponseKV{
				{Key: "scan-a", Value: "a-old", Time: 5},
				{Key: "scan-b", Value: "b-old", Time: 7},
			},
		},
		{
			name:   "interval omits keys without a live version inside bounds",
			target: "/scan?prefix=scan-&min-time=9&max-time=12",
			want: []scanResponseKV{
				{Key: "scan-a", Value: "a-new", Time: 10},
			},
		},
		{
			name:   "exact timestamp returns matching rows only",
			target: "/scan?prefix=scan-&exact-time=7",
			want: []scanResponseKV{
				{Key: "scan-b", Value: "b-old", Time: 7},
			},
		},
		{
			name:   "bounded staleness returns rows inside reference window",
			target: "/scan?prefix=scan-&reference-time=15&max-staleness=5",
			want: []scanResponseKV{
				{Key: "scan-a", Value: "a-new", Time: 10},
				{Key: "scan-b", Value: "b-new", Time: 15},
			},
		},
		{
			name:   "exact staleness returns rows at reference offset",
			target: "/scan?prefix=scan-&reference-time=15&exact-staleness=5",
			want: []scanResponseKV{
				{Key: "scan-a", Value: "a-new", Time: 10},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			seedHistoricalScan(t, s)

			rr := httptest.NewRecorder()
			s.handleScan(rr, httptest.NewRequest(http.MethodGet, tc.target, nil))
			if rr.Code != http.StatusOK {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			var rows []scanResponseKV
			if err := json.Unmarshal(rr.Body.Bytes(), &rows); err != nil {
				t.Fatal(err)
			}
			if len(rows) != len(tc.want) {
				t.Fatalf("rows=%v, want %v", rows, tc.want)
			}
			for i := range tc.want {
				if rows[i] != tc.want[i] {
					t.Fatalf("rows=%v, want %v", rows, tc.want)
				}
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestHandleKVRejectsMalformedTimestampSelectorsBeforeConsensusProgress(t *testing.T) {
	tests := malformedTimestampSelectorQueries()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			rr := httptest.NewRecorder()
			s.handleKV(rr, httptest.NewRequest(http.MethodGet, "/kv/hist?"+tc.query, nil))
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%q, want status=%d", rr.Code, rr.Body.String(), http.StatusBadRequest)
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestHandleScanRejectsMalformedTimestampSelectorsBeforeConsensusProgress(t *testing.T) {
	tests := malformedTimestampSelectorQueries()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			rr := httptest.NewRecorder()
			s.handleScan(rr, httptest.NewRequest(http.MethodGet, "/scan?prefix=scan-&"+tc.query, nil))
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%q, want status=%d", rr.Code, rr.Body.String(), http.StatusBadRequest)
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestStalenessReadsHonorStorageFaultGate(t *testing.T) {
	tests := []struct {
		name   string
		target string
		handle func(*service, http.ResponseWriter, *http.Request)
		seed   func(*testing.T, *service)
	}{
		{name: "kv at selector", target: "/kv/hist?at=12", handle: (*service).handleKV, seed: seedHistoricalKV},
		{name: "scan at selector", target: "/scan?prefix=scan-&at=8", handle: (*service).handleScan, seed: seedHistoricalScan},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			tc.seed(t, s)
			s.setStorageFault(true)

			rr := httptest.NewRecorder()
			tc.handle(s, rr, httptest.NewRequest(http.MethodGet, tc.target, nil))
			if rr.Code != http.StatusServiceUnavailable || rr.Body.String() != "storage fault active\n" {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestHandleKVLatestReadWithoutSelectorStillWaitsForConsensusBarrier(t *testing.T) {
	s := newTestService(t)
	if err := s.db.PutVersion([]byte("latest"), []byte("direct"), 7); err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	s.handleKV(rr, httptest.NewRequest(http.MethodGet, "/kv/latest", nil))
	if rr.Code != http.StatusOK || rr.Body.String() != "direct" {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	if !hasExecutedRef(s.node.Status().Executed, ref) {
		t.Fatalf("executed refs=%v, want %s", s.node.Status().Executed, ref)
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
		{name: "empty barrier part", method: http.MethodGet, target: "/scan?barrier=alpha,,beta", want: http.StatusBadRequest},
		{name: "embedded separator barrier key", method: http.MethodGet, target: "/scan?barrier=alpha%00beta", want: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			rr := httptest.NewRecorder()
			s.handleScan(rr, httptest.NewRequest(tc.method, tc.target, nil))
			if rr.Code != tc.want {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			if tc.want == http.StatusBadRequest {
				status := s.node.Status()
				if len(status.Instances) != 0 || len(status.Executed) != 0 {
					t.Fatalf("node status after rejected request: instances=%v executed=%v", status.Instances, status.Executed)
				}
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
	tests := []struct {
		name    string
		request *http.Request
		want    int
	}{
		{name: "empty key", request: httptest.NewRequest(http.MethodGet, "/kv/", nil), want: http.StatusBadRequest},
		{name: "bad escape", request: &http.Request{Method: http.MethodGet, URL: &url.URL{Path: "/kv/%zz"}, Body: http.NoBody}, want: http.StatusBadRequest},
		{name: "embedded separator key", request: &http.Request{Method: http.MethodPut, URL: &url.URL{Path: "/kv/alpha%00beta"}, Body: http.NoBody}, want: http.StatusBadRequest},
		{name: "wrong method", request: httptest.NewRequest(http.MethodPost, "/kv/alpha", nil), want: http.StatusMethodNotAllowed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			rr := httptest.NewRecorder()
			s.handleKV(rr, tc.request)
			if rr.Code != tc.want {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			status := s.node.Status()
			if rr.Code == http.StatusBadRequest && (len(status.Instances) != 0 || len(status.Executed) != 0) {
				t.Fatalf("node status after rejected request: instances=%v executed=%v", status.Instances, status.Executed)
			}
		})
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
		t.Fatalf("node status after request: instances=%v executed=%v", status.Instances, status.Executed)
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

func seedHistoricalKV(t *testing.T, s *service) {
	t.Helper()
	if err := s.db.PutVersion([]byte("hist"), []byte("old"), 5); err != nil {
		t.Fatal(err)
	}
	if err := s.db.PutVersion([]byte("hist"), []byte("mid"), 10); err != nil {
		t.Fatal(err)
	}
	if err := s.db.PutVersion([]byte("hist"), []byte("new"), 20); err != nil {
		t.Fatal(err)
	}
}

func seedHistoricalScan(t *testing.T, s *service) {
	t.Helper()
	if err := s.db.PutVersion([]byte("scan-a"), []byte("a-old"), 5); err != nil {
		t.Fatal(err)
	}
	if err := s.db.PutVersion([]byte("scan-a"), []byte("a-new"), 10); err != nil {
		t.Fatal(err)
	}
	if err := s.db.PutVersion([]byte("scan-b"), []byte("b-old"), 7); err != nil {
		t.Fatal(err)
	}
	if err := s.db.PutVersion([]byte("scan-b"), []byte("b-new"), 15); err != nil {
		t.Fatal(err)
	}
	if err := s.db.PutVersion([]byte("other"), []byte("outside-prefix"), 6); err != nil {
		t.Fatal(err)
	}
}

func malformedTimestampSelectorQueries() []struct {
	name  string
	query string
} {
	return []struct {
		name  string
		query string
	}{
		{name: "empty at", query: "at="},
		{name: "negative at", query: "at=-1"},
		{name: "empty interval minimum", query: "min-time=&max-time=5"},
		{name: "empty interval maximum", query: "min-time=1&max-time="},
		{name: "empty staleness reference", query: "reference-time=&max-staleness=2"},
		{name: "empty bounded staleness", query: "reference-time=10&max-staleness="},
		{name: "empty exact staleness", query: "reference-time=10&exact-staleness="},
		{name: "duplicate at", query: "at=1&at=2"},
		{name: "negative interval minimum", query: "min-time=-1&max-time=5"},
		{name: "negative bounded staleness", query: "reference-time=10&max-staleness=-1"},
		{name: "non-numeric exact timestamp", query: "exact-time=soon"},
		{name: "missing max-time partner", query: "min-time=1"},
		{name: "missing min-time partner", query: "max-time=2"},
		{name: "missing staleness partner", query: "reference-time=10"},
		{name: "missing reference for bounded staleness", query: "max-staleness=2"},
		{name: "missing reference for exact staleness", query: "exact-staleness=2"},
		{name: "invalid interval", query: "min-time=5&max-time=4"},
		{name: "duplicate interval param", query: "min-time=1&min-time=2&max-time=3"},
		{name: "mutually exclusive at and exact groups", query: "at=1&exact-time=1"},
		{name: "mutually exclusive bounded and exact staleness groups", query: "reference-time=10&max-staleness=2&exact-staleness=1"},
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
