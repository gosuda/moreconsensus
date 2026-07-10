package main

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"

	"gosuda.org/moreconsensus/epaxos"
)

type proxyFixture struct {
	proxy    *FaultProxy
	upstream *httptest.Server
	client   *http.Client
	mu       sync.Mutex
	bodies   [][]byte
}

func newProxyFixture(t *testing.T) *proxyFixture {
	t.Helper()
	fixture := &proxyFixture{client: &http.Client{Timeout: time.Second}}
	fixture.upstream = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		fixture.mu.Lock()
		fixture.bodies = append(fixture.bodies, append([]byte(nil), body...))
		fixture.mu.Unlock()
		writer.Header().Set("X-Upstream", "yes")
		writer.WriteHeader(http.StatusNoContent)
	}))
	upstreamURL, err := url.Parse(fixture.upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixture.proxy = newFaultProxy(listener, upstreamURL, nil)
	if err := fixture.proxy.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = fixture.proxy.Close()
		fixture.upstream.Close()
	})
	return fixture
}

func proxyMessage(t *testing.T, from, to epaxos.ReplicaID, instance epaxos.InstanceNum) []byte {
	t.Helper()
	message := epaxos.Message{
		Type: epaxos.MsgPreAccept, From: from, To: to,
		Ref: epaxos.InstanceRef{Replica: from, Instance: instance, Conf: 1},
		Ballot: epaxos.Ballot{Replica: from}, RecordBallot: epaxos.Ballot{Replica: from},
		Seq: 1, Deps: []epaxos.InstanceNum{0, 0}, RecordStatus: epaxos.StatusPreAccepted,
		Command: epaxos.Command{ID: epaxos.CommandID{Client: uint64(from), Sequence: uint64(instance)}, Payload: []byte("value"), ConflictKeys: [][]byte{[]byte("key")}},
	}
	body, err := epaxos.EncodeMessage(nil, message)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func (f *proxyFixture) post(t *testing.T, body []byte) int {
	t.Helper()
	response, err := f.client.Post(f.proxy.URL()+"/epaxos/message?test=yes", "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

func (f *proxyFixture) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.bodies)
}

func TestFaultProxyPassDropAndDuplicate(t *testing.T) {
	fixture := newProxyFixture(t)
	body := proxyMessage(t, 1, 2, 1)
	if status := fixture.post(t, body); status != http.StatusNoContent || fixture.count() != 1 {
		t.Fatalf("pass status=%d upstream=%d", status, fixture.count())
	}
	if err := fixture.proxy.Schedule(ProxyAction{ID: 1, Kind: "drop", From: 1, To: 2}); err != nil {
		t.Fatal(err)
	}
	if status := fixture.post(t, body); status != http.StatusNoContent || fixture.count() != 1 {
		t.Fatalf("drop status=%d upstream=%d", status, fixture.count())
	}
	if err := fixture.proxy.Schedule(ProxyAction{ID: 2, Kind: "duplicate", From: 1, To: 2}); err != nil {
		t.Fatal(err)
	}
	if status := fixture.post(t, body); status != http.StatusNoContent || fixture.count() != 3 {
		t.Fatalf("duplicate status=%d upstream=%d", status, fixture.count())
	}
	receipts := fixture.proxy.Receipts()
	if len(receipts) != 2 || receipts[0].ID == 0 || receipts[0].Kind != "drop" || receipts[1].Kind != "duplicate" || receipts[1].Copies != 2 {
		t.Fatalf("receipts=%#v", receipts)
	}
}

func TestFaultProxyDelayAndExplicitReverseRelease(t *testing.T) {
	fixture := newProxyFixture(t)
	if err := fixture.proxy.Schedule(
		ProxyAction{ID: 1, Kind: "delay", From: 1, To: 2},
		ProxyAction{ID: 2, Kind: "delay", From: 1, To: 2},
	); err != nil {
		t.Fatal(err)
	}
	if status := fixture.post(t, proxyMessage(t, 1, 2, 1)); status != http.StatusNoContent {
		t.Fatalf("first delay status=%d", status)
	}
	if status := fixture.post(t, proxyMessage(t, 1, 2, 2)); status != http.StatusNoContent {
		t.Fatalf("second delay status=%d", status)
	}
	if fixture.count() != 0 || !reflect.DeepEqual(fixture.proxy.HeldIDs(), []uint64{1, 2}) {
		t.Fatalf("upstream=%d held=%v", fixture.count(), fixture.proxy.HeldIDs())
	}
	release, err := fixture.proxy.Release([]uint64{2, 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(release) != 1 || !reflect.DeepEqual(release[0].ReleasedMessageIDs, []uint64{2, 1}) || fixture.count() != 2 {
		t.Fatalf("release=%#v upstream=%d", release, fixture.count())
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	var instances []epaxos.InstanceNum
	for _, body := range fixture.bodies {
		var message epaxos.Message
		if err := epaxos.DecodeMessage(body, &message); err != nil {
			t.Fatal(err)
		}
		instances = append(instances, message.Ref.Instance)
	}
	if !reflect.DeepEqual(instances, []epaxos.InstanceNum{2, 1}) {
		t.Fatalf("release order=%v", instances)
	}
}

func TestFaultProxyMatchesDirectedLinkAndPassesMalformed(t *testing.T) {
	fixture := newProxyFixture(t)
	if err := fixture.proxy.Schedule(ProxyAction{ID: 1, Kind: "drop", From: 9, To: 2}); err != nil {
		t.Fatal(err)
	}
	if status := fixture.post(t, []byte("malformed")); status != http.StatusNoContent {
		t.Fatalf("malformed pass status=%d", status)
	}
	if status := fixture.post(t, proxyMessage(t, 1, 2, 1)); status != http.StatusNoContent {
		t.Fatalf("unrelated pass status=%d", status)
	}
	if status := fixture.post(t, proxyMessage(t, 9, 2, 1)); status != http.StatusNoContent {
		t.Fatalf("matching drop status=%d", status)
	}
	if fixture.count() != 2 {
		t.Fatalf("upstream count=%d, want malformed and unrelated only", fixture.count())
	}
}

func TestFaultProxyDuplicateFailsIfEitherForwardFails(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 2 {
			http.Error(writer, "second copy rejected", http.StatusInternalServerError)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := newFaultProxy(listener, upstreamURL, nil)
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if err := proxy.Schedule(ProxyAction{ID: 1, Kind: "duplicate", From: 1, To: 2}); err != nil {
		t.Fatal(err)
	}
	response, err := (&http.Client{Timeout: time.Second}).Post(proxy.URL()+"/epaxos/message", "application/octet-stream", bytes.NewReader(proxyMessage(t, 1, 2, 1)))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadGateway || calls != 2 {
		t.Fatalf("duplicate status=%d calls=%d, want 502 and two attempts", response.StatusCode, calls)
	}
	receipts := proxy.Receipts()
	if len(receipts) != 1 || receipts[0].Copies != 1 || receipts[0].HTTPStatus != http.StatusBadGateway {
		t.Fatalf("duplicate failure receipt=%#v", receipts)
	}
}

func TestFaultProxyRejectsInvalidSchedulesAndOversizedBodies(t *testing.T) {
	fixture := newProxyFixture(t)
	for _, actions := range [][]ProxyAction{
		{{Kind: "drop"}},
		{{ID: 1, Kind: "unknown"}},
		{{ID: 2, Kind: "drop"}, {ID: 2, Kind: "pass"}},
	} {
		if err := fixture.proxy.Schedule(actions...); err == nil {
			t.Fatalf("schedule accepted %#v", actions)
		}
	}
	if err := fixture.proxy.Schedule(ProxyAction{ID: 3, Kind: "drop"}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.proxy.Schedule(ProxyAction{ID: 3, Kind: "drop"}); err == nil {
		t.Fatal("proxy accepted previously scheduled action ID")
	}
	if status := fixture.post(t, bytes.Repeat([]byte{'x'}, int(maxProxyBodyBytes)+1)); status != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status=%d", status)
	}
}

func TestFaultProxyReceiptCopiesAndCloseAreDefensive(t *testing.T) {
	fixture := newProxyFixture(t)
	if err := fixture.proxy.Schedule(ProxyAction{ID: 1, Kind: "delay", From: 1, To: 2}); err != nil {
		t.Fatal(err)
	}
	fixture.post(t, proxyMessage(t, 1, 2, 1))
	release, err := fixture.proxy.Release([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	release[0].ReleasedMessageIDs[0] = 99
	receipts := fixture.proxy.Receipts()
	receipts[len(receipts)-1].ReleasedMessageIDs[0] = 77
	fresh := fixture.proxy.Receipts()
	if got := fresh[len(fresh)-1].ReleasedMessageIDs[0]; got != 1 {
		t.Fatalf("receipt ownership leaked, got %d", got)
	}
	if err := fixture.proxy.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.proxy.Close(); err != nil {
		t.Fatal(err)
	}
}
