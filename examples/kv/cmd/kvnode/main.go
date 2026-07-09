//go:build kvnode

// Command kvnode runs the Pebble-backed EPaxos key-value example.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"gosuda.org/moreconsensus/epaxos"
	"gosuda.org/moreconsensus/examples/kv"
)

type readyApplier interface {
	ApplyReady(epaxos.Ready) error
}

type service struct {
	mu                 sync.Mutex
	faultMu            sync.RWMutex
	id                 epaxos.ReplicaID
	node               *epaxos.RawNode
	ready              readyApplier
	db                 *kv.DB
	peers              map[epaxos.ReplicaID]string
	client             *http.Client
	sendq              chan epaxos.Message
	nextSeq            uint64
	transportDrops     map[transportLink]struct{}
	storageFailed      bool
	requestDeadline    time.Duration
	maxClientBodyBytes int64
	maxPeerBodyBytes   int64
	maxAdminBodyBytes  int64
	maxScanLimit       int
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

const (
	maxDurationMillis        = int64(1<<63-1) / int64(time.Millisecond)
	maxRequestBodySize      = int64(1<<63 - 2)
	defaultMaxClientBodySize = int64(1 << 20)
	defaultMaxPeerBodySize   = int64(1 << 20)
	defaultMaxAdminBodySize  = int64(64 << 10)
	defaultMaxScanLimit      = 1024
)

var (
	errRequestBodyTooLarge = errors.New("request body too large")
	scanBarrierConflictKey = []byte("\x00kvnode-scan-barrier")
)

func main() {
	idFlag := flag.Uint64("id", 1, "replica id")
	listen := flag.String("listen", ":8080", "client HTTP listen address")
	peerListen := flag.String("peer-listen", ":8081", "peer HTTP listen address")
	adminListen := flag.String("admin-listen", ":8082", "admin HTTP listen address")
	data := flag.String("data", "kvnode-data", "Pebble data directory")
	peersFlag := flag.String("peers", "1=http://127.0.0.1:8081", "comma-separated id=peer-url entries")
	requestDeadlineMS := flag.Int("request-deadline-ms", 5000, "client-facing HTTP deadline budget in milliseconds")
	peerDeadlineMS := flag.Int("peer-deadline-ms", 2000, "peer HTTP deadline budget in milliseconds")
	tlsCert := flag.String("tls-cert", "", "TLS certificate chain PEM for client, peer, and admin listeners")
	tlsKey := flag.String("tls-key", "", "TLS private key PEM for client, peer, and admin listeners")
	tlsCA := flag.String("tls-ca", "", "optional CA bundle PEM used by the peer HTTP client")
	maxClientBodyBytes := flag.Int64("max-client-body-bytes", defaultMaxClientBodySize, "maximum bytes accepted for client write and transaction request bodies")
	maxPeerBodyBytes := flag.Int64("max-peer-body-bytes", defaultMaxPeerBodySize, "maximum bytes accepted for peer replication message bodies")
	maxAdminBodyBytes := flag.Int64("max-admin-body-bytes", defaultMaxAdminBodySize, "maximum bytes accepted for administrative request bodies")
	maxScanLimitFlag := flag.Int("max-scan-limit", defaultMaxScanLimit, "maximum rows returned by one scan request")
	flag.Parse()
	requestDeadline, err := durationFromMillis("request deadline", *requestDeadlineMS)
	if err != nil {
		log.Fatal(err)
	}
	peerDeadline, err := durationFromMillis("peer deadline", *peerDeadlineMS)
	if err != nil {
		log.Fatal(err)
	}
	serverTLSConfig, clientTLSConfig, err := parseTLSConfig(*tlsCert, *tlsKey, *tlsCA)
	if err != nil {
		log.Fatal(err)
	}
	maxClientBody, err := positiveBytes("max client body bytes", *maxClientBodyBytes)
	if err != nil {
		log.Fatal(err)
	}
	maxPeerBody, err := positiveBytes("max peer body bytes", *maxPeerBodyBytes)
	if err != nil {
		log.Fatal(err)
	}
	maxAdminBody, err := positiveBytes("max admin body bytes", *maxAdminBodyBytes)
	if err != nil {
		log.Fatal(err)
	}
	maxScanLimit, err := positiveInt("max scan limit", *maxScanLimitFlag)
	if err != nil {
		log.Fatal(err)
	}
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
	s := &service{id: epaxos.ReplicaID(*idFlag), node: node, ready: db, db: db, peers: peers, client: newPeerClient(peerDeadline, clientTLSConfig), sendq: make(chan epaxos.Message, 1024), nextSeq: 1, transportDrops: make(map[transportLink]struct{}), requestDeadline: requestDeadline, maxClientBodyBytes: maxClientBody, maxPeerBodyBytes: maxPeerBody, maxAdminBodyBytes: maxAdminBody, maxScanLimit: maxScanLimit}
	for range 8 {
		go s.transportWorker()
	}
	clientMux := s.clientMux()
	peerMux := s.peerMux()
	adminMux := s.adminMux()
	log.Printf("kvnode %d listening on client=%s peer=%s admin=%s", s.id, *listen, *peerListen, *adminListen)
	log.Fatal(serveHTTPServers(
		newHTTPServer(*listen, clientMux, requestDeadline, serverTLSConfig),
		newHTTPServer(*peerListen, peerMux, peerDeadline, serverTLSConfig),
		newHTTPServer(*adminListen, adminMux, requestDeadline, serverTLSConfig),
	))
}

func durationFromMillis(name string, ms int) (time.Duration, error) {
	if ms <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	if int64(ms) > maxDurationMillis {
		return 0, fmt.Errorf("%s too large", name)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func positiveBytes(name string, n int64) (int64, error) {
	if n <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	if n > maxRequestBodySize {
		return 0, fmt.Errorf("%s too large", name)
	}
	return n, nil
}

func positiveInt(name string, n int) (int, error) {
	if n <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return n, nil
}

func newPeerClient(deadline time.Duration, tlsConfig *tls.Config) *http.Client {
	client := &http.Client{Timeout: deadline}
	if tlsConfig != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConfig
		client.Transport = transport
	}
	return client
}

func newHTTPServer(addr string, handler http.Handler, deadline time.Duration, tlsConfig *tls.Config) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		TLSConfig:         tlsConfig,
		ReadHeaderTimeout: deadline,
		ReadTimeout:       deadline,
		WriteTimeout:      deadline,
		IdleTimeout:       deadline,
	}
}

func parseTLSConfig(certFile, keyFile, caFile string) (*tls.Config, *tls.Config, error) {
	if (certFile == "") != (keyFile == "") {
		return nil, nil, fmt.Errorf("tls-cert and tls-key must be set together")
	}
	var roots *x509.CertPool
	if caFile != "" {
		var err error
		roots, err = loadCertPool(caFile)
		if err != nil {
			return nil, nil, err
		}
	}
	clientTLSConfig := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	if certFile == "" {
		if roots == nil {
			return nil, nil, nil
		}
		return nil, clientTLSConfig, nil
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, nil, err
	}
	serverTLSConfig := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
	return serverTLSConfig, clientTLSConfig, nil
}

func loadCertPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no CA certificates in %s", path)
	}
	return roots, nil
}

func (s *service) requestContext(parent context.Context) (context.Context, context.CancelFunc) {
	if s.requestDeadline <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, s.requestDeadline)
}

func (s *service) clientMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/txn", s.handleTxn)
	mux.HandleFunc("/scan", s.handleScan)
	return mux
}

func (s *service) peerMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/epaxos/message", s.handleMessage)
	return mux
}

func (s *service) adminMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/faults/transport", s.handleTransportFault)
	mux.HandleFunc("/faults/storage", s.handleStorageFault)
	mux.HandleFunc("/health", s.handleLive)
	mux.HandleFunc("/livez", s.handleLive)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/metrics", s.handleMetrics)
	return mux
}

func serveHTTPServers(servers ...*http.Server) error {
	errc := make(chan error, len(servers))
	for _, srv := range servers {
		srv := srv
		go func() {
			if srv.TLSConfig != nil {
				errc <- srv.ListenAndServeTLS("", "")
				return
			}
			errc <- srv.ListenAndServe()
		}()
	}
	err := <-errc
	for _, srv := range servers {
		_ = srv.Close()
	}
	return err
}

func readLimitedBody(body io.Reader, limit int64) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > limit {
		return nil, errRequestBodyTooLarge
	}
	return payload, nil
}

func writeBodyReadError(w http.ResponseWriter, err error) {
	if errors.Is(err, errRequestBodyTooLarge) {
		http.Error(w, errRequestBodyTooLarge.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	http.Error(w, err.Error(), http.StatusBadRequest)
}

func configuredBodyLimit(configured, fallback int64) int64 {
	if configured > 0 {
		return configured
	}
	return fallback
}

func (s *service) clientBodyLimit() int64 {
	return configuredBodyLimit(s.maxClientBodyBytes, defaultMaxClientBodySize)
}

func (s *service) peerBodyLimit() int64 {
	return configuredBodyLimit(s.maxPeerBodyBytes, defaultMaxPeerBodySize)
}

func (s *service) adminBodyLimit() int64 {
	return configuredBodyLimit(s.maxAdminBodyBytes, defaultMaxAdminBodySize)
}

func (s *service) scanLimit() int {
	if s.maxScanLimit > 0 {
		return s.maxScanLimit
	}
	return defaultMaxScanLimit
}
func (s *service) handleLive(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
}

func (s *service) handleReady(w http.ResponseWriter, _ *http.Request) {
	if s.storageFaultActive() {
		http.Error(w, "storage fault active", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ready"))
}

func (s *service) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	status := s.node.Status()
	queueDepth := len(s.sendq)
	s.mu.Unlock()

	storageFaultActive := 0
	if s.storageFaultActive() {
		storageFaultActive = 1
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_storage_fault_active gauge\nkvnode_storage_fault_active %d\n", storageFaultActive)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_transport_dropped_links gauge\nkvnode_transport_dropped_links %d\n", s.transportDropCount())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_epaxos_instances gauge\nkvnode_epaxos_instances %d\n", len(status.Instances))
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_epaxos_executed gauge\nkvnode_epaxos_executed %d\n", len(status.Executed))
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_send_queue_depth gauge\nkvnode_send_queue_depth %d\n", queueDepth)
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
	ctx, cancel := s.requestContext(r.Context())
	defer cancel()
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
			if err := s.waitForKeys(ctx, key); err != nil {
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
		body, err := readLimitedBody(r.Body, s.clientBodyLimit())
		if err != nil {
			writeBodyReadError(w, err)
			return
		}
		if err := s.proposeAndWait(ctx, withScanBarrierConflict(kv.CommandForPut(uint64(s.id), s.next(), key, body))); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if s.storageFaultActive() {
			http.Error(w, "storage fault active", http.StatusServiceUnavailable)
			return
		}
		if err := s.proposeAndWait(ctx, withScanBarrierConflict(kv.CommandForDelete(uint64(s.id), s.next(), key))); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

type txnRequestOp struct {
	Delete      bool    `json:"delete"`
	Key         string  `json:"key"`
	Value       *string `json:"value,omitempty"`
	ValueBase64 *string `json:"value_b64,omitempty"`
}

func txnOpValue(op txnRequestOp) ([]byte, error) {
	if op.Value != nil && op.ValueBase64 != nil {
		return nil, fmt.Errorf("ambiguous value encoding")
	}
	if op.ValueBase64 != nil {
		value, err := base64.StdEncoding.DecodeString(*op.ValueBase64)
		if err != nil {
			return nil, fmt.Errorf("bad value_b64")
		}
		return value, nil
	}
	if op.Value == nil {
		return nil, nil
	}
	return []byte(*op.Value), nil
}

func (s *service) handleTxn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request []txnRequestOp
	body, err := readLimitedBody(r.Body, s.clientBodyLimit())
	if err != nil {
		writeBodyReadError(w, err)
		return
	}
	if err := json.Unmarshal(body, &request); err != nil {
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
		value, err := txnOpValue(op)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ops = append(ops, kv.TxnOp{Delete: op.Delete, Key: key, Value: value})
	}
	ctx, cancel := s.requestContext(r.Context())
	defer cancel()
	if s.storageFaultActive() {
		http.Error(w, "storage fault active", http.StatusServiceUnavailable)
		return
	}
	if err := s.proposeAndWait(ctx, withScanBarrierConflict(kv.CommandForTxn(uint64(s.id), s.next(), ops))); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type scanResponseKV struct {
	Key         string `json:"key"`
	Value       string `json:"value"`
	ValueBase64 string `json:"value_b64,omitempty"`
	Time        uint64 `json:"time"`
}

func scanResponseFromKV(row kv.KV) scanResponseKV {
	out := scanResponseKV{Key: string(row.Key), Time: row.Time}
	if utf8.Valid(row.Value) {
		out.Value = string(row.Value)
		return out
	}
	out.ValueBase64 = base64.StdEncoding.EncodeToString(row.Value)
	return out
}

func (s *service) handleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	maxLimit := s.scanLimit()
	limit := maxLimit
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if n <= 0 || n > maxLimit {
			http.Error(w, "bad limit", http.StatusBadRequest)
			return
		}
		limit = n
	}
	prefix := []byte(q.Get("prefix"))
	start := []byte(q.Get("start"))
	end := []byte(q.Get("end"))
	for _, key := range [][]byte{prefix, start, end} {
		if len(key) == 0 {
			continue
		}
		if err := kv.ValidateKey(key); err != nil {
			http.Error(w, "bad scan bounds", http.StatusBadRequest)
			return
		}
	}
	if len(prefix) > 0 && (len(start) > 0 || len(end) > 0) {
		http.Error(w, "bad scan bounds", http.StatusBadRequest)
		return
	}
	if len(prefix) == 0 {
		if len(start) == 0 || len(end) == 0 || bytes.Compare(start, end) >= 0 {
			http.Error(w, "bad scan bounds", http.StatusBadRequest)
			return
		}
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
	barrier := q.Get("barrier")
	if hasBounds && barrier != "" {
		http.Error(w, "bad timestamp selector", http.StatusBadRequest)
		return
	}
	ctx, cancel := s.requestContext(r.Context())
	defer cancel()
	if s.storageFaultActive() {
		http.Error(w, "storage fault active", http.StatusServiceUnavailable)
		return
	}
	if barrier != "" {
		parts := strings.Split(barrier, ",")
		keys := make([][]byte, 0, len(parts))
		for _, part := range parts {
			key := []byte(part)
			if err := kv.ValidateKey(key); err != nil {
				http.Error(w, "bad barrier key", http.StatusBadRequest)
				return
			}
			keys = append(keys, key)
		}
		if err := s.waitForKeys(ctx, keys...); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	} else if !hasBounds {
		if err := s.waitForScanBarrier(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	rows, err := s.db.Scan(kv.ScanOptions{
		Start:   start,
		End:     end,
		Prefix:  prefix,
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
		out[i] = scanResponseFromKV(row)
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
		body, err := readLimitedBody(r.Body, s.adminBodyLimit())
		if err != nil {
			writeBodyReadError(w, err)
			return
		}
		if err := json.Unmarshal(body, &req); err != nil {
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
		body, err := readLimitedBody(r.Body, s.adminBodyLimit())
		if err != nil {
			writeBodyReadError(w, err)
			return
		}
		if err := json.Unmarshal(body, &req); err != nil {
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

func (s *service) transportDropCount() int {
	s.faultMu.RLock()
	defer s.faultMu.RUnlock()
	return len(s.transportDrops)
}

func (s *service) transportDropped(from, to epaxos.ReplicaID) bool {
	s.faultMu.RLock()
	defer s.faultMu.RUnlock()
	_, ok := s.transportDrops[transportLink{from: from, to: to}]
	return ok
}

func (s *service) handleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := readLimitedBody(r.Body, s.peerBodyLimit())
	if err != nil {
		writeBodyReadError(w, err)
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

func withScanBarrierConflict(cmd epaxos.Command) epaxos.Command {
	for _, key := range cmd.ConflictKeys {
		if bytes.Equal(key, scanBarrierConflictKey) {
			return cmd
		}
	}
	cmd.ConflictKeys = append(cmd.ConflictKeys, append([]byte(nil), scanBarrierConflictKey...))
	return cmd
}

func (s *service) waitForScanBarrier(ctx context.Context) error {
	return s.proposeAndWait(ctx, epaxos.Command{
		ID:           epaxos.CommandID{Client: uint64(s.id), Sequence: s.next()},
		ConflictKeys: [][]byte{append([]byte(nil), scanBarrierConflictKey...)},
	})
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
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
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
