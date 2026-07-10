//go:build kvnode

// Command kvnode runs the Pebble-backed EPaxos key-value example.
package main

import (
	"bytes"
	"container/heap"
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
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"gosuda.org/moreconsensus/epaxos"
	"gosuda.org/moreconsensus/examples/kv"
)

type readyApplier interface {
	ApplyReady(epaxos.Ready) error
}
type outboundFrame struct {
	to       epaxos.ReplicaID
	endpoint string
	body     []byte
	retryAttempt uint32
	retryAt      uint64
	retryOrder   uint64
	admitted     bool
}

type pooledTransportBuffer struct {
	bytes []byte
}
type outboundRetryHeap []*outboundFrame

func (h outboundRetryHeap) Len() int {
	return len(h)
}

func (h outboundRetryHeap) Less(i, j int) bool {
	if h[i].retryAt != h[j].retryAt {
		return h[i].retryAt < h[j].retryAt
	}
	return h[i].retryOrder < h[j].retryOrder
}

func (h outboundRetryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *outboundRetryHeap) Push(value any) {
	*h = append(*h, value.(*outboundFrame))
}

func (h *outboundRetryHeap) Pop() any {
	old := *h
	last := len(old) - 1
	frame := old[last]
	old[last] = nil
	*h = old[:last]
	return frame
}


type transportAdmissionError struct {
	Cause     error
	Attempted int
	Available int
}

func (e *transportAdmissionError) Error() string {
	if errors.Is(e.Cause, errSendQueueBackpressure) {
		return fmt.Sprintf("outbound transport backpressure: admit %d frames with %d queue slots available: %v", e.Attempted, e.Available, e.Cause)
	}
	return fmt.Sprintf("outbound transport backpressure: prepare %d frames: %v", e.Attempted, e.Cause)
}

func (e *transportAdmissionError) Unwrap() error {
	return e.Cause
}

type outboundFrameMetrics struct {
	attempted     atomic.Uint64
	admitted      atomic.Uint64
	retried       atomic.Uint64
	backpressured atomic.Uint64
	terminalFailed atomic.Uint64
	retryOverflow  atomic.Uint64
}

type postOutcome uint8

const (
	postSucceeded postOutcome = iota
	postRetry
	postTerminalFailure
)

const maxRetainedTransportBufferCapacity = 64 << 10

const maxWireMessageCollectionEntries = 128

const transportReadChunkSize = 4 << 10
const initialRetryDelayTicks = uint64(1)

const maxRetryDelayTicks = uint64(64)


var (
	errSendQueueBackpressure = errors.New("outbound send queue capacity exhausted")
	errTransportClosed       = errors.New("outbound transport closed")
	errOutboundFrameTooLarge = errors.New("outbound frame too large")
	errTransportTickOverflow  = errors.New("outbound transport logical tick overflow")
)

func uvarintSize(value uint64) int64 {
	size := int64(1)
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}

func encodedMessageSizeWithin(message epaxos.Message, limit int64) (int, error) {
	if len(message.Deps) > maxWireMessageCollectionEntries ||
		len(message.AcceptDeps) > maxWireMessageCollectionEntries ||
		len(message.AcceptEvidence) > maxWireMessageCollectionEntries ||
		len(message.Command.ConflictKeys) > maxWireMessageCollectionEntries {
		return 0, epaxos.ErrInvalidMessage
	}
	for _, evidence := range message.AcceptEvidence {
		if len(evidence.Deps) > maxWireMessageCollectionEntries {
			return 0, epaxos.ErrInvalidMessage
		}
	}

	size := int64(4 + 1 + 1 + 1 + 32) // Magic, three boolean flags, and checksum.
	add := func(n int64) bool {
		if n < 0 || size > limit || n > limit-size {
			return false
		}
		size += n
		return true
	}
	addUvarints := func(values ...uint64) bool {
		for _, value := range values {
			if !add(uvarintSize(value)) {
				return false
			}
		}
		return true
	}

	if !addUvarints(
		uint64(message.Type), uint64(message.From), uint64(message.To),
		uint64(message.Ref.Replica), uint64(message.Ref.Instance), uint64(message.Ref.Conf),
		message.ProcessAt,
		message.Ballot.Epoch, message.Ballot.Number, uint64(message.Ballot.Replica),
		message.RecordBallot.Epoch, message.RecordBallot.Number, uint64(message.RecordBallot.Replica),
		message.Seq, uint64(len(message.Deps)),
	) {
		return 0, errOutboundFrameTooLarge
	}
	for _, dependency := range message.Deps {
		if !add(uvarintSize(uint64(dependency))) {
			return 0, errOutboundFrameTooLarge
		}
	}
	if !addUvarints(message.AcceptSeq, uint64(len(message.AcceptDeps))) {
		return 0, errOutboundFrameTooLarge
	}
	for _, dependency := range message.AcceptDeps {
		if !add(uvarintSize(uint64(dependency))) {
			return 0, errOutboundFrameTooLarge
		}
	}
	if !add(uvarintSize(uint64(len(message.AcceptEvidence)))) {
		return 0, errOutboundFrameTooLarge
	}
	for _, evidence := range message.AcceptEvidence {
		if !addUvarints(uint64(evidence.Sender), evidence.Seq, uint64(len(evidence.Deps))) {
			return 0, errOutboundFrameTooLarge
		}
		for _, dependency := range evidence.Deps {
			if !add(uvarintSize(uint64(dependency))) {
				return 0, errOutboundFrameTooLarge
			}
		}
	}
	if !addUvarints(
		uint64(message.IgnoreDependency.Ref.Replica),
		uint64(message.IgnoreDependency.Ref.Instance),
		uint64(message.IgnoreDependency.Ref.Conf),
		message.Command.ID.Client,
		message.Command.ID.Sequence,
		uint64(message.Command.Kind),
		uint64(len(message.Command.Payload)),
	) || !add(int64(len(message.Command.Payload))) ||
		!add(uvarintSize(uint64(len(message.Command.ConflictKeys)))) {
		return 0, errOutboundFrameTooLarge
	}
	for _, key := range message.Command.ConflictKeys {
		if !add(uvarintSize(uint64(len(key)))) || !add(int64(len(key))) {
			return 0, errOutboundFrameTooLarge
		}
	}
	if !addUvarints(
		message.RejectHint.Epoch,
		message.RejectHint.Number,
		uint64(message.RejectHint.Replica),
		uint64(message.RejectReason),
		uint64(message.ConflictRef.Replica),
		uint64(message.ConflictRef.Instance),
		uint64(message.ConflictRef.Conf),
		uint64(message.ConflictStatus),
		message.DepsCommitted,
		uint64(message.RecordStatus),
	) {
		return 0, errOutboundFrameTooLarge
	}
	if size > int64(^uint(0)>>1) {
		return 0, errOutboundFrameTooLarge
	}
	return int(size), nil
}

func resetTransportBytes(payload []byte) []byte {
	if cap(payload) > maxRetainedTransportBufferCapacity {
		clear(payload[:cap(payload)])
		return nil
	}
	clear(payload[:cap(payload)])
	return payload[:0]
}

func readLimitedBodyInto(body io.Reader, limit int64, dst []byte) ([]byte, error) {
	dst = dst[:0]
	maximum := limit + 1
	emptyReads := 0
	for {
		if int64(len(dst)) > limit {
			return dst, errRequestBodyTooLarge
		}
		if int64(len(dst)) == maximum {
			return dst, errRequestBodyTooLarge
		}
		if len(dst) == cap(dst) {
			nextCapacity := int64(cap(dst) * 2)
			if nextCapacity < transportReadChunkSize {
				nextCapacity = transportReadChunkSize
			}
			if nextCapacity > maximum {
				nextCapacity = maximum
			}
			grown := make([]byte, len(dst), int(nextCapacity))
			copy(grown, dst)
			dst = grown
		}
		available := cap(dst) - len(dst)
		remaining := int(maximum - int64(len(dst)))
		if available > remaining {
			available = remaining
		}
		n, err := body.Read(dst[len(dst) : len(dst)+available])
		dst = dst[:len(dst)+n]
		if int64(len(dst)) > limit {
			return dst, errRequestBodyTooLarge
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return dst, nil
			}
			return dst, err
		}
		if n == 0 {
			emptyReads++
			if emptyReads >= 100 {
				return dst, io.ErrNoProgress
			}
			continue
		}
		emptyReads = 0
	}
}


type service struct {
	mu                 sync.Mutex
	faultMu            sync.RWMutex
	transportMu        sync.RWMutex
	transportWG        sync.WaitGroup
	transportCloseOnce sync.Once
	transportSchedulerOnce sync.Once
	id                 epaxos.ReplicaID
	node               *epaxos.RawNode
	ready              readyApplier
	db                 *kv.DB
	peers              map[epaxos.ReplicaID]string
	client             *http.Client
	sendq              chan *outboundFrame
	transportCtx       context.Context
	transportCancel    context.CancelFunc
	framePool          sync.Pool
	peerBodyPool       sync.Pool
	outboundMetrics    outboundFrameMetrics
	retryq             outboundRetryHeap
	transportWake      chan struct{}
	transportTick      uint64
	retryOrder         uint64
	outstandingFrames  int
	schedulerTick      atomic.Uint64
	nextSeq            uint64
	transportDrops     map[transportLink]struct{}
	storageFailed      bool
	transportClosed    bool
	requestDeadline    time.Duration
	maxClientBodyBytes int64
	maxPeerBodyBytes   int64
	maxAdminBodyBytes  int64
	maxScanLimit       int
	legacyProcessAtDomain string
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

type listenerFactory func(network, address string) (net.Listener, error)

type application struct {
	service         *service
	servers         []*http.Server
	storage         io.Closer
	listen          listenerFactory
	shutdownTimeout time.Duration
	closeOnce       sync.Once
	closeErr        error
}

const (
	maxDurationMillis        = int64(1<<63-1) / int64(time.Millisecond)
	maxRequestBodySize       = int64(1<<63 - 2)
	defaultMaxClientBodySize = int64(1 << 20)
	defaultMaxPeerBodySize   = int64(1 << 20)
	defaultMaxAdminBodySize  = int64(64 << 10)
	defaultMaxScanLimit      = 1024
	transportWorkerCount     = 8
	gracefulShutdownTimeout  = 15 * time.Second
)

var (
	errRequestBodyTooLarge = errors.New("request body too large")
	scanBarrierConflictKey = []byte("\x00kvnode-scan-barrier")
)

func main() {
	if err := runWithSignals(os.Args[1:], net.Listen); err != nil {
		log.Printf("kvnode: %v", err)
		os.Exit(1)
	}
}

func runWithSignals(args []string, listen listenerFactory) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runKVNode(ctx, args, listen)
}

func runKVNode(ctx context.Context, args []string, listen listenerFactory) error {
	flags := flag.NewFlagSet("kvnode", flag.ContinueOnError)
	idFlag := flags.Uint64("id", 1, "replica id")
	clientListen := flags.String("listen", ":8080", "client HTTP listen address")
	peerListen := flags.String("peer-listen", ":8081", "peer HTTP listen address")
	adminListen := flags.String("admin-listen", ":8082", "admin HTTP listen address")
	data := flags.String("data", "kvnode-data", "Pebble data directory")
	peersFlag := flags.String("peers", "1=http://127.0.0.1:8081", "comma-separated id=peer-url entries")
	requestDeadlineMS := flags.Int("request-deadline-ms", 5000, "client-facing HTTP deadline budget in milliseconds")
	peerDeadlineMS := flags.Int("peer-deadline-ms", 2000, "peer HTTP deadline budget in milliseconds")
	tlsCert := flags.String("tls-cert", "", "TLS certificate chain PEM for client, peer, and admin listeners")
	tlsKey := flags.String("tls-key", "", "TLS private key PEM for client, peer, and admin listeners")
	tlsCA := flags.String("tls-ca", "", "optional CA bundle PEM used by the peer HTTP client")
	maxClientBodyBytes := flags.Int64("max-client-body-bytes", defaultMaxClientBodySize, "maximum bytes accepted for client write and transaction request bodies")
	maxPeerBodyBytes := flags.Int64("max-peer-body-bytes", defaultMaxPeerBodySize, "maximum bytes accepted for peer replication message bodies")
	maxAdminBodyBytes := flags.Int64("max-admin-body-bytes", defaultMaxAdminBodySize, "maximum bytes accepted for administrative request bodies")
	maxScanLimitFlag := flags.Int("max-scan-limit", defaultMaxScanLimit, "maximum rows returned by one scan request")
	legacyProcessAtDomainFlag := flags.String("legacy-process-at-domain", "", "trusted migration assertion for legacy ProcessAt records: untimed, logical, or toq")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if listen == nil {
		return fmt.Errorf("listener factory is required")
	}
	requestDeadline, err := durationFromMillis("request deadline", *requestDeadlineMS)
	if err != nil {
		return err
	}
	peerDeadline, err := durationFromMillis("peer deadline", *peerDeadlineMS)
	if err != nil {
		return err
	}
	serverTLSConfig, clientTLSConfig, err := parseTLSConfig(*tlsCert, *tlsKey, *tlsCA)
	if err != nil {
		return err
	}
	maxClientBody, err := positiveBytes("max client body bytes", *maxClientBodyBytes)
	if err != nil {
		return err
	}
	maxPeerBody, err := positiveBytes("max peer body bytes", *maxPeerBodyBytes)
	if err != nil {
		return err
	}
	maxAdminBody, err := positiveBytes("max admin body bytes", *maxAdminBodyBytes)
	if err != nil {
		return err
	}
	maxScanLimit, err := positiveInt("max scan limit", *maxScanLimitFlag)
	if err != nil {
		return err
	}
	legacyProcessAtDomain, legacyProcessAtDomainMode, err := parseLegacyProcessAtDomain(*legacyProcessAtDomainFlag)
	if err != nil {
		return err
	}
	peers, voters, err := parsePeers(*peersFlag)
	if err != nil {
		return err
	}
	db, err := kv.Open(*data)
	if err != nil {
		return err
	}
	store := db.EPaxosStorage()
	// kvnode has no synchronized logical tick driver across replicas; keep this
	// request-driven service on classic EPaxos so time-optimized pre-accepts
	// cannot wait on slower peer ticks.
	node, err := epaxos.NewRawNode(epaxos.Config{ID: epaxos.ReplicaID(*idFlag), Voters: voters, Storage: store, RetryTicks: 2, RecoveryTicks: 5, LegacyProcessAtDomain: legacyProcessAtDomain})
	if err != nil {
		return errorsJoin(err, db.Close())
	}
	transportCtx, transportCancel := context.WithCancel(context.Background())
	s := &service{id: epaxos.ReplicaID(*idFlag), node: node, ready: db, db: db, peers: peers, client: newPeerClient(peerDeadline, clientTLSConfig), sendq: make(chan *outboundFrame, 1024), transportCtx: transportCtx, transportCancel: transportCancel, retryq: make(outboundRetryHeap, 0, 1024), transportWake: make(chan struct{}, 1), nextSeq: 1, transportDrops: make(map[transportLink]struct{}), requestDeadline: requestDeadline, maxClientBodyBytes: maxClientBody, maxPeerBodyBytes: maxPeerBody, maxAdminBodyBytes: maxAdminBody, maxScanLimit: maxScanLimit, legacyProcessAtDomain: legacyProcessAtDomainMode}
	s.startTransportWorkers(transportWorkerCount)
	app := &application{
		service: s,
		servers: []*http.Server{
			newHTTPServer(*clientListen, s.clientMux(), requestDeadline, serverTLSConfig),
			newHTTPServer(*peerListen, s.peerMux(), peerDeadline, serverTLSConfig),
			newHTTPServer(*adminListen, s.adminMux(), requestDeadline, serverTLSConfig),
		},
		storage:         db,
		listen:          listen,
		shutdownTimeout: gracefulShutdownTimeout,
	}
	log.Printf("kvnode %d starting client=%s peer=%s admin=%s legacy-process-at-domain=%s (trusted operator migration assertion)", s.id, *clientListen, *peerListen, *adminListen, legacyProcessAtDomainMode)
	return app.run(ctx)
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
func parseLegacyProcessAtDomain(raw string) (*epaxos.TimingDomain, string, error) {
	var domain epaxos.TimingDomain
	switch raw {
	case "":
		return nil, "unset", nil
	case "untimed":
		domain = epaxos.TimingDomainUntimed
	case "logical":
		domain = epaxos.TimingDomainLogical
	case "toq":
		domain = epaxos.TimingDomainTOQ
	default:
		return nil, "", fmt.Errorf("legacy process-at domain must be untimed, logical, or toq")
	}
	return &domain, raw, nil
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
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig
	}
	return &http.Client{Timeout: deadline, Transport: transport}
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

func serveHTTPServers(ctx context.Context, shutdownTimeout time.Duration, servers ...*http.Server) error {
	return serveHTTPServersWithListener(ctx, shutdownTimeout, net.Listen, servers...)
}

func serveHTTPServersWithListener(ctx context.Context, shutdownTimeout time.Duration, listen listenerFactory, servers ...*http.Server) error {
	if len(servers) == 0 {
		return fmt.Errorf("at least one HTTP server is required")
	}
	if listen == nil {
		return fmt.Errorf("listener factory is required")
	}
	listeners := make([]net.Listener, len(servers))
	for i, srv := range servers {
		ln, err := listen("tcp", srv.Addr)
		if err != nil {
			startErr := fmt.Errorf("listen %q: %w", srv.Addr, err)
			for j := 0; j < i; j++ {
				startErr = errorsJoin(startErr, listeners[j].Close())
			}
			return startErr
		}
		listeners[i] = ln
	}
	type serveResult struct {
		index int
		err   error
	}
	errc := make(chan serveResult, len(servers))
	for i, srv := range servers {
		go func(index int, srv *http.Server, ln net.Listener) {
			var err error
			if srv.TLSConfig != nil {
				err = srv.ServeTLS(ln, "", "")
			} else {
				err = srv.Serve(ln)
			}
			errc <- serveResult{index: index, err: err}
		}(i, srv, listeners[i])
	}

	remaining := len(servers)
	var serveErr error
	select {
	case <-ctx.Done():
	case result := <-errc:
		remaining--
		if result.err == nil {
			serveErr = fmt.Errorf("HTTP server %q stopped unexpectedly", servers[result.index].Addr)
		} else {
			serveErr = fmt.Errorf("HTTP server %q stopped unexpectedly: %w", servers[result.index].Addr, result.err)
		}
	}
	shutdownErr := shutdownHTTPServers(shutdownTimeout, servers)
	for ; remaining > 0; remaining-- {
		result := <-errc
		if result.err != nil && !errors.Is(result.err, http.ErrServerClosed) {
			serveErr = errorsJoin(serveErr, fmt.Errorf("serve HTTP server %q: %w", servers[result.index].Addr, result.err))
		}
	}
	return errorsJoin(serveErr, shutdownErr)
}

func shutdownHTTPServers(timeout time.Duration, servers []*http.Server) error {
	if timeout <= 0 {
		var closeErr error
		for _, srv := range servers {
			closeErr = errorsJoin(closeErr, srv.Close())
		}
		return errorsJoin(fmt.Errorf("shutdown timeout must be positive"), closeErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	type shutdownResult struct {
		addr string
		err  error
	}
	errc := make(chan shutdownResult, len(servers))
	for _, srv := range servers {
		go func(srv *http.Server) {
			errc <- shutdownResult{addr: srv.Addr, err: srv.Shutdown(ctx)}
		}(srv)
	}
	var shutdownErr error
	for range servers {
		result := <-errc
		if result.err != nil {
			shutdownErr = errorsJoin(shutdownErr, fmt.Errorf("shutdown HTTP server %q: %w", result.addr, result.err))
		}
	}
	if shutdownErr == nil {
		return nil
	}
	for _, srv := range servers {
		if err := srv.Close(); err != nil {
			shutdownErr = errorsJoin(shutdownErr, fmt.Errorf("close HTTP server %q: %w", srv.Addr, err))
		}
	}
	return shutdownErr
}

func (a *application) run(ctx context.Context) error {
	serveErr := serveHTTPServersWithListener(ctx, a.shutdownTimeout, a.listen, a.servers...)
	return errorsJoin(serveErr, a.close())
}

func (a *application) close() error {
	a.closeOnce.Do(func() {
		a.service.closeTransport()
		a.closeErr = a.storage.Close()
	})
	return a.closeErr
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
func (s *service) getOutboundFrame() *outboundFrame {
	if pooled := s.framePool.Get(); pooled != nil {
		return pooled.(*outboundFrame)
	}
	return new(outboundFrame)
}

func (s *service) releaseOutboundFrame(frame *outboundFrame) {
	if frame == nil {
		return
	}
	frame.body = resetTransportBytes(frame.body)
	frame.to = 0
	frame.endpoint = ""
	frame.retryAttempt = 0
	frame.retryAt = 0
	frame.retryOrder = 0
	frame.admitted = false
	s.framePool.Put(frame)
}

func (s *service) getPeerBodyBuffer() *pooledTransportBuffer {
	if pooled := s.peerBodyPool.Get(); pooled != nil {
		return pooled.(*pooledTransportBuffer)
	}
	return new(pooledTransportBuffer)
}

func (s *service) releasePeerBodyBuffer(buffer *pooledTransportBuffer) {
	if buffer == nil {
		return
	}
	buffer.bytes = resetTransportBytes(buffer.bytes)
	s.peerBodyPool.Put(buffer)
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
	s.mu.Unlock()
	s.transportMu.RLock()
	queueDepth := len(s.sendq)
	retryDepth := len(s.retryq)
	outstanding := s.outstandingFrames
	s.transportMu.RUnlock()

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
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_retry_queue_depth gauge\nkvnode_outbound_retry_queue_depth %d\n", retryDepth)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_frames_outstanding gauge\nkvnode_outbound_frames_outstanding %d\n", outstanding)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_frames_attempted_total counter\nkvnode_outbound_frames_attempted_total %d\n", s.outboundMetrics.attempted.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_frames_admitted_total counter\nkvnode_outbound_frames_admitted_total %d\n", s.outboundMetrics.admitted.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_frames_retried_total counter\nkvnode_outbound_frames_retried_total %d\n", s.outboundMetrics.retried.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_frames_backpressured_total counter\nkvnode_outbound_frames_backpressured_total %d\n", s.outboundMetrics.backpressured.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_frames_terminal_failed_total counter\nkvnode_outbound_frames_terminal_failed_total %d\n", s.outboundMetrics.terminalFailed.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_retry_tick_overflow_total counter\nkvnode_outbound_retry_tick_overflow_total %d\n", s.outboundMetrics.retryOverflow.Load())
	legacyMode := s.legacyProcessAtDomain
	if legacyMode == "" {
		legacyMode = "unset"
	}
	for _, mode := range []string{"unset", "untimed", "logical", "toq"} {
		selected := 0
		if legacyMode == mode {
			selected = 1
		}
		_, _ = fmt.Fprintf(w, "# TYPE kvnode_legacy_process_at_domain_%s gauge\nkvnode_legacy_process_at_domain_%s %d\n", mode, mode, selected)
	}
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
	bodyBuffer := s.getPeerBodyBuffer()
	body, err := readLimitedBodyInto(r.Body, s.peerBodyLimit(), bodyBuffer.bytes)
	bodyBuffer.bytes = body
	if err != nil {
		s.releasePeerBodyBuffer(bodyBuffer)
		writeBodyReadError(w, err)
		return
	}
	defer s.releasePeerBodyBuffer(bodyBuffer)

	scratch := epaxos.GetDecodeScratch()
	defer epaxos.PutDecodeScratch(scratch)
	var msg epaxos.Message
	if err := epaxos.DecodeMessageWithScratch(body, &msg, scratch); err != nil {
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
	drainErr := s.drainLocked()
	s.mu.Unlock()
	if drainErr != nil {
		http.Error(w, errorsJoin(err, drainErr).Error(), http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
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
	drainErr := s.drainLocked()
	done := err == nil && drainErr == nil && s.executedLocked(ref)
	s.mu.Unlock()
	if err != nil || drainErr != nil {
		return errorsJoin(err, drainErr)
	}
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
		if tickErr := s.tickLocked(); tickErr != nil {
			s.mu.Unlock()
			return tickErr
		}
		drainErr := s.drainLocked()
		done := s.executedLocked(ref)
		s.mu.Unlock()
		if drainErr != nil {
			return drainErr
		}
		if done {
			return nil
		}
	}
	return fmt.Errorf("proposal outcome unknown after logical tick deadline")
}

func (s *service) executedLocked(ref epaxos.InstanceRef) bool {
	for _, got := range s.node.Status().Executed {
		if got == ref {
			return true
		}
	}
	return false
}

func (s *service) prepareOutboundFrames(messages []epaxos.Message) ([]*outboundFrame, error) {
	if len(messages) == 0 {
		return nil, nil
	}
	s.outboundMetrics.attempted.Add(uint64(len(messages)))
	frames := make([]*outboundFrame, 0, len(messages))
	releasePrepared := func() {
		for _, frame := range frames {
			s.releaseOutboundFrame(frame)
		}
	}
	limit := s.peerBodyLimit()
	for index, message := range messages {
		size, err := encodedMessageSizeWithin(message, limit)
		if err != nil {
			releasePrepared()
			s.outboundMetrics.backpressured.Add(uint64(len(messages)))
			return nil, &transportAdmissionError{
				Cause:     fmt.Errorf("encode outbound message %d: %w", index, err),
				Attempted: len(messages),
			}
		}
		frame := s.getOutboundFrame()
		if int64(cap(frame.body)) > limit {
			clear(frame.body[:cap(frame.body)])
			frame.body = nil
		}
		if cap(frame.body) < size {
			frame.body = make([]byte, 0, size)
		} else {
			frame.body = frame.body[:0]
		}
		frame.body, err = epaxos.EncodeMessage(frame.body, message)
		actualSize := len(frame.body)
		if err != nil || actualSize != size {
			s.releaseOutboundFrame(frame)
			releasePrepared()
			s.outboundMetrics.backpressured.Add(uint64(len(messages)))
			if err == nil {
				err = fmt.Errorf("canonical size changed from %d to %d", size, actualSize)
			}
			return nil, &transportAdmissionError{
				Cause:     fmt.Errorf("encode outbound message %d: %w", index, err),
				Attempted: len(messages),
			}
		}
		frame.to = message.To
		frame.endpoint = s.peers[message.To]
		frames = append(frames, frame)
	}
	return frames, nil
}

func (s *service) admitOutboundFrames(frames []*outboundFrame) error {
	if len(frames) == 0 {
		return nil
	}
	releasePrepared := func() {
		for _, frame := range frames {
			s.releaseOutboundFrame(frame)
		}
	}
	s.transportMu.Lock()
	if s.transportClosed {
		s.transportMu.Unlock()
		releasePrepared()
		s.outboundMetrics.backpressured.Add(uint64(len(frames)))
		return &transportAdmissionError{Cause: errTransportClosed, Attempted: len(frames)}
	}
	available := cap(s.sendq) - s.outstandingFrames
	if len(frames) > available {
		s.transportMu.Unlock()
		releasePrepared()
		s.outboundMetrics.backpressured.Add(uint64(len(frames)))
		return &transportAdmissionError{
			Cause:     errSendQueueBackpressure,
			Attempted: len(frames),
			Available: available,
		}
	}
	// drainLocked serializes the only new-frame producer under service.mu.
	// outstandingFrames counts queued, in-flight, and retry-heap owners, so this
	// preflight reserves the entire batch before any frame becomes visible.
	for _, frame := range frames {
		frame.admitted = true
		s.outstandingFrames++
		s.sendq <- frame
	}
	s.transportMu.Unlock()
	s.outboundMetrics.admitted.Add(uint64(len(frames)))
	return nil
}

func (s *service) drainLocked() error {
	for round := 0; round < 1000; round++ {
		if !s.node.HasReady() {
			return nil
		}
		ready := s.node.Ready()
		if err := s.ready.ApplyReady(ready); err != nil {
			return err
		}
		frames, err := s.prepareOutboundFrames(ready.Messages)
		if err != nil {
			return err
		}
		if err := s.admitOutboundFrames(frames); err != nil {
			return err
		}
		if err := s.node.Advance(ready); err != nil {
			return err
		}
	}
	return fmt.Errorf("ready processing did not quiesce")
}

func (s *service) postFrame(frame *outboundFrame) postOutcome {
	if frame.to == s.id || frame.endpoint == "" || s.transportDropped(s.id, frame.to) || s.client == nil {
		return postTerminalFailure
	}
	ctx := s.transportCtx
	if ctx == nil {
		ctx = context.Background()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, frame.endpoint+"/epaxos/message", bytes.NewReader(frame.body))
	if err != nil {
		return postTerminalFailure
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err := s.client.Do(request)
	if err != nil {
		return postRetry
	}
	if response.Body != nil {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
	switch {
	case response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices:
		return postSucceeded
	case response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError:
		return postRetry
	default:
		return postTerminalFailure
	}
}

func retryDelayTicks(attempt uint32) uint64 {
	if attempt >= 6 {
		return maxRetryDelayTicks
	}
	delay := initialRetryDelayTicks << attempt
	if delay > maxRetryDelayTicks {
		return maxRetryDelayTicks
	}
	return delay
}

func (s *service) signalTransportSchedulerLocked() {
	if s.transportWake == nil {
		return
	}
	select {
	case s.transportWake <- struct{}{}:
	default:
	}
}

func (s *service) advanceTransportTick() error {
	s.transportMu.Lock()
	if s.transportTick == ^uint64(0) {
		s.transportMu.Unlock()
		s.outboundMetrics.retryOverflow.Add(1)
		return errTransportTickOverflow
	}
	s.transportTick++
	s.signalTransportSchedulerLocked()
	s.transportMu.Unlock()
	return nil
}

func (s *service) tickLocked() error {
	if err := s.node.Tick(); err != nil {
		return err
	}
	return s.advanceTransportTick()
}

func (s *service) finishOutboundFrame(frame *outboundFrame, terminal bool) {
	s.transportMu.Lock()
	admitted := frame.admitted
	frame.admitted = false
	s.transportMu.Unlock()
	if terminal {
		s.outboundMetrics.terminalFailed.Add(1)
	}
	s.releaseOutboundFrame(frame)
	if admitted {
		s.transportMu.Lock()
		if s.outstandingFrames > 0 {
			s.outstandingFrames--
		}
		s.transportMu.Unlock()
	}
}

func (s *service) scheduleRetry(frame *outboundFrame) {
	terminal := false
	overflow := false
	s.transportMu.Lock()
	delay := retryDelayTicks(frame.retryAttempt)
	if s.transportClosed || s.transportTick > ^uint64(0)-delay || len(s.retryq) >= cap(s.sendq) {
		terminal = true
		overflow = s.transportTick > ^uint64(0)-delay
	} else {
		frame.retryAt = s.transportTick + delay
		frame.retryOrder = s.retryOrder
		s.retryOrder++
		if frame.retryAttempt < ^uint32(0) {
			frame.retryAttempt++
		}
		heap.Push(&s.retryq, frame)
	}
	s.transportMu.Unlock()
	if overflow {
		s.outboundMetrics.retryOverflow.Add(1)
	}
	if terminal {
		s.finishOutboundFrame(frame, true)
		return
	}
	s.outboundMetrics.retried.Add(1)
}

func (s *service) releaseDueRetries() bool {
	var terminal []*outboundFrame
	s.transportMu.Lock()
	if s.transportClosed {
		s.transportMu.Unlock()
		return false
	}
	now := s.transportTick
	for len(s.retryq) != 0 && s.retryq[0].retryAt <= now {
		frame := heap.Pop(&s.retryq).(*outboundFrame)
		select {
		case s.sendq <- frame:
		default:
			if frame.admitted {
				if s.outstandingFrames > 0 {
					s.outstandingFrames--
				}
				frame.admitted = false
			}
			terminal = append(terminal, frame)
		}
	}
	s.schedulerTick.Store(now)
	s.transportMu.Unlock()
	for _, frame := range terminal {
		s.outboundMetrics.backpressured.Add(1)
		s.outboundMetrics.terminalFailed.Add(1)
		s.releaseOutboundFrame(frame)
	}
	return true
}

func (s *service) transportRetryScheduler() {
	for range s.transportWake {
		if !s.releaseDueRetries() {
			return
		}
	}
}

func (s *service) startTransportWorkers(count int) {
	s.transportMu.Lock()
	if s.transportWake == nil {
		s.transportWake = make(chan struct{}, 1)
	}
	if s.retryq == nil {
		s.retryq = make(outboundRetryHeap, 0, cap(s.sendq))
	}
	s.transportMu.Unlock()
	s.transportSchedulerOnce.Do(func() {
		s.transportWG.Add(1)
		go func() {
			defer s.transportWG.Done()
			s.transportRetryScheduler()
		}()
	})
	s.transportWG.Add(count)
	for range count {
		go func() {
			defer s.transportWG.Done()
			s.transportWorker()
		}()
	}
}

func (s *service) closeTransport() {
	s.transportCloseOnce.Do(func() {
		s.transportMu.Lock()
		s.transportClosed = true
		retrying := s.retryq
		s.retryq = nil
		for _, frame := range retrying {
			if frame.admitted {
				if s.outstandingFrames > 0 {
					s.outstandingFrames--
				}
				frame.admitted = false
			}
		}
		if s.sendq != nil {
			close(s.sendq)
		}
		s.signalTransportSchedulerLocked()
		s.transportMu.Unlock()
		for _, frame := range retrying {
			s.outboundMetrics.terminalFailed.Add(1)
			s.releaseOutboundFrame(frame)
		}
		if s.client != nil {
			s.client.CloseIdleConnections()
		}
		if s.transportCancel != nil {
			s.transportCancel()
		}
		s.transportWG.Wait()
		if s.sendq != nil {
			for frame := range s.sendq {
				s.finishOutboundFrame(frame, true)
			}
		}
	})
}

func (s *service) transportWorker() {
	for frame := range s.sendq {
		s.deliverFrame(frame)
	}
}

func (s *service) deliverFrame(frame *outboundFrame) {
	if s.transportCtx != nil {
		select {
		case <-s.transportCtx.Done():
			s.finishOutboundFrame(frame, true)
			return
		default:
		}
	}
	switch s.postFrame(frame) {
	case postSucceeded:
		s.finishOutboundFrame(frame, false)
	case postTerminalFailure:
		s.finishOutboundFrame(frame, true)
	case postRetry:
		if s.transportCtx != nil {
			select {
			case <-s.transportCtx.Done():
				s.finishOutboundFrame(frame, true)
				return
			default:
			}
		}
		s.scheduleRetry(frame)
	}
}

func errorsJoin(a, b error) error {
	return errors.Join(a, b)
}
