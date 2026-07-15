//go:build kvnode

// Command kvnode runs the Pebble-backed EPaxos key-value example.
package main

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
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

	"github.com/cockroachdb/pebble"
	"gosuda.org/moreconsensus/epaxos"
	"gosuda.org/moreconsensus/examples/kv"
)

type readyApplier interface {
	ApplyReady(epaxos.Ready) error
	LoadInstance(epaxos.InstanceRef) (epaxos.InstanceRecord, bool, error)
}

type logicalTickSource interface {
	C() <-chan struct{}
	Stop()
}

type tickerTickSource struct {
	pulses chan struct{}
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
}

func newTickerTickSource(period time.Duration) *tickerTickSource {
	source := &tickerTickSource{
		pulses: make(chan struct{}, 1),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go func() {
		defer close(source.done)
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				select {
				case source.pulses <- struct{}{}:
				default:
				}
			case <-source.stop:
				return
			}
		}
	}()
	return source
}

func (s *tickerTickSource) C() <-chan struct{} {
	return s.pulses
}

func (s *tickerTickSource) Stop() {
	s.once.Do(func() {
		close(s.stop)
		<-s.done
	})
}

type proposalWaiter struct {
	result chan error
}

type serviceConfig struct {
	protocolTick            time.Duration
	queueCapacity           int
	retryCapacity           int
	transportWorkers        int
	shutdownTimeout         time.Duration
	peerMaxIdleConnsPerHost int
	peerMaxConnsPerHost     int
}
type outboundFrame struct {
	to           epaxos.ReplicaID
	endpoint     string
	body         []byte
	retryAt      uint64
	retryOrder   uint64
	retryStarted uint64
	retryAttempt uint32
	admitted     bool
	inRetry      bool
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
	attempted      atomic.Uint64
	admitted       atomic.Uint64
	retried        atomic.Uint64
	backpressured  atomic.Uint64
	terminalFailed atomic.Uint64
	retryOverflow  atomic.Uint64
}

var requestDurationBuckets = [...]time.Duration{
	time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2500 * time.Millisecond,
	5 * time.Second,
}

type requestDurationHistogram struct {
	buckets [len(requestDurationBuckets) + 1]atomic.Uint64
	count   atomic.Uint64
	sumNS   atomic.Uint64
}

func (h *requestDurationHistogram) observe(elapsed time.Duration) {
	index := len(requestDurationBuckets)
	for candidate, upper := range requestDurationBuckets {
		if elapsed <= upper {
			index = candidate
			break
		}
	}
	h.buckets[index].Add(1)
	h.count.Add(1)
	h.sumNS.Add(uint64(elapsed))
}

type serviceMetrics struct {
	framePoolHits          atomic.Uint64
	framePoolMisses        atomic.Uint64
	peerBodyPoolHits       atomic.Uint64
	peerBodyPoolMisses     atomic.Uint64
	outboundEncodedBytes   atomic.Uint64
	inboundDecodedBytes    atomic.Uint64
	readyApplyFailures     atomic.Uint64
	readyAdmissionFailures atomic.Uint64
	protocolTerminalErrors atomic.Uint64
	clientDuration         requestDurationHistogram
	peerDuration           requestDurationHistogram
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
	errTransportTickOverflow = errors.New("outbound transport logical tick overflow")
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
		len(message.Command.Footprint.Points) > maxWireMessageCollectionEntries ||
		len(message.Command.Footprint.Spans) > maxWireMessageCollectionEntries {
		return 0, epaxos.ErrInvalidMessage
	}
	for _, evidence := range message.AcceptEvidence {
		if len(evidence.Deps) > maxWireMessageCollectionEntries {
			return 0, epaxos.ErrInvalidMessage
		}
	}

	size := int64(4 + 1 + 1 + 1 + 1 + 32) // Magic, TOQ/All/Reject/FastPath flags, and checksum.
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
		message.FromIncarnation, message.ToIncarnation,
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
		uint64(message.Kind),
		uint64(message.ConfChange.Type),
		uint64(message.ConfChange.Replica),
		uint64(len(message.ProtocolControl)),
	) || !add(int64(len(message.ProtocolControl))) ||
		!addUvarints(message.Command.ID.Client, message.Command.ID.Sequence, uint64(len(message.Command.Payload))) ||
		!add(int64(len(message.Command.Payload))) ||
		!add(uvarintSize(uint64(len(message.Command.Footprint.Points)))) {
		return 0, errOutboundFrameTooLarge
	}
	for _, point := range message.Command.Footprint.Points {
		if !add(uvarintSize(uint64(len(point)))) || !add(int64(len(point))) {
			return 0, errOutboundFrameTooLarge
		}
	}
	if !add(uvarintSize(uint64(len(message.Command.Footprint.Spans)))) {
		return 0, errOutboundFrameTooLarge
	}
	for _, span := range message.Command.Footprint.Spans {
		if !add(uvarintSize(uint64(len(span.Start)))) || !add(int64(len(span.Start))) ||
			!add(uvarintSize(uint64(len(span.End)))) || !add(int64(len(span.End))) {
			return 0, errOutboundFrameTooLarge
		}
	}
	if !add(uvarintSize(uint64(len(message.Command.CycleKey)))) || !add(int64(len(message.Command.CycleKey))) {
		return 0, errOutboundFrameTooLarge
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

type retentionLimits struct {
	maxResidentInstances uint64
	maxDurableRecords    uint64
	maxDataBytes         uint64
}

type service struct {
	mu                     sync.Mutex
	faultMu                sync.RWMutex
	transportMu            sync.RWMutex
	transportWG            sync.WaitGroup
	transportCloseOnce     sync.Once
	transportSchedulerOnce sync.Once
	id                     epaxos.ReplicaID
	node                   *epaxos.RawNode
	ready                  readyApplier
	db                     *kv.DB
	peers                  map[epaxos.ReplicaID]string
	client                 *http.Client
	peerClients            map[epaxos.ReplicaID]*http.Client
	production             bool
	enableFaultInjection   bool
	sendq                  chan *outboundFrame
	transportCtx           context.Context
	transportCancel        context.CancelFunc
	framePool              sync.Pool
	peerBodyPool           sync.Pool
	outboundMetrics        outboundFrameMetrics
	metrics                serviceMetrics
	retryq                 outboundRetryHeap
	transportWake          chan struct{}
	transportSpace         chan struct{}
	transportTick          uint64
	retryOrder             uint64
	queueOwned             int
	retryOwned             int
	outstandingFrames      int
	retryCapacity          int
	schedulerTick          atomic.Uint64
	nextSeq                uint64
	transportDrops         map[transportLink]struct{}
	storageFailed          bool
	transportClosed        bool
	requestDeadline        time.Duration
	maxClientBodyBytes     int64
	maxPeerBodyBytes       int64
	maxAdminBodyBytes      int64
	maxScanLimit           int
	legacyProcessAtDomain  string
	waiters                map[epaxos.InstanceRef]*proposalWaiter
	terminalErr            error
	terminalReason         string
	shuttingDown           bool
	admissionBlocked       bool
	blockedPeerIngress     map[epaxos.ReplicaID][][sha256.Size]byte
	readyApplied           bool
	readyCheckpointApplied bool
	readyLoadsApplied      int
	frozenFrames           []*outboundFrame
	singlePreparedFrame    [1]*outboundFrame
	reusableReady          epaxos.Ready
	retention              retentionLimits
	retentionLevel         int
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
	tickSource      logicalTickSource
	protocolCtx     context.Context
	protocolCancel  context.CancelFunc
	protocolWG      sync.WaitGroup
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
	defaultProtocolTick      = 100 * time.Millisecond
	defaultQueueCapacity     = 1024
	defaultRetryCapacity     = 1024
	defaultTransportWorkers  = 8
	defaultShutdownTimeout   = 15 * time.Second
)

var (
	errRequestBodyTooLarge = errors.New("request body too large")
	errServiceShuttingDown = errors.New("kvnode service shutting down")
	errRetentionLimit      = errors.New("configured retention limit reached")
	errRetentionPressure   = errors.New("configured retention pressure threshold reached")
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
	protocolTickMS := flags.Int("protocol-tick-ms", 100, "protocol logical tick cadence in milliseconds")
	queueCapacityFlag := flags.Int("transport-queue-capacity", defaultQueueCapacity, "maximum queued and in-flight transport frames")
	retryCapacityFlag := flags.Int("transport-retry-capacity", defaultRetryCapacity, "maximum transport retry frames")
	transportWorkersFlag := flags.Int("transport-workers", defaultTransportWorkers, "outbound transport worker count")
	shutdownTimeoutMS := flags.Int("shutdown-timeout-ms", 15000, "graceful shutdown timeout in milliseconds")
	peerMaxIdleFlag := flags.Int("peer-max-idle-conns-per-host", 8, "maximum idle peer connections per host")
	peerMaxConnsFlag := flags.Int("peer-max-conns-per-host", 16, "maximum peer connections per host")
	peerTLSCert := flags.String("peer-tls-cert", "", "peer-plane TLS certificate chain PEM")
	peerTLSKey := flags.String("peer-tls-key", "", "peer-plane TLS private key PEM")
	peerTLSCA := flags.String("peer-tls-ca", "", "peer-plane trust bundle PEM")
	clientTLSCert := flags.String("client-tls-cert", "", "client-plane TLS certificate chain PEM")
	clientTLSKey := flags.String("client-tls-key", "", "client-plane TLS private key PEM")
	clientClientCA := flags.String("client-client-ca", "", "client-plane client trust bundle PEM")
	adminTLSCert := flags.String("admin-tls-cert", "", "admin-plane TLS certificate chain PEM")
	adminTLSKey := flags.String("admin-tls-key", "", "admin-plane TLS private key PEM")
	adminClientCA := flags.String("admin-client-ca", "", "admin-plane client trust bundle PEM")
	productionFlag := flags.Bool("production", false, "enable fail-closed production policy")
	enableFaultInjectionFlag := flags.Bool("enable-fault-injection", false, "register non-production fault injection routes")
	maxClientBodyBytes := flags.Int64("max-client-body-bytes", defaultMaxClientBodySize, "maximum bytes accepted for client write and transaction request bodies")
	maxPeerBodyBytes := flags.Int64("max-peer-body-bytes", defaultMaxPeerBodySize, "maximum bytes accepted for peer replication message bodies")
	maxAdminBodyBytes := flags.Int64("max-admin-body-bytes", defaultMaxAdminBodySize, "maximum bytes accepted for administrative request bodies")
	maxScanLimitFlag := flags.Int("max-scan-limit", defaultMaxScanLimit, "maximum rows returned by one scan request")
	legacyProcessAtDomainFlag := flags.String("legacy-process-at-domain", "", "trusted migration assertion for legacy ProcessAt records: untimed, logical, or toq")
	pebbleCacheBytes := flags.Int64("pebble-cache-bytes", 8388608, "Pebble block cache bytes")
	pebbleMemtableBytes := flags.Uint64("pebble-memtable-bytes", 4194304, "Pebble memtable bytes")
	pebbleMemtableStopWrites := flags.Int("pebble-memtable-stop-writes", 2, "Pebble queued memtable write-stop threshold")
	pebbleMaxOpenFiles := flags.Int("pebble-max-open-files", 1000, "Pebble maximum open files")
	pebbleMaxConcurrentCompactions := flags.Int("pebble-max-concurrent-compactions", 1, "Pebble maximum concurrent compactions")
	pebbleBytesPerSync := flags.Int("pebble-bytes-per-sync", 524288, "Pebble SST bytes per background sync")
	pebbleWALBytesPerSync := flags.Int("pebble-wal-bytes-per-sync", 0, "Pebble WAL bytes per background sync")
	retentionMaxResident := flags.Uint64("retention-max-resident-instances", 0, "maximum resident EPaxos instances")
	retentionMaxDurable := flags.Uint64("retention-max-durable-records", 0, "maximum durable EPaxos records")
	retentionMaxDataBytes := flags.Uint64("retention-max-data-bytes", 0, "maximum Pebble disk usage bytes")
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
	protocolTick, err := boundedDurationFromMillis("protocol tick", *protocolTickMS, 1, 60000)
	if err != nil {
		return err
	}
	shutdownTimeout, err := boundedDurationFromMillis("shutdown timeout", *shutdownTimeoutMS, 1, 300000)
	if err != nil {
		return err
	}
	queueCapacity, err := boundedInt("transport queue capacity", *queueCapacityFlag, 1, 1048576)
	if err != nil {
		return err
	}
	retryCapacity, err := boundedInt("transport retry capacity", *retryCapacityFlag, 1, 1048576)
	if err != nil {
		return err
	}
	transportWorkers, err := boundedInt("transport workers", *transportWorkersFlag, 1, 256)
	if err != nil {
		return err
	}
	peerMaxIdle, err := boundedInt("peer max idle connections per host", *peerMaxIdleFlag, 1, 4096)
	if err != nil {
		return err
	}
	peerMaxConns, err := boundedInt("peer max connections per host", *peerMaxConnsFlag, 1, 4096)
	if err != nil {
		return err
	}
	if peerMaxIdle > peerMaxConns {
		return fmt.Errorf("peer max idle connections per host must not exceed peer max connections per host")
	}
	if *pebbleCacheBytes <= 0 || *pebbleMemtableBytes == 0 || *pebbleMemtableStopWrites < 2 ||
		*pebbleMaxOpenFiles <= 0 || *pebbleMaxConcurrentCompactions <= 0 ||
		*pebbleBytesPerSync < 0 || *pebbleWALBytesPerSync < 0 {
		return fmt.Errorf("invalid Pebble cache, memtable, file, compaction, or sync settings")
	}
	peerServerTLS, peerClientTLS, err := parsePlaneTLSConfig("peer", *peerTLSCert, *peerTLSKey, *peerTLSCA)
	if err != nil {
		return err
	}
	clientServerTLS, _, err := parsePlaneTLSConfig("client", *clientTLSCert, *clientTLSKey, *clientClientCA)
	if err != nil {
		return err
	}
	adminServerTLS, _, err := parsePlaneTLSConfig("admin", *adminTLSCert, *adminTLSKey, *adminClientCA)
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
	if *productionFlag {
		if peerServerTLS == nil || clientServerTLS == nil || adminServerTLS == nil {
			return fmt.Errorf("production mode requires complete peer, client, and admin mTLS files")
		}
		if *enableFaultInjectionFlag {
			return fmt.Errorf("production mode forbids fault injection")
		}
		if maxPeerBody < 2<<20 {
			return fmt.Errorf("production max peer body bytes must be at least 2097152")
		}
		if *retentionMaxResident == 0 || *retentionMaxDurable == 0 || *retentionMaxDataBytes == 0 {
			return fmt.Errorf("production mode requires positive retention limits")
		}
		for id, endpoint := range peers {
			parsed, parseErr := url.Parse(endpoint)
			if parseErr != nil || parsed.Scheme != "https" {
				return fmt.Errorf("production peer %d must use HTTPS", id)
			}
		}
	}
	cache := pebble.NewCache(*pebbleCacheBytes)
	pebbleOptions := &pebble.Options{
		Cache:                       cache,
		MemTableSize:                *pebbleMemtableBytes,
		MemTableStopWritesThreshold: *pebbleMemtableStopWrites,
		MaxOpenFiles:                *pebbleMaxOpenFiles,
		MaxConcurrentCompactions:    func() int { return *pebbleMaxConcurrentCompactions },
		BytesPerSync:                *pebbleBytesPerSync,
		WALBytesPerSync:             *pebbleWALBytesPerSync,
	}
	db, err := kv.OpenWithOptions(*data, pebbleOptions)
	cache.Unref()
	if err != nil {
		return err
	}
	nextCommandSequence, err := db.NextCommandSequence(uint64(*idFlag))
	if err != nil {
		return errorsJoin(err, db.Close())
	}
	store := db.EPaxosStorage()
	// The application supplies deterministic logical pulses. Classic EPaxos
	// remains selected because the KV service has no certified synchronized
	// clock domain for TOQ.
	node, err := epaxos.NewRawNode(epaxos.Config{ID: epaxos.ReplicaID(*idFlag), Voters: voters, Storage: store, RetryTicks: 2, RecoveryTicks: 5, MaxReadyMessages: queueCapacity, LegacyProcessAtDomain: legacyProcessAtDomain})
	if err != nil {
		return errorsJoin(err, db.Close())
	}
	transportCtx, transportCancel := context.WithCancel(context.Background())
	config := serviceConfig{
		protocolTick:            protocolTick,
		queueCapacity:           queueCapacity,
		retryCapacity:           retryCapacity,
		transportWorkers:        transportWorkers,
		shutdownTimeout:         shutdownTimeout,
		peerMaxIdleConnsPerHost: peerMaxIdle,
		peerMaxConnsPerHost:     peerMaxConns,
	}
	peerClients, err := newPeerClients(peerDeadline, peerClientTLS, config, peers)
	if err != nil {
		return errorsJoin(err, db.Close())
	}
	s := &service{
		id:                    epaxos.ReplicaID(*idFlag),
		node:                  node,
		ready:                 db,
		db:                    db,
		peers:                 peers,
		peerClients:           peerClients,
		production:            *productionFlag,
		enableFaultInjection:  *enableFaultInjectionFlag,
		sendq:                 make(chan *outboundFrame, queueCapacity),
		transportCtx:          transportCtx,
		transportCancel:       transportCancel,
		retryq:                make(outboundRetryHeap, 0, retryCapacity),
		retryCapacity:         retryCapacity,
		transportWake:         make(chan struct{}, 1),
		transportSpace:        make(chan struct{}, 1),
		nextSeq:               nextCommandSequence,
		transportDrops:        make(map[transportLink]struct{}),
		waiters:               make(map[epaxos.InstanceRef]*proposalWaiter),
		requestDeadline:       requestDeadline,
		maxClientBodyBytes:    maxClientBody,
		maxPeerBodyBytes:      maxPeerBody,
		maxAdminBodyBytes:     maxAdminBody,
		maxScanLimit:          maxScanLimit,
		legacyProcessAtDomain: legacyProcessAtDomainMode,
		retention: retentionLimits{
			maxResidentInstances: *retentionMaxResident,
			maxDurableRecords:    *retentionMaxDurable,
			maxDataBytes:         *retentionMaxDataBytes,
		},
	}
	s.mu.Lock()
	s.evaluateRetentionLocked()
	startupErr := s.terminalErr
	s.mu.Unlock()
	if startupErr != nil {
		return errorsJoin(startupErr, db.Close())
	}
	s.startTransportWorkers(transportWorkers)
	app := &application{
		service: s,
		servers: []*http.Server{
			newHTTPServer(*clientListen, s.clientMux(), requestDeadline, clientServerTLS),
			newHTTPServer(*peerListen, s.peerMux(), peerDeadline, peerServerTLS),
			newHTTPServer(*adminListen, s.adminMux(), requestDeadline, adminServerTLS),
		},
		storage:         db,
		listen:          listen,
		shutdownTimeout: shutdownTimeout,
		tickSource:      newTickerTickSource(protocolTick),
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

func boundedDurationFromMillis(name string, ms, minimum, maximum int) (time.Duration, error) {
	if ms < minimum || ms > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d milliseconds", name, minimum, maximum)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func boundedInt(name string, value, minimum, maximum int) (int, error) {
	if value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d", name, minimum, maximum)
	}
	return value, nil
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

func newPeerClient(deadline time.Duration, tlsConfig *tls.Config, configs ...serviceConfig) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig
	}
	if len(configs) != 0 {
		transport.MaxIdleConnsPerHost = configs[0].peerMaxIdleConnsPerHost
		transport.MaxConnsPerHost = configs[0].peerMaxConnsPerHost
	}
	return &http.Client{Timeout: deadline, Transport: transport}
}

func newPeerClients(deadline time.Duration, tlsConfig *tls.Config, config serviceConfig, peers map[epaxos.ReplicaID]string) (map[epaxos.ReplicaID]*http.Client, error) {
	clients := make(map[epaxos.ReplicaID]*http.Client, len(peers))
	for id := range peers {
		var identityConfig *tls.Config
		if tlsConfig != nil {
			identityConfig = tlsConfig.Clone()
			expectedID := id
			identityConfig.VerifyConnection = func(state tls.ConnectionState) error {
				if len(state.PeerCertificates) == 0 {
					return fmt.Errorf("peer %d presented no certificate", expectedID)
				}
				actualID, err := peerReplicaIDFromCertificate(state.PeerCertificates[0])
				if err != nil {
					return err
				}
				if actualID != expectedID {
					return fmt.Errorf("peer certificate replica %d does not match configured replica %d", actualID, expectedID)
				}
				return nil
			}
		}
		clients[id] = newPeerClient(deadline, identityConfig, config)
	}
	return clients, nil
}

func peerReplicaIDFromCertificate(certificate *x509.Certificate) (epaxos.ReplicaID, error) {
	if certificate == nil || len(certificate.URIs) != 1 {
		return 0, fmt.Errorf("peer certificate must contain exactly one URI SAN")
	}
	identity := certificate.URIs[0]
	const prefix = "/moreconsensus/replica/"
	if identity.Scheme != "spiffe" || identity.Host != "gosuda.org" || !strings.HasPrefix(identity.Path, prefix) || identity.RawQuery != "" || identity.Fragment != "" || identity.User != nil {
		return 0, fmt.Errorf("invalid peer URI SAN %q", identity.String())
	}
	idText := strings.TrimPrefix(identity.Path, prefix)
	if idText == "" || strings.Contains(idText, "/") {
		return 0, fmt.Errorf("invalid peer URI SAN %q", identity.String())
	}
	id, err := strconv.ParseUint(idText, 10, 64)
	if err != nil || id == 0 {
		return 0, fmt.Errorf("invalid peer URI SAN %q", identity.String())
	}
	return epaxos.ReplicaID(id), nil
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

func parsePlaneTLSConfig(plane, certFile, keyFile, caFile string) (*tls.Config, *tls.Config, error) {
	configured := 0
	for _, path := range []string{certFile, keyFile, caFile} {
		if path != "" {
			configured++
		}
	}
	if configured == 0 {
		return nil, nil, nil
	}
	if configured != 3 {
		return nil, nil, fmt.Errorf("%s TLS certificate, key, and CA must be set together", plane)
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load %s TLS identity: %w", plane, err)
	}
	roots, err := loadCertPool(caFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load %s TLS CA: %w", plane, err)
	}
	server := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		ClientCAs:    roots,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	client := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      roots,
	}
	return server, client, nil
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

func observeRequestDuration(histogram *requestDurationHistogram, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		handler(w, r)
		histogram.observe(time.Since(started))
	}
}

func (s *service) clientMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", observeRequestDuration(&s.metrics.clientDuration, s.handleKV))
	mux.HandleFunc("/txn", observeRequestDuration(&s.metrics.clientDuration, s.handleTxn))
	mux.HandleFunc("/scan", observeRequestDuration(&s.metrics.clientDuration, s.handleScan))
	return mux
}

func (s *service) peerMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/epaxos/message", observeRequestDuration(&s.metrics.peerDuration, s.handleMessage))
	mux.HandleFunc("/epaxos/snapshot/", observeRequestDuration(&s.metrics.peerDuration, s.handleSnapshotBundle))
	return mux
}

func (s *service) adminMux() *http.ServeMux {
	mux := http.NewServeMux()
	if s.enableFaultInjection && !s.production {
		mux.HandleFunc("/faults/transport", s.handleTransportFault)
		mux.HandleFunc("/faults/storage", s.handleStorageFault)
	}
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
	a.protocolCtx, a.protocolCancel = context.WithCancel(context.Background())
	a.protocolWG.Add(1)
	go func() {
		defer a.protocolWG.Done()
		a.service.protocolLoop(a.protocolCtx, a.tickSource)
	}()
	serveErr := serveHTTPServersWithListener(ctx, a.shutdownTimeout, a.listen, a.servers...)
	return errorsJoin(serveErr, a.close())
}

func (a *application) close() error {
	a.closeOnce.Do(func() {
		a.service.mu.Lock()
		a.service.shuttingDown = true
		a.service.completeAllWaitersLocked(errServiceShuttingDown)
		a.service.mu.Unlock()
		if a.tickSource != nil {
			a.tickSource.Stop()
		}
		if a.protocolCancel != nil {
			a.protocolCancel()
		}
		a.protocolWG.Wait()
		a.service.mu.Lock()
		for _, frame := range a.service.frozenFrames {
			a.service.releaseOutboundFrame(frame)
		}
		a.service.clearSinglePreparedBorrow(a.service.frozenFrames)
		a.service.frozenFrames = nil
		a.service.reusableReady.Release()
		a.service.mu.Unlock()
		a.service.closeTransport()
		if a.storage != nil {
			a.closeErr = a.storage.Close()
		}
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

func (s *service) protocolLoop(ctx context.Context, source logicalTickSource) {
	if source == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-source.C():
			if !ok {
				return
			}
			s.mu.Lock()
			s.protocolPulseLocked()
			s.mu.Unlock()
		}
	}
}

func (s *service) protocolPulseLocked() {
	s.evaluateRetentionLocked()
	if s.shuttingDown || s.terminalErr != nil {
		return
	}
	if s.admissionBlocked {
		if err := s.drainLocked(); err != nil {
			s.setTerminalLocked(err)
			return
		}
		if s.admissionBlocked {
			if err := s.advanceTransportTick(); err != nil {
				s.setTerminalLocked(err)
			}
		}
		return
	}
	if err := s.node.Tick(); err != nil {
		s.setTerminalLocked(err)
		return
	}
	if err := s.advanceTransportTick(); err != nil {
		s.setTerminalLocked(err)
		return
	}
	if err := s.drainLocked(); err != nil {
		s.setTerminalLocked(err)
	}
}

func retentionUsageLevel(value, limit uint64) int {
	if limit == 0 {
		return 0
	}
	threshold := func(percent uint64) uint64 {
		return (limit/100)*percent + (limit%100*percent+99)/100
	}
	switch {
	case value >= limit:
		return 3
	case value >= threshold(90):
		return 2
	case value >= threshold(80):
		return 1
	default:
		return 0
	}
}

func (s *service) evaluateRetentionLocked() {
	if s.node == nil || s.terminalErr != nil {
		return
	}
	runtimeStats := s.node.RuntimeStats()
	level := retentionUsageLevel(uint64(runtimeStats.ResidentInstances), s.retention.maxResidentInstances)
	if s.db != nil {
		storageStats := s.db.StorageStats()
		if candidate := retentionUsageLevel(storageStats.DurableInstanceRecords, s.retention.maxDurableRecords); candidate > level {
			level = candidate
		}
		if candidate := retentionUsageLevel(storageStats.DiskUsageBytes, s.retention.maxDataBytes); candidate > level {
			level = candidate
		}
	}
	s.retentionLevel = level
	if level == 3 {
		s.terminalReason = "retention-limit"
		s.setTerminalLocked(errRetentionLimit)
	}
}

func (s *service) setTerminalLocked(err error) {
	if err == nil || s.terminalErr != nil {
		return
	}
	if s.terminalReason == "" {
		s.terminalReason = "protocol-failed"
	}
	s.metrics.protocolTerminalErrors.Add(1)
	s.terminalErr = err
	s.completeAllWaitersLocked(err)
}

func (s *service) completeAllWaitersLocked(err error) {
	for ref, waiter := range s.waiters {
		waiter.result <- err
		delete(s.waiters, ref)
	}
}

func (s *service) completeExecutedWaitersLocked() {
	for ref, waiter := range s.waiters {
		if !s.node.IsExecuted(ref) {
			continue
		}
		waiter.result <- nil
		delete(s.waiters, ref)
	}
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
		s.metrics.framePoolHits.Add(1)
		return pooled.(*outboundFrame)
	}
	s.metrics.framePoolMisses.Add(1)
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
	frame.retryStarted = 0
	frame.admitted = false
	frame.inRetry = false
	s.framePool.Put(frame)
}

func (s *service) getPeerBodyBuffer() *pooledTransportBuffer {
	if pooled := s.peerBodyPool.Get(); pooled != nil {
		s.metrics.peerBodyPoolHits.Add(1)
		return pooled.(*pooledTransportBuffer)
	}
	s.metrics.peerBodyPoolMisses.Add(1)
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
		http.Error(w, "storage-failed", http.StatusServiceUnavailable)
		return
	}
	s.mu.Lock()
	s.evaluateRetentionLocked()
	terminalReason := s.terminalReason
	admissionBlocked := s.admissionBlocked
	shuttingDown := s.shuttingDown
	retentionLevel := s.retentionLevel
	s.mu.Unlock()
	if terminalReason != "" {
		http.Error(w, terminalReason, http.StatusServiceUnavailable)
		return
	}
	if shuttingDown {
		http.Error(w, "protocol-failed", http.StatusServiceUnavailable)
		return
	}
	if retentionLevel >= 2 {
		http.Error(w, "retention-pressure", http.StatusServiceUnavailable)
		return
	}
	s.transportMu.RLock()
	closed := s.transportClosed
	owned := s.queueOwned + s.retryOwned
	capacity := cap(s.sendq) + s.retryCapacity
	s.transportMu.RUnlock()
	if closed {
		http.Error(w, "transport-closed", http.StatusServiceUnavailable)
		return
	}
	saturated := admissionBlocked
	if capacity > 0 && owned*10 >= capacity*9 {
		saturated = true
	}
	if saturated {
		http.Error(w, "transport-saturated", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ready"))
}

func (s *service) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	runtimeStats := s.node.RuntimeStats()
	admissionBlocked := s.admissionBlocked
	retentionLevel := s.retentionLevel
	storageStats := kv.StorageStats{}
	if s.db != nil {
		storageStats = s.db.StorageStats()
	}
	s.mu.Unlock()
	s.transportMu.RLock()
	queueDepth := len(s.sendq)
	queueOwned := s.queueOwned
	retryOwned := s.retryOwned
	outstanding := s.outstandingFrames
	transportTick := s.transportTick
	oldestRetryAge := uint64(0)
	for _, frame := range s.retryq {
		if age := transportTick - frame.retryStarted; age > oldestRetryAge {
			oldestRetryAge = age
		}
	}
	s.transportMu.RUnlock()

	storageFaultActive := 0
	if s.storageFaultActive() {
		storageFaultActive = 1
	}
	blocked := 0
	if admissionBlocked {
		blocked = 1
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_storage_fault_active gauge\nkvnode_storage_fault_active %d\n", storageFaultActive)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_transport_dropped_links gauge\nkvnode_transport_dropped_links %d\n", s.transportDropCount())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_epaxos_instances gauge\nkvnode_epaxos_instances %d\n", runtimeStats.ResidentInstances)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_epaxos_executed gauge\nkvnode_epaxos_executed %d\n", runtimeStats.ExecutedRefs)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_epaxos_deferred_preaccepts gauge\nkvnode_epaxos_deferred_preaccepts %d\n", runtimeStats.DeferredPreAccepts)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_epaxos_active_recoveries gauge\nkvnode_epaxos_active_recoveries %d\n", runtimeStats.ActiveRecoveries)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_epaxos_frozen_ready_records gauge\nkvnode_epaxos_frozen_ready_records %d\n", runtimeStats.FrozenReadyRecords)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_epaxos_frozen_ready_messages gauge\nkvnode_epaxos_frozen_ready_messages %d\n", runtimeStats.FrozenReadyMessages)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_epaxos_pending_ready_records gauge\nkvnode_epaxos_pending_ready_records %d\n", runtimeStats.PendingReadyRecords)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_epaxos_pending_ready_messages gauge\nkvnode_epaxos_pending_ready_messages %d\n", runtimeStats.PendingReadyMessages)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_epaxos_next_ready_records gauge\nkvnode_epaxos_next_ready_records %d\n", runtimeStats.NextReadyRecords)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_epaxos_next_ready_messages gauge\nkvnode_epaxos_next_ready_messages %d\n", runtimeStats.NextReadyMessages)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_epaxos_oldest_unexecuted_age_ticks gauge\nkvnode_epaxos_oldest_unexecuted_age_ticks %d\n", runtimeStats.OldestUnexecutedAgeTicks)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_transport_queued_frames gauge\nkvnode_transport_queued_frames %d\n", queueDepth)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_send_queue_depth gauge\nkvnode_send_queue_depth %d\n", queueDepth)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_transport_inflight_frames gauge\nkvnode_transport_inflight_frames %d\n", queueOwned-queueDepth)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_retention_level gauge\nkvnode_retention_level %d\n", retentionLevel)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_storage_cache_size_bytes gauge\nkvnode_storage_cache_size_bytes %d\n", storageStats.CacheSizeBytes)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_storage_cache_hits_total counter\nkvnode_storage_cache_hits_total %d\n", storageStats.CacheHits)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_storage_cache_misses_total counter\nkvnode_storage_cache_misses_total %d\n", storageStats.CacheMisses)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_storage_memtable_size_bytes gauge\nkvnode_storage_memtable_size_bytes %d\n", storageStats.MemTableSizeBytes)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_storage_memtable_count gauge\nkvnode_storage_memtable_count %d\n", storageStats.MemTableCount)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_storage_compaction_debt_bytes gauge\nkvnode_storage_compaction_debt_bytes %d\n", storageStats.CompactionDebtBytes)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_storage_compactions_in_progress gauge\nkvnode_storage_compactions_in_progress %d\n", storageStats.CompactionsInProgress)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_storage_wal_bytes_total counter\nkvnode_storage_wal_bytes_total %d\n", storageStats.WALBytes)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_storage_disk_usage_bytes gauge\nkvnode_storage_disk_usage_bytes %d\n", storageStats.DiskUsageBytes)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_storage_read_amplification gauge\nkvnode_storage_read_amplification %d\n", storageStats.ReadAmplification)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_storage_durable_instance_records gauge\nkvnode_storage_durable_instance_records %d\n", storageStats.DurableInstanceRecords)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_transport_retry_frames gauge\nkvnode_transport_retry_frames %d\n", retryOwned)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_retry_queue_depth gauge\nkvnode_outbound_retry_queue_depth %d\n", retryOwned)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_frames_outstanding gauge\nkvnode_outbound_frames_outstanding %d\n", outstanding)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_ready_admission_blocked gauge\nkvnode_ready_admission_blocked %d\n", blocked)
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_frames_attempted_total counter\nkvnode_outbound_frames_attempted_total %d\n", s.outboundMetrics.attempted.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_frames_admitted_total counter\nkvnode_outbound_frames_admitted_total %d\n", s.outboundMetrics.admitted.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_frames_retried_total counter\nkvnode_outbound_frames_retried_total %d\n", s.outboundMetrics.retried.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_frames_backpressured_total counter\nkvnode_outbound_frames_backpressured_total %d\n", s.outboundMetrics.backpressured.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_frames_terminal_failed_total counter\nkvnode_outbound_frames_terminal_failed_total %d\n", s.outboundMetrics.terminalFailed.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_retry_tick_overflow_total counter\nkvnode_outbound_retry_tick_overflow_total %d\n", s.outboundMetrics.retryOverflow.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_transport_frame_pool_hits_total counter\nkvnode_transport_frame_pool_hits_total %d\n", s.metrics.framePoolHits.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_transport_frame_pool_misses_total counter\nkvnode_transport_frame_pool_misses_total %d\n", s.metrics.framePoolMisses.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_peer_body_pool_hits_total counter\nkvnode_peer_body_pool_hits_total %d\n", s.metrics.peerBodyPoolHits.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_peer_body_pool_misses_total counter\nkvnode_peer_body_pool_misses_total %d\n", s.metrics.peerBodyPoolMisses.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_encoded_bytes_total counter\nkvnode_outbound_encoded_bytes_total %d\n", s.metrics.outboundEncodedBytes.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_inbound_decoded_bytes_total counter\nkvnode_inbound_decoded_bytes_total %d\n", s.metrics.inboundDecodedBytes.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_ready_apply_failures_total counter\nkvnode_ready_apply_failures_total %d\n", s.metrics.readyApplyFailures.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_ready_admission_failures_total counter\nkvnode_ready_admission_failures_total %d\n", s.metrics.readyAdmissionFailures.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_protocol_terminal_errors_total counter\nkvnode_protocol_terminal_errors_total %d\n", s.metrics.protocolTerminalErrors.Load())
	_, _ = fmt.Fprintf(w, "# TYPE kvnode_outbound_oldest_retry_age_ticks gauge\nkvnode_outbound_oldest_retry_age_ticks %d\n", oldestRetryAge)
	writeRequestDurationHistogram(w, "kvnode_client_request_duration_seconds", &s.metrics.clientDuration)
	writeRequestDurationHistogram(w, "kvnode_peer_request_duration_seconds", &s.metrics.peerDuration)
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

func writeRequestDurationHistogram(w io.Writer, name string, histogram *requestDurationHistogram) {
	_, _ = fmt.Fprintf(w, "# TYPE %s histogram\n", name)
	cumulative := uint64(0)
	for index, upper := range requestDurationBuckets {
		cumulative += histogram.buckets[index].Load()
		_, _ = fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", name, upper.Seconds(), cumulative)
	}
	cumulative += histogram.buckets[len(requestDurationBuckets)].Load()
	_, _ = fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, cumulative)
	_, _ = fmt.Fprintf(w, "%s_sum %g\n", name, float64(histogram.sumNS.Load())/float64(time.Second))
	_, _ = fmt.Fprintf(w, "%s_count %d\n", name, histogram.count.Load())
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
			command := kv.CommandForGet(uint64(s.id), s.next(), key)
			if err = s.proposeAndWait(ctx, command); err == nil {
				var result kv.OrderedGetResult
				var found bool
				value, found, err = s.db.CommandResult(command.ID)
				if err == nil && !found {
					err = fmt.Errorf("ordered read completed without a durable result")
				}
				if err == nil {
					result, err = kv.DecodeOrderedGetResult(value)
					value, ok = result.Value, result.Found
				}
			}
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
		if err := s.proposeAndWait(ctx, kv.CommandForPut(uint64(s.id), s.next(), key, body)); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if s.storageFaultActive() {
			http.Error(w, "storage fault active", http.StatusServiceUnavailable)
			return
		}
		if err := s.proposeAndWait(ctx, kv.CommandForDelete(uint64(s.id), s.next(), key)); err != nil {
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
	if err := s.proposeAndWait(ctx, kv.CommandForTxn(uint64(s.id), s.next(), ops)); err != nil {
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
	if hasBounds && q.Get("barrier") != "" {
		http.Error(w, "bad timestamp selector", http.StatusBadRequest)
		return
	}
	ctx, cancel := s.requestContext(r.Context())
	defer cancel()
	if s.storageFaultActive() {
		http.Error(w, "storage fault active", http.StatusServiceUnavailable)
		return
	}
	options := kv.ScanOptions{
		Start: start, End: end, Prefix: prefix, Limit: limit, Reverse: reverse, Bounds: bounds,
	}
	var rows []kv.KV
	if hasBounds {
		rows, err = s.db.Scan(options)
	} else {
		var command epaxos.Command
		command, err = kv.CommandForScan(uint64(s.id), s.next(), options)
		if err == nil {
			if proposeErr := s.proposeAndWait(ctx, command); proposeErr != nil {
				http.Error(w, proposeErr.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		if err == nil {
			var response []byte
			var found bool
			response, found, err = s.db.CommandResult(command.ID)
			if err == nil && !found {
				err = fmt.Errorf("ordered scan completed without a durable result")
			}
			if err == nil {
				var result kv.OrderedScanResult
				result, err = kv.DecodeOrderedScanResult(response)
				rows = result.Rows
			}
		}
	}
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

func (s *service) validateInboundPeer(r *http.Request, message epaxos.Message) error {
	if message.To != s.id {
		return fmt.Errorf("message target %d does not match local replica %d", message.To, s.id)
	}
	if _, configured := s.peers[message.From]; !configured {
		return fmt.Errorf("message sender %d is not configured", message.From)
	}
	if !s.production {
		return nil
	}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return fmt.Errorf("verified peer client certificate is required")
	}
	certificateID, err := peerReplicaIDFromCertificate(r.TLS.PeerCertificates[0])
	if err != nil {
		return err
	}
	if certificateID != message.From {
		return fmt.Errorf("peer certificate replica %d does not match message sender %d", certificateID, message.From)
	}
	return nil
}

func (s *service) validateSnapshotPeer(r *http.Request) error {
	if !s.production {
		return nil
	}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return fmt.Errorf("verified peer client certificate is required")
	}
	replica, err := peerReplicaIDFromCertificate(r.TLS.PeerCertificates[0])
	if err != nil {
		return err
	}
	if _, configured := s.peers[replica]; !configured {
		return fmt.Errorf("peer certificate replica %d is not configured", replica)
	}
	return nil
}

func (s *service) handleSnapshotBundle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.validateSnapshotPeer(r); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	encoded := strings.TrimPrefix(r.URL.Path, "/epaxos/snapshot/")
	if len(encoded) != 80 {
		http.Error(w, "bad application snapshot handle", http.StatusBadRequest)
		return
	}
	handle, err := hex.DecodeString(encoded)
	if err != nil {
		http.Error(w, "bad application snapshot handle", http.StatusBadRequest)
		return
	}
	bundle, err := s.db.ApplicationSnapshotBundle(handle)
	if errors.Is(err, pebble.ErrNotFound) {
		http.Error(w, "application snapshot not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(bundle)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(bundle)
}

func (s *service) materializeCheckpointOffer(ctx context.Context, message epaxos.Message) error {
	checkpoint, offered, err := epaxos.CheckpointOffer(message)
	if err != nil || !offered {
		return err
	}
	handle := checkpoint.ApplicationSnapshot
	size, err := kv.ApplicationSnapshotBundleSize(handle)
	if err != nil {
		return err
	}
	if size > uint64(s.peerBodyLimit()) {
		return fmt.Errorf("application snapshot size %d exceeds peer body limit", size)
	}
	if _, err := s.db.ApplicationSnapshotBundle(handle); err == nil {
		return nil
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	endpoint := s.peers[message.From]
	client := s.peerClients[message.From]
	if endpoint == "" || client == nil {
		return fmt.Errorf("checkpoint source replica %d has no peer transport", message.From)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		endpoint+"/epaxos/snapshot/"+hex.EncodeToString(handle), nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, transportReadChunkSize))
		return fmt.Errorf("checkpoint source replica %d returned %s", message.From, response.Status)
	}
	if response.ContentLength >= 0 && uint64(response.ContentLength) != size {
		return fmt.Errorf("application snapshot content length %d does not match advertised %d", response.ContentLength, size)
	}
	bundle, err := readLimitedBody(response.Body, int64(size))
	if err != nil {
		return err
	}
	if uint64(len(bundle)) != size {
		return fmt.Errorf("application snapshot length %d does not match advertised %d", len(bundle), size)
	}
	return s.db.MaterializeApplicationSnapshot(handle, bundle)
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
	if err := s.validateInboundPeer(r, msg); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	s.metrics.inboundDecodedBytes.Add(uint64(len(body)))
	if s.transportDropped(msg.From, s.id) {
		http.Error(w, "transport link dropped", http.StatusConflict)
		return
	}
	if s.storageFaultActive() {
		http.Error(w, "storage fault active", http.StatusServiceUnavailable)
		return
	}
	if err := s.materializeCheckpointOffer(r.Context(), msg); err != nil {
		http.Error(w, fmt.Sprintf("materialize checkpoint offer: %v", err), http.StatusServiceUnavailable)
		return
	}
	s.mu.Lock()
	s.evaluateRetentionLocked()
	if s.terminalErr != nil || s.shuttingDown {
		unavailable := s.terminalErr
		if unavailable == nil {
			unavailable = errServiceShuttingDown
		}
		s.mu.Unlock()
		http.Error(w, unavailable.Error(), http.StatusServiceUnavailable)
		return
	}
	wasAdmissionBlocked := s.admissionBlocked
	var blockedFingerprint [sha256.Size]byte
	if wasAdmissionBlocked {
		blockedFingerprint = sha256.Sum256(body)
		accepted := s.blockedPeerIngress[msg.From]
		for _, fingerprint := range accepted {
			if fingerprint == blockedFingerprint {
				s.mu.Unlock()
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		if len(accepted) >= cap(s.sendq) {
			s.mu.Unlock()
			http.Error(w, errSendQueueBackpressure.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	err = s.node.Step(msg)
	if err == nil && wasAdmissionBlocked {
		if s.blockedPeerIngress == nil {
			s.blockedPeerIngress = make(map[epaxos.ReplicaID][][sha256.Size]byte, len(s.peers))
		}
		s.blockedPeerIngress[msg.From] = append(s.blockedPeerIngress[msg.From], blockedFingerprint)
	}
	drainErr := s.drainLocked()
	if drainErr != nil {
		s.setTerminalLocked(drainErr)
	}
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
	s.evaluateRetentionLocked()
	if s.terminalErr != nil {
		err := s.terminalErr
		s.mu.Unlock()
		return err
	}
	if s.shuttingDown {
		s.mu.Unlock()
		return errServiceShuttingDown
	}
	if s.admissionBlocked {
		s.mu.Unlock()
		return errSendQueueBackpressure
	}
	if s.retentionLevel >= 2 {
		s.mu.Unlock()
		return errRetentionPressure
	}
	ref, err := s.node.Propose(cmd)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if s.waiters == nil {
		s.waiters = make(map[epaxos.InstanceRef]*proposalWaiter)
	}
	waiter := &proposalWaiter{result: make(chan error, 1)}
	s.waiters[ref] = waiter
	if err := s.drainLocked(); err != nil {
		s.setTerminalLocked(err)
	}
	s.evaluateRetentionLocked()
	if s.node.IsExecuted(ref) {
		if current, ok := s.waiters[ref]; ok && current == waiter {
			delete(s.waiters, ref)
			waiter.result <- nil
		}
	}
	s.mu.Unlock()

	select {
	case result := <-waiter.result:
		return result
	case <-ctx.Done():
		s.mu.Lock()
		if current, ok := s.waiters[ref]; ok && current == waiter {
			delete(s.waiters, ref)
			s.mu.Unlock()
			return ctx.Err()
		}
		s.mu.Unlock()
		return <-waiter.result
	}
}

func (s *service) executedLocked(ref epaxos.InstanceRef) bool {
	return s.node.IsExecuted(ref)
}

// prepareOutboundFrames returns a service-owned borrowed slice for one-message
// batches. Callers must finish admitting or disposing that batch before the
// next call while holding service.mu.
func (s *service) prepareOutboundFrames(messages []epaxos.Message) ([]*outboundFrame, error) {
	if len(messages) == 0 {
		return nil, nil
	}
	s.outboundMetrics.attempted.Add(uint64(len(messages)))
	var frames []*outboundFrame
	if len(messages) == 1 {
		s.singlePreparedFrame[0] = nil
		frames = s.singlePreparedFrame[:]
	} else {
		frames = make([]*outboundFrame, 0, len(messages))
	}
	releasePrepared := func() {
		for _, frame := range frames {
			s.releaseOutboundFrame(frame)
		}
		if len(messages) == 1 {
			s.singlePreparedFrame[0] = nil
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
		s.metrics.outboundEncodedBytes.Add(uint64(actualSize))
		frame.to = message.To
		frame.endpoint = s.peers[message.To]
		if len(messages) == 1 {
			frames[0] = frame
		} else {
			frames = append(frames, frame)
		}
	}
	return frames, nil
}

func (s *service) clearSinglePreparedBorrow(frames []*outboundFrame) {
	if len(frames) == 1 && &frames[0] == &s.singlePreparedFrame[0] {
		s.singlePreparedFrame[0] = nil
	}
}

func (s *service) admitOutboundFrames(frames []*outboundFrame) error {
	if len(frames) == 0 {
		return nil
	}
	s.transportMu.Lock()
	if s.transportClosed {
		s.transportMu.Unlock()
		s.outboundMetrics.backpressured.Add(uint64(len(frames)))
		for _, frame := range frames {
			s.releaseOutboundFrame(frame)
		}
		return &transportAdmissionError{Cause: errTransportClosed, Attempted: len(frames)}
	}
	available := cap(s.sendq) - s.queueOwned
	if queueSlots := cap(s.sendq) - len(s.sendq); queueSlots < available {
		available = queueSlots
	}
	if len(frames) > available {
		s.transportMu.Unlock()
		s.outboundMetrics.backpressured.Add(uint64(len(frames)))
		return errSendQueueBackpressure
	}
	// drainLocked is the sole producer of newly admitted frames. Reserve the
	// complete visible Ready prefix before publishing any frame.
	for _, frame := range frames {
		frame.admitted = true
		s.queueOwned++
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
			s.admissionBlocked = false
			s.blockedPeerIngress = nil
			s.completeExecutedWaitersLocked()
			return nil
		}
		if err := s.node.ReadyInto(&s.reusableReady); err != nil {
			return fmt.Errorf("freeze Ready: %w", err)
		}
		if !s.readyApplied {
			if err := s.ready.ApplyReady(s.reusableReady); err != nil {
				s.metrics.readyApplyFailures.Add(1)
				s.terminalReason = "storage-failed"
				return fmt.Errorf("apply Ready: %w", err)
			}
			s.readyApplied = true
		}
		if !s.readyCheckpointApplied && s.reusableReady.Checkpoint != nil {
			result, err := s.db.CreateApplicationCheckpoint(*s.reusableReady.Checkpoint)
			if err != nil {
				return fmt.Errorf("create Ready checkpoint: %w", err)
			}
			if err := s.node.ProvideCheckpoint(result); err != nil {
				return fmt.Errorf("provide Ready checkpoint: %w", err)
			}
			s.readyCheckpointApplied = true
		}
		for s.readyLoadsApplied < len(s.reusableReady.RecordLoads) {
			ref := s.reusableReady.RecordLoads[s.readyLoadsApplied]
			rec, found, err := s.ready.LoadInstance(ref)
			if err != nil {
				return fmt.Errorf("load Ready record %s: %w", ref, err)
			}
			if err := s.node.ProvideRecordLoad(epaxos.RecordLoadResult{Ref: ref, Record: rec, Found: found}); err != nil {
				return fmt.Errorf("provide Ready record %s: %w", ref, err)
			}
			s.readyLoadsApplied++
		}
		if s.frozenFrames == nil {
			frames, err := s.prepareOutboundFrames(s.reusableReady.Messages)
			if err != nil {
				s.metrics.readyAdmissionFailures.Add(1)
				return err
			}
			s.frozenFrames = frames
		}
		if err := s.admitOutboundFrames(s.frozenFrames); err != nil {
			s.metrics.readyAdmissionFailures.Add(1)
			if errors.Is(err, errSendQueueBackpressure) {
				if !s.admissionBlocked {
					s.blockedPeerIngress = nil
				}
				s.admissionBlocked = true
				return nil
			}
			s.clearSinglePreparedBorrow(s.frozenFrames)
			s.frozenFrames = nil
			return err
		}
		s.clearSinglePreparedBorrow(s.frozenFrames)
		s.frozenFrames = nil
		if err := s.node.Advance(s.reusableReady); err != nil {
			return fmt.Errorf("advance Ready: %w", err)
		}
		s.readyApplied = false
		s.readyCheckpointApplied = false
		s.readyLoadsApplied = 0
		s.admissionBlocked = false
		s.blockedPeerIngress = nil
		s.completeExecutedWaitersLocked()
	}
	return fmt.Errorf("ready processing did not quiesce")
}

func (s *service) postFrame(frame *outboundFrame) postOutcome {
	if frame.to == s.id || frame.endpoint == "" || s.transportDropped(s.id, frame.to) {
		return postTerminalFailure
	}
	client := s.peerClients[frame.to]
	if client == nil {
		client = s.client
	}
	if client == nil {
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
	response, err := client.Do(request)
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

func (s *service) signalTransportSpaceLocked() {
	if s.transportSpace == nil {
		return
	}
	select {
	case s.transportSpace <- struct{}{}:
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
	s.signalTransportSpaceLocked()
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
	if frame.admitted {
		if frame.inRetry {
			if s.retryOwned > 0 {
				s.retryOwned--
			}
		} else if s.queueOwned > 0 {
			s.queueOwned--
		}
		if s.outstandingFrames > 0 {
			s.outstandingFrames--
		}
		frame.admitted = false
		frame.inRetry = false
		s.signalTransportSpaceLocked()
	}
	s.releaseOutboundFrame(frame)
	s.transportMu.Unlock()
	if terminal {
		s.outboundMetrics.terminalFailed.Add(1)
	}
}

func (s *service) scheduleRetry(frame *outboundFrame) *outboundFrame {
	delay := retryDelayTicks(frame.retryAttempt)
	if frame.retryAttempt == 0 {
		s.transportMu.Lock()
		frame.retryStarted = s.transportTick
		s.transportMu.Unlock()
	}
	for {
		s.transportMu.Lock()
		overflow := s.transportTick > ^uint64(0)-delay
		if s.transportClosed || overflow {
			s.transportMu.Unlock()
			if overflow {
				s.outboundMetrics.retryOverflow.Add(1)
			}
			s.finishOutboundFrame(frame, true)
			return nil
		}
		if s.retryOwned < s.retryCapacity {
			s.prepareRetryLocked(frame, delay)
			if s.queueOwned > 0 {
				s.queueOwned--
			}
			s.retryOwned++
			heap.Push(&s.retryq, frame)
			s.signalTransportSpaceLocked()
			s.transportMu.Unlock()
			s.outboundMetrics.retried.Add(1)
			return nil
		}
		if len(s.retryq) != 0 && s.retryq[0].retryAt <= s.transportTick {
			due := heap.Pop(&s.retryq).(*outboundFrame)
			s.prepareRetryLocked(frame, delay)
			heap.Push(&s.retryq, frame)
			due.inRetry = false
			s.signalTransportSpaceLocked()
			s.transportMu.Unlock()
			s.outboundMetrics.retried.Add(1)
			return due
		}
		space := s.transportSpace
		ctx := s.transportCtx
		s.transportMu.Unlock()
		if ctx == nil {
			<-space
			continue
		}
		select {
		case <-ctx.Done():
			s.finishOutboundFrame(frame, true)
			return nil
		case <-space:
		}
	}
}

func (s *service) prepareRetryLocked(frame *outboundFrame, delay uint64) {
	frame.retryAt = s.transportTick + delay
	frame.retryOrder = s.retryOrder
	s.retryOrder++
	if frame.retryAttempt < ^uint32(0) {
		frame.retryAttempt++
	}
	frame.inRetry = true
}

func (s *service) releaseDueRetries() bool {
	s.transportMu.Lock()
	if s.transportClosed {
		s.transportMu.Unlock()
		return false
	}
	now := s.transportTick
	for len(s.retryq) != 0 && s.retryq[0].retryAt <= now && s.queueOwned < cap(s.sendq) {
		frame := heap.Pop(&s.retryq).(*outboundFrame)
		s.retryOwned--
		s.queueOwned++
		frame.inRetry = false
		s.sendq <- frame
		s.signalTransportSpaceLocked()
	}
	s.schedulerTick.Store(now)
	s.transportMu.Unlock()
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
	if s.transportSpace == nil {
		s.transportSpace = make(chan struct{}, 1)
	}
	if s.retryCapacity <= 0 {
		s.retryCapacity = cap(s.sendq)
	}
	if s.retryq == nil {
		s.retryq = make(outboundRetryHeap, 0, s.retryCapacity)
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
				if s.retryOwned > 0 {
					s.retryOwned--
				}
				if s.outstandingFrames > 0 {
					s.outstandingFrames--
				}
				frame.admitted = false
				frame.inRetry = false
			}
		}
		if s.sendq != nil {
			close(s.sendq)
		}
		s.signalTransportSchedulerLocked()
		s.signalTransportSpaceLocked()
		s.transportMu.Unlock()
		for _, frame := range retrying {
			s.outboundMetrics.terminalFailed.Add(1)
			s.releaseOutboundFrame(frame)
		}
		if s.client != nil {
			s.client.CloseIdleConnections()
		}
		for _, client := range s.peerClients {
			client.CloseIdleConnections()
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
	for frame != nil {
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
			frame = nil
		case postTerminalFailure:
			s.finishOutboundFrame(frame, true)
			frame = nil
		case postRetry:
			if s.transportCtx != nil {
				select {
				case <-s.transportCtx.Done():
					s.finishOutboundFrame(frame, true)
					return
				default:
				}
			}
			frame = s.scheduleRetry(frame)
		}
	}
}

func errorsJoin(a, b error) error {
	return errors.Join(a, b)
}
