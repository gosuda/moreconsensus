//go:build kvnode

package main

import (
	"fmt"
	"net/http"
	"testing"

	"gosuda.org/moreconsensus/epaxos"
	"gosuda.org/moreconsensus/examples/kv"
)

var benchmarkFrames []*outboundFrame
var benchmarkInbound epaxos.Message

func benchmarkPeerMessage(size int) epaxos.Message {
	return epaxos.Message{
		Type: epaxos.MsgPreAccept, From: 1, To: 2,
		Ref:    epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Ballot: epaxos.Ballot{Replica: 1}, Seq: 1,
		Deps:    []epaxos.InstanceNum{0, 0, 0},
		Command: epaxos.Command{Kind: epaxos.CommandUser, Payload: make([]byte, size), ConflictKeys: [][]byte{[]byte("fixed-conflict-key")}},
	}
}

func benchmarkService(b *testing.B, voters []epaxos.ReplicaID, id epaxos.ReplicaID, queue int) *service {
	b.Helper()
	db, err := kv.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	node, err := epaxos.NewRawNode(epaxos.Config{ID: id, Voters: voters, Storage: db.EPaxosStorage(), RetryTicks: 2, RecoveryTicks: 5})
	if err != nil {
		b.Fatal(err)
	}
	peers := make(map[epaxos.ReplicaID]string, len(voters))
	for _, voter := range voters {
		peers[voter] = fmt.Sprintf("http://127.0.0.1:%d", 9000+voter)
	}
	return &service{id: id, node: node, ready: db, db: db, peers: peers, client: &http.Client{}, sendq: make(chan *outboundFrame, queue), nextSeq: 1, maxPeerBodyBytes: 2 << 20}
}

func BenchmarkPrepareOutboundFrames(b *testing.B) {
	for _, size := range []int{64, 1024, 64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			s := &service{peers: map[epaxos.ReplicaID]string{2: "http://127.0.0.1:9002"}, maxPeerBodyBytes: 2 << 20}
			message := benchmarkPeerMessage(size)
			b.ReportAllocs()
			for range b.N {
				frames, err := s.prepareOutboundFrames([]epaxos.Message{message})
				if err != nil {
					b.Fatal(err)
				}
				benchmarkFrames = frames
				for _, frame := range frames {
					s.releaseOutboundFrame(frame)
				}
			}
		})
	}
}

func BenchmarkInboundStep(b *testing.B) {
	for _, voters := range []int{3, 5, 7} {
		b.Run(fmt.Sprintf("voters=%d", voters), func(b *testing.B) {
			ids := make([]epaxos.ReplicaID, voters)
			for i := range ids {
				ids[i] = epaxos.ReplicaID(i + 1)
			}
			for _, size := range []int{64, 1024, 64 << 10, 1 << 20} {
				b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
					message := benchmarkPeerMessage(size)
					message.Deps = make([]epaxos.InstanceNum, voters)
					body, err := epaxos.EncodeMessage(nil, message)
					if err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.StopTimer()
					for range b.N {
						node, err := epaxos.NewRawNode(epaxos.Config{ID: 2, Voters: ids})
						if err != nil {
							b.Fatal(err)
						}
						var decoded epaxos.Message
						var scratch epaxos.DecodeScratch
						b.StartTimer()
						err = epaxos.DecodeMessageWithScratch(body, &decoded, &scratch)
						if err == nil {
							err = node.Step(decoded)
						}
						b.StopTimer()
						if err != nil {
							b.Fatal(err)
						}
						benchmarkInbound = decoded
					}
				})
			}
		})
	}
}

func BenchmarkRetryAdmission(b *testing.B) {
	s := &service{sendq: make(chan *outboundFrame, 1)}
	s.outstandingFrames = 1
	b.ReportAllocs()
	for range b.N {
		frame := s.getOutboundFrame()
		err := s.admitOutboundFrames([]*outboundFrame{frame})
		if err == nil {
			b.Fatal("saturated admission unexpectedly succeeded")
		}
	}
}

func BenchmarkDrainLocked(b *testing.B) {
	for _, size := range []int{64, 1024, 64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			s := benchmarkService(b, []epaxos.ReplicaID{1}, 1, 8)
			cmd := kv.CommandForPut(1, 1, []byte("fixed-conflict-key"), make([]byte, size))
			b.ReportAllocs()
			b.StopTimer()
			for range b.N {
				if _, err := s.node.Propose(cmd); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				err := s.drainLocked()
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
