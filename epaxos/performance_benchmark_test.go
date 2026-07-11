package epaxos

import (
	"fmt"
	"testing"
	"unsafe"
)

var benchmarkReady Ready
var benchmarkBytes []byte
var benchmarkMessage Message

func benchmarkCommand(size int) Command {
	return Command{Kind: CommandUser, Payload: make([]byte, size), ConflictKeys: [][]byte{[]byte("fixed-conflict-key")}}
}

func BenchmarkProposeReadyAdvance(b *testing.B) {
	for _, voters := range []int{3, 5, 7} {
		for _, size := range []int{64, 1024, 64 << 10, 1 << 20} {
			b.Run(fmt.Sprintf("voters=%d/payload=%d", voters, size), func(b *testing.B) {
				b.ReportAllocs()
				cmd := benchmarkCommand(size)
				for i := 0; i < b.N; i++ {
					n, err := NewRawNode(Config{ID: 1, Voters: makeIDs(voters)})
					if err != nil {
						b.Fatal(err)
					}
					if _, err = n.Propose(cmd); err != nil {
						b.Fatal(err)
					}
					rd := n.Ready()
					if err = n.Advance(rd); err != nil {
						b.Fatal(err)
					}
					benchmarkReady = rd
				}
			})
		}
	}
}

func BenchmarkReadyRetry(b *testing.B) {
	for _, voters := range []int{3, 5, 7} {
		b.Run(fmt.Sprintf("voters=%d", voters), func(b *testing.B) {
			n, err := NewRawNode(Config{ID: 1, Voters: makeIDs(voters)})
			if err != nil {
				b.Fatal(err)
			}
			if _, err = n.Propose(benchmarkCommand(64)); err != nil {
				b.Fatal(err)
			}
			var dst Ready
			if err = n.ReadyInto(&dst); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err = n.ReadyInto(&dst); err != nil {
					b.Fatal(err)
				}
				benchmarkReady = dst
			}
		})
	}
}

func BenchmarkEncodeMessage(b *testing.B) {
	for _, size := range []int{64, 1024, 64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			m := Message{Type: MsgPreAccept, From: 1, To: 2, Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Ballot: Ballot{Replica: 1}, Seq: 1, Deps: []InstanceNum{0, 0, 0}, Command: benchmarkCommand(size)}
			buf := make([]byte, 0, size+512)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				buf, err = EncodeMessage(buf[:0], m)
				if err != nil {
					b.Fatal(err)
				}
			}
			benchmarkBytes = buf
		})
	}
}

func BenchmarkDecodeMessageWithScratch(b *testing.B) {
	for _, size := range []int{64, 1024, 64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			m := Message{Type: MsgPreAccept, From: 1, To: 2, Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Ballot: Ballot{Replica: 1}, Seq: 1, Deps: []InstanceNum{0, 0, 0}, Command: benchmarkCommand(size)}
			frame, err := EncodeMessage(nil, m)
			if err != nil {
				b.Fatal(err)
			}
			var dst Message
			var scratch DecodeScratch
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err = DecodeMessageWithScratch(frame, &dst, &scratch); err != nil {
					b.Fatal(err)
				}
			}
			benchmarkMessage = dst
		})
	}
}

func BenchmarkStepPreAccept(b *testing.B) {
	for _, voters := range []int{3, 5, 7} {
		b.Run(fmt.Sprintf("voters=%d", voters), func(b *testing.B) {
			for _, size := range []int{64, 1024, 64 << 10, 1 << 20} {
				b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						n, err := NewRawNode(Config{ID: 2, Voters: makeIDs(voters)})
						if err != nil {
							b.Fatal(err)
						}
						m := Message{Type: MsgPreAccept, From: 1, To: 2, Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Ballot: Ballot{Replica: 1}, Seq: 1, Deps: make([]InstanceNum, voters), Command: benchmarkCommand(size)}
						if err = n.Step(m); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkDeferredTOQ(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	for range b.N {
		clock := uint64(10)
		conf := ConfState{ID: 1, Voters: makeIDs(3)}
		n, err := NewRawNode(Config{
			ID: 2, Voters: conf.Voters, TOQ: true,
			TOQClock: func() uint64 { return clock },
			TOQRuntime: &TOQRuntimeConfig{
				Conf:        conf,
				OneWayDelay: map[ReplicaID]uint64{1: 1, 2: 0, 3: 1},
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		m := Message{
			Type: MsgPreAccept, From: 1, To: 2,
			Ref:     InstanceRef{Replica: 1, Instance: 1, Conf: 1},
			Ballot:  Ballot{Replica: 1},
			Command: benchmarkCommand(64), TOQ: true, ProcessAt: 11,
		}
		if err = n.Step(m); err != nil {
			b.Fatal(err)
		}
		clock = 11
		b.StartTimer()
		err = n.ProcessTOQ()
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOptimizedRecovery(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	for range b.N {
		n, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), RetryTicks: 10, RecoveryTicks: 10})
		if err != nil {
			b.Fatal(err)
		}
		ref := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
		inst := &instance{rec: InstanceRecord{Ref: ref, Status: StatusNone, Deps: n.q.deps()}}
		n.instances[ref] = inst
		n.startPrepare(inst)
		ballot := inst.rec.Ballot
		rd := n.Ready()
		if err = n.Advance(rd); err != nil {
			b.Fatal(err)
		}
		response := Message{
			Type: MsgPrepareResp, From: 2, To: 1, Ref: ref, Ballot: ballot,
			RecordStatus: StatusNone, Deps: n.q.deps(),
		}
		b.StartTimer()
		err = n.Step(response)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if inst.phase != phaseAccept || inst.rec.Command.Kind != CommandNoop {
			b.Fatal("optimized recovery decision did not enter no-op accept")
		}
	}
}

func BenchmarkExecutionDrive(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	for range b.N {
		n, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
		if err != nil {
			b.Fatal(err)
		}
		ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		rec := InstanceRecord{
			Ref: ref, Ballot: Ballot{Replica: 1}, RecordBallot: Ballot{Replica: 1},
			Status: StatusCommitted, Seq: 1, Deps: make([]InstanceNum, 3),
			Command: benchmarkCommand(64),
		}
		rec.Checksum = ChecksumRecord(rec)
		n.instances[ref] = &instance{rec: rec, phase: phaseCommitted}
		b.StartTimer()
		n.tryExecute()
		b.StopTimer()
		if !n.executed.contains(ref) {
			b.Fatal("committed instance was not executed")
		}
	}
}

func BenchmarkPoolCommandRoundTrip(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{{"warm-hit", 64}, {"retained-boundary", 64 << 10}, {"oversized-drop", (64 << 10) + 1}} {
		b.Run(tc.name, func(b *testing.B) {
			payload := make([]byte, tc.size)
			c := GetCommand()
			c.Payload = make([]byte, 0, tc.size)
			PutCommand(c)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				c = GetCommand()
				c.Payload = append(c.Payload, payload...)
				PutCommand(c)
			}
		})
	}
	b.Run("cold-miss", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			c := new(Command)
			c.Payload = make([]byte, 64)
			resetCommandForPool(c)
		}
	})
}

func BenchmarkPoolMessageRoundTrip(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{{"warm-hit", 64}, {"retained-boundary", 64 << 10}, {"oversized-drop", (64 << 10) + 1}} {
		b.Run(tc.name, func(b *testing.B) {
			payload := make([]byte, tc.size)
			m := GetMessage()
			m.Command.Payload = make([]byte, 0, tc.size)
			PutMessage(m)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				m = GetMessage()
				m.Command.Payload = append(m.Command.Payload, payload...)
				PutMessage(m)
			}
		})
	}
	b.Run("cold-miss", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			m := new(Message)
			m.Command.Payload = make([]byte, 64)
			resetMessageForPool(m)
		}
	})
}

func BenchmarkLiveInstanceRetention(b *testing.B) {
	const corpusSize = 64
	for _, voters := range []int{3, 5, 7} {
		b.Run(fmt.Sprintf("voters=%d", voters), func(b *testing.B) {
			corpus := make([]instance, corpusSize)
			var resident uintptr
			for i := range corpus {
				rec := InstanceRecord{
					Ref:    InstanceRef{Replica: ReplicaID(i%voters + 1), Instance: InstanceNum(i/voters + 1), Conf: 1},
					Ballot: Ballot{Replica: 1}, RecordBallot: Ballot{Replica: 1},
					Status: StatusExecuted, Seq: uint64(i + 1),
					Deps: make([]InstanceNum, voters, voters),
					Command: Command{
						Kind:         CommandUser,
						Payload:      make([]byte, 64, 64),
						ConflictKeys: [][]byte{make([]byte, len("fixed-conflict-key"), len("fixed-conflict-key"))},
					},
				}
				corpus[i] = instance{rec: rec, phase: phaseCommitted}
				resident += residentInstanceBytes(&corpus[i])
			}
			b.ReportMetric(float64(resident)/corpusSize, "resident-B/instance")
			b.ReportAllocs()
			var sink uintptr
			for range b.N {
				for i := range corpus {
					sink += residentInstanceBytes(&corpus[i])
				}
			}
			if sink == 0 {
				b.Fatal("unreachable")
			}
		})
	}
}

func residentInstanceBytes(inst *instance) uintptr {
	bytes := unsafe.Sizeof(*inst)
	bytes += uintptr(cap(inst.rec.Deps)) * unsafe.Sizeof(InstanceNum(0))
	bytes += uintptr(cap(inst.rec.Command.Payload))
	bytes += uintptr(cap(inst.rec.Command.ConflictKeys)) * unsafe.Sizeof([]byte(nil))
	for _, key := range inst.rec.Command.ConflictKeys {
		bytes += uintptr(cap(key))
	}
	return bytes
}
