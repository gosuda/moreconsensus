package kv

import (
	"fmt"
	"testing"

	"gosuda.org/moreconsensus/epaxos"
)

func BenchmarkApplyReady(b *testing.B) {
	for _, count := range []int{1, 32} {
		b.Run(fmt.Sprintf("records=%d/sync", count), func(b *testing.B) {
			b.StopTimer()
			db, err := Open(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = db.Close() })
			records := make([]epaxos.InstanceRecord, count)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := range b.N {
				for index := range records {
					record := epaxos.InstanceRecord{
						Ref:    epaxos.InstanceRef{Replica: 1, Instance: epaxos.InstanceNum(uint64(iteration)*uint64(count) + uint64(index) + 1), Conf: 1}, //nolint:gosec // benchmark loop variables are positive
						Ballot: epaxos.Ballot{Replica: 1}, RecordBallot: epaxos.Ballot{Replica: 1},
						Status: epaxos.StatusAccepted, Seq: 1, Deps: []epaxos.InstanceNum{0}, Kind: epaxos.EntryCommand,
						Command: epaxos.Command{Payload: []byte("value"), Footprint: epaxos.Footprint{Points: [][]byte{[]byte("fixed-conflict-key")}}},
					}
					record.Checksum = epaxos.ChecksumRecord(record)
					records[index] = record
				}
				b.StartTimer()
				err = db.ApplyReady(epaxos.Ready{Records: records, MustSync: true})
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
