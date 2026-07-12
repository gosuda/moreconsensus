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
						Ref:    epaxos.InstanceRef{Replica: 1, Instance: epaxos.InstanceNum(iteration*count + index + 1), Conf: 1},
						Ballot: epaxos.Ballot{Replica: 1}, RecordBallot: epaxos.Ballot{Replica: 1},
						Status: epaxos.StatusAccepted, Seq: 1, Deps: []epaxos.InstanceNum{0},
						Command: epaxos.Command{Kind: epaxos.CommandUser, Payload: []byte("value"), ConflictKeys: [][]byte{[]byte("fixed-conflict-key")}},
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
