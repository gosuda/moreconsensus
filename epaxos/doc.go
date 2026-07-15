// Package epaxos implements a deterministic, library-embedded EPaxos core.
//
// RawNode owns only deterministic protocol state. The embedding owns durable
// storage, transport, application state and responses, snapshot materialization,
// and any wall-clock sampling. Propose treats Command.Payload and Command.ID as
// opaque bytes and metadata; the core interprets only the canonical logical
// Footprint and replicated CycleKey. Protocol no-ops, configuration changes,
// certified membership controls, and checkpoint barriers are separate entry
// kinds and never appear in Ready.Apply.
//
// All external work leaves through Ready. An embedding processes each batch in
// phase order: atomically persist hard state, protocol records, and snapshot
// metadata; send messages; install a received application snapshot; apply
// Ready.Apply strictly in slice order; service Ready.Checkpoint; atomically
// execute Ready.Compact; then acknowledge the exact completed prefix with
// Advance. Ready work may repeat unchanged before Advance and after a crash.
// Application effects and the CommandID-to-response/digest record therefore
// must be committed atomically by the embedding.
//
//	for rn.HasReady() {
//		rd := rn.Ready()
//		if err := processInPhaseOrder(rd); err != nil {
//			return err
//		}
//		if err := rn.Advance(rd); err != nil {
//			return err
//		}
//		rd.Release()
//	}
//
// A Footprint contains canonical byte-lexicographic points, half-open spans, or
// explicit group-wide All scope. Nonoverlapping commands may execute in either
// order, so the embedding must guarantee strong commutativity of final state,
// each response, dedup state, and deterministic side effects. CycleKey orders
// equal-sequence members of an SCC after Seq and before InstanceRef.
//
// Transport integrations use EncodeMessage and DecodeMessageWithScratch.
// Decoded command and footprint bytes alias the input buffer; Step owns anything
// it retains. Propose normally clones payload, footprint, and cycle bytes. With
// ZeroCopyProposals, the caller transfers already-canonical buffers until they
// are no longer observable through Ready or Status.
//
// Ready.RecordLoads requests retained durable records; ProvideRecordLoad returns
// the result. Ready.Checkpoint requests an opaque application snapshot handle
// and digest; ProvideCheckpoint returns it. Certified checkpoints bind an exact
// execution frontier before Ready.Compact authorizes durable protocol deletion.
// Restart installs the checkpoint and replays only retained delta. The core
// never performs I/O or calls an application callback.
package epaxos
