// Package epaxos implements a deterministic, library-embedded EPaxos core.
//
// RawNode follows the same ownership split as raft-style libraries: the
// application owns durable storage, transport, and logical ticking, while the
// node owns protocol state. Calls to Propose and Step only mutate local state.
// All externally visible work is returned through Ready.
//
// A typical integration loop persists records before sending messages or
// acknowledging committed commands:
//
//	for rn.HasReady() {
//		rd := rn.Ready()
//		if err := storage.ApplyReady(rd); err != nil {
//			return err
//		}
//		for _, msg := range rd.Messages {
//			send(msg)
//		}
//		if err := apply(rd.Committed); err != nil {
//			return err
//		}
//		if err := rn.Advance(rd); err != nil {
//			return err
//		}
//		rd.Release()
//	}
//
// Advance accepts exact prefixes of the outstanding Ready. Applications may
// acknowledge only records after partial durable writes, but messages and
// committed commands require all records from the same Ready batch to be
// acknowledged first. This preserves the durable-before-visible barrier.
// If storage or application work fails before Advance, Ready returns the same
// outstanding batch again so the caller can retry without losing progress.
//
// Transport integrations use EncodeMessage to append into caller-owned buffers
// and DecodeMessageWithScratch to reuse decoded dependency and conflict-key
// slice headers. Decoded payload and conflict-key bytes alias the input buffer;
// Step copies inbound command bytes before retaining them.
//
// By default Propose clones command payload and conflict-key slices. When
// Config.ZeroCopyProposals is true, the caller transfers ownership of those
// slices to the node until they are no longer observable through Ready or
// Status.
package epaxos
