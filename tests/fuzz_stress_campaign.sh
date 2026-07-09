#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fuzztime="${MORECONSENSUS_FUZZTIME:-5s}"

# Fixed-seed stress covers randomized transport, restart, checksum, and simulator convergence.
go test ./epaxos -run 'TestDeterministicRandomizedCoreSimulationConverges|TestDuplicateMessagesAndMalformedInput|TestCodecChecksumZeroCopy|TestMessageCodecProcessAtRoundTripAndChecksum' -count=1

# Bounded fuzzing exercises DecodeMessage over the committed seed corpus plus generated frames.
go test ./epaxos -run '^$' -fuzz '^FuzzDecodeMessage$' -fuzztime="$fuzztime" -parallel=1
