package sim

func applyLearning(trace *ScenarioTrace) {
	current := learningFor(trace.Scenario.ID, "")
	for i := range trace.Frames {
		if trace.Frames[i].Headline != "" {
			current = learningFor(trace.Scenario.ID, trace.Frames[i].Headline)
		}
		lesson := current
		lesson.Algorithm = append([]string(nil), current.Algorithm...)
		trace.Frames[i].Learning = &lesson
	}
}

func learningFor(scenario, headline string) LearningView { //nolint:cyclop // The explicit chapter table keeps teaching copy beside its protocol state.
	switch scenario + ":" + headline {
	case "parallel:":
		return lesson("01 · COMMAND OWNERSHIP", "Start with instances, not a leader",
			"Every command is assigned to an instance owned by the replica that received it. Ownership names the lane; it does not grant permanent authority over other commands.",
			"Independent coordinators remove the single leader bottleneck. Safety comes from quorum evidence and dependency attributes, not from routing every client through one process.",
			"An instance is identified by owner, instance number, and configuration. At most one command can be chosen for that identity.",
			"Client selects any healthy coordinator.", "Coordinator allocates its next local instance.", "Command footprint declares every logical resource that can affect state, response, or deduplication.")
	case "parallel:No permanent leader":
		return lesson("01 · COMMAND OWNERSHIP", "Coordination is per command",
			"R1 owns the east instance while R3 owns the west instance. Both begin PreAccept without electing or consulting a cluster-wide leader.",
			"The owner is responsible for gathering one command's evidence. Another replica can coordinate the next command at the same time.",
			"A replica may own an instance only in its own lane, and the instance remains pinned to the configuration in which it was created.",
			"Allocate R1.1 and R3.1 independently.", "Persist each command before sending protocol messages.", "Count the owner's durable local vote exactly once.")
	case "parallel:Different keys, no dependencies":
		return lesson("01 · FOOTPRINTS", "Commutativity is declared as logical resources",
			"The two SET footprints contain different K/V resources, plus command-unique result and dedup keys. Their overlap is empty, so each replica computes an empty cross-command dependency.",
			"EPaxos may leave commuting commands unordered only when swapping them preserves state, responses, durable deduplication, and deterministic side effects.",
			"If two operations can observe or change the same logical invariant, their canonical footprints must overlap even when their physical storage keys differ.",
			"Canonicalize points and half-open ranges.", "Query the conflict index for earlier overlapping instances.", "Set Seq to one greater than the maximum dependency sequence.")
	case "parallel:Both take the fast path":
		return lesson("01 · PARALLEL FAST PATH", "Independent work commits in parallel",
			"Each owner receives matching attributes from its fast quorum and commits without an Accept round. Neither command waits for the other because no dependency edge connects them.",
			"Parallel coordination increases throughput when the workload contains disjoint logical resources.",
			"Fast commit requires matching sequence and dependency attributes plus committed-prefix evidence; response count alone is insufficient.",
			"Find one eligible matching fast quorum.", "Verify dependency-prefix evidence from that quorum.", "Persist COMMITTED, broadcast Commit, then execute when dependencies are discharged.")
	case "fast-path:":
		return lesson("02 · PREACCEPT", "PreAccept discovers order",
			"PreAccept asks each voter to compare the proposed footprint with its local conflict index and return the sequence and dependency vector it would persist.",
			"The first phase is both a vote and distributed conflict discovery. Replies can confirm the owner's attributes or add information the owner did not see.",
			"A receiver records the command and computed attributes durably before its reply can count.",
			"Validate configuration, ballot, command checksum, and canonical footprint.", "Compute local conflicts and merge them into dependencies.", "Persist PRE-ACCEPTED before replying.")
	case "fast-path:R1 owns this instance":
		return lesson("02 · PREACCEPT", "The command leader proposes one tuple",
			"R1 allocates its next instance, computes Seq=1 with an empty dependency vector, persists it, and sends that exact tuple to the other voters.",
			"A command leader is replaceable during recovery. The durable tuple and ballot—not process identity—carry safety across failure.",
			"Retries preserve the same instance identity, ballot, timing domain, and initial tuple.",
			"Allocate the owner-local instance number.", "Seed the owner vote after durable persistence.", "Send PreAccept to the pinned voter set.")
	case "fast-path:A fast quorum matches":
		return lesson("02 · FAST-PATH PROOF", "Matching attributes are necessary, not sufficient alone",
			"R1 and R2 durably report the same Seq and dependency vector. This does not literally prove that no conflict exists anywhere: a replica outside the quorum may know more. Safety comes from the optimized fast-quorum intersection, exact attribute matching, and committed-prefix evidence for every final dependency slot.",
			"Any later recovery quorum intersects the fast decision in enough voters to recover the compatible value. Prefix evidence closes the implicit-dependency gap that a compact dependency vector would otherwise leave.",
			"The coordinator may fast-commit only when one eligible fast quorum matches the final tuple and collectively covers every dependency prefix required by that tuple.",
			"Group replies by exact ballot, Seq, and dependency vector.", "Select an eligible fast quorum using the configured quorum formula.", "For every dependency slot k, require a matching participant that durably covers committed instances through k.", "Reject fast commit when any marker, tuple, or prefix proof is missing.")
	case "fast-path:Accept is skipped":
		return lesson("02 · COMMIT", "Commit follows PreAccept directly",
			"The fast predicate is satisfied, so a second Accept vote cannot add safety. R1 persists COMMITTED and broadcasts the chosen command and attributes.",
			"Skipping Accept removes one coordinator-to-quorum round trip in the common case.",
			"Commit does not imply immediate execution: every true dependency must be committed and executable first.",
			"Persist the committed record.", "Send Commit to every voter.", "Feed the committed instance into the dependency executor.")
	case "conflict-cycle:":
		return lesson("03 · CONFLICTS", "Overlapping footprints create order",
			"Two concurrent writes touch the same cart key. Different replicas can observe them in different arrival orders, so the commands cannot safely remain unordered.",
			"EPaxos records ordering constraints as dependencies instead of forcing every command into one global log slot.",
			"Every non-commuting pair that is successfully committed must execute in the same relative order at every replica.",
			"Index both footprints.", "Return the latest conflicting instance per voter lane.", "Raise Seq above every dependency sequence.")
	case "conflict-cycle:Same key, different arrival order":
		return lesson("03 · CONFLICTS", "Concurrent coordinators start with partial knowledge",
			"R1 first knows cart=reserved; R2 first knows cart=paid. Each initial PreAccept is locally valid, but neither coordinator yet has the other's ordering fact.",
			"Asynchrony permits these views. The protocol must merge them without relying on message arrival order at a single leader.",
			"A local PRE-ACCEPTED record is tentative until the fast predicate or a later Accept/Commit decision succeeds.",
			"Persist both tentative instances.", "Exchange PreAccept across the conflicting coordinators.", "Recompute attributes against each receiver's local conflict index.")
	case "conflict-cycle:Attributes diverge":
		return lesson("03 · SLOW-PATH TRIGGER", "Different replies invalidate the fast proof",
			"Each reply adds the other instance as a dependency. The returned tuples no longer match, so the coordinator cannot identify one fast quorum that witnessed the same ordering attributes.",
			"Divergence is evidence that order discovery was incomplete at proposal time. Accept is required to choose one merged tuple durably.",
			"A coordinator must never infer fast-path safety from reply count after Seq or dependencies diverge.",
			"Union the returned dependency vectors lane by lane.", "Choose the maximum returned sequence.", "Persist ACCEPTED and start a majority Accept round.")
	case "conflict-cycle:Accept chooses merged attributes":
		return lesson("03 · ACCEPT", "A majority chooses the merged tuple",
			"Accept carries the command with its final sequence and dependency vector. A slow quorum records the same tuple under the current ballot before commit.",
			"Classic quorum intersection gives recovery a durable witness even though the one-round fast predicate failed.",
			"Only current-ballot Accept responses count; stale or duplicate responses cannot change the decision.",
			"Persist ACCEPTED locally.", "Broadcast Accept with the chosen tuple.", "Commit after a unique-sender slow quorum acknowledges the current ballot.")
	case "conflict-cycle:The cycle becomes an order":
		return lesson("03 · DEPENDENCY DAG", "Tarjan SCCs make cycles deterministic",
			"The committed dependency graph contains reciprocal edges. The executor collapses the cycle into one strongly connected component, waits for every true external dependency, then applies the component in the same total order everywhere.",
			"A graph cycle is not a deadlock. SCC condensation turns the graph into a DAG; deterministic ordering inside each SCC produces one replica-independent application sequence.",
			"Known conflicting PRE-ACCEPTED or ACCEPTED instances still block execution. Within a ready SCC, order is ascending (Seq, CycleKey, InstanceRef), with Seq dominant.",
			"Build vertices for committed, unexecuted instances and edges for dependency refs.", "Apply chain pruning only when a reciprocal higher-sequence edge makes an intermediate dependency redundant.", "Run Tarjan DFS: assign index and lowlink, push each vertex, and pop one SCC when lowlink equals index.", "Build the condensation DAG and reject components with unresolved external or tentative conflicting dependencies.", "Topologically release ready SCCs; sort each SCC by (Seq, CycleKey, InstanceRef).", "Emit Ready.Apply in that exact order. Complexity is O(V+E) before the per-SCC deterministic sort.")
	case "recovery:":
		return lesson("04 · RECOVERY", "Recovery is owner-independent",
			"A stalled instance may be recovered by another deterministic coordinator. Prepare collects durable status and value-ballot evidence from a majority.",
			"Progress does not depend on reviving the original command owner.",
			"Recovery must preserve any value that could already have been chosen and must never invent an application command.",
			"Raise a unique promise ballot.", "Collect Prepare responses from the instance's pinned configuration.", "Select a value by durable status and record ballot.")
	case "recovery:The owner disappears":
		return lesson("04 · FAILURE DETECTION", "Logical timers expose stalled work",
			"R1 stops after peers have durable PRE-ACCEPTED evidence. Healthy replicas retain that evidence even though replies cannot reach the owner.",
			"Failure detection changes liveness, not safety. Timeout only authorizes a higher-ballot recovery attempt.",
			"Paused or crashed nodes do not tick or process new Ready work; queued evidence remains explicit in the transport.",
			"Observe the missing progress.", "Advance deterministic recovery ticks.", "Elect the deterministic recovery coordinator for the blocked reference.")
	case "recovery:A dependency blocks execution":
		return lesson("04 · EXECUTION BARRIER", "Successors wait for unresolved predecessors",
			"The later order=confirmed command depends on the stalled order=created instance. It may commit, but applying it first would violate execution consistency.",
			"Commit and execution are separate. Dependency readiness protects state-machine order while recovery repairs missing protocol progress.",
			"A committed successor cannot execute until every true external dependency is executed or proven irrelevant by the protocol.",
			"Inspect external dependency refs.", "Schedule recovery for missing or uncommitted blockers.", "Keep the successor out of Ready.Apply.")
	case "recovery:R2 raises a recovery ballot":
		return lesson("04 · PREPARE", "Prepare selects durable evidence",
			"R2 broadcasts Prepare and compares response status, promise ballot, durable record ballot, command tuple, and fast-path markers.",
			"Promise ballots prevent lower-ballot work from racing the recovery decision. RecordBallot remains separate so Prepare cannot erase the ballot that accepted the value.",
			"Committed wins; otherwise choose the highest durable accepted tuple, then eligible matching pre-accepted evidence, then merged pre-accepted evidence, and only choose a no-op when the quorum reports no value.",
			"Deduplicate responses by sender.", "Prefer COMMITTED evidence immediately.", "Select highest RecordBallot ACCEPTED tuple without unioning lower-ballot values.", "Use TryPreAccept only when optimized recovery evidence permits it; otherwise finish through Accept.")
	case "recovery:The value survives its owner":
		return lesson("04 · RECOVERY COMMIT", "The quorum finishes without R1",
			"R2 and R3 recover the original value, discharge the dependency, and apply both commands before R1 returns. Commit propagation lets the restarted owner converge later.",
			"Durable quorum evidence makes the chosen value independent of any one process's volatile memory.",
			"Each application command is emitted once per replica in dependency order; the embedding still persists result deduplication because Ready may repeat across crash.",
			"Finish the selected value through TryPreAccept or Accept.", "Broadcast Commit.", "Execute the now-ready dependency DAG and retain durable command results.")
	case "optimization:":
		return lesson("05 · OPTIMIZATION", "Measure the base path before optimizing",
			"The final chapter uses a five-node financial K/V workload, actual RawNode transitions, per-link RTT, locality routing, and crash/restart controls.",
			"An optimization is useful only when its operational assumptions and safety boundary are visible beside a base control.",
			"The financial TRANSFER is one atomic command whose footprint includes debit, credit, transaction-result, and dedup resources.",
			"Bootstrap durable configuration and account state.", "Run base EPaxos with RTT-delayed transport.", "Compare TOQ and dependency-chain pruning without claiming they are enabled in the base trace.")
	case "optimization:Bootstrap durable state":
		return lesson("05 · BOOTSTRAP", "Durability precedes traffic",
			"Each replica starts offline, opens durable storage, constructs RawNode, persists initial HardState, and installs the same account snapshot before accepting client work.",
			"A node must know its replica identity, voter configuration, and application snapshot before protocol messages can be interpreted safely.",
			"No transfer is routable until at least one home replica is bootstrapped; consensus progress still requires a slow quorum of three.",
			"Open and validate durable protocol storage.", "Construct RawNode with voters R1–R5.", "Persist initial Ready atomically.", "Install the account snapshot and expose the node as RUNNING.")
	case "optimization:Five locality replicas are online":
		return lesson("05 · LOCALITY", "Adjacent home pairs balance client routing",
			"Accounts are assigned around a ring: each account has two adjacent home replicas. The router chooses the least-loaded running member of that pair, with deterministic home order as the tie-breaker.",
			"Across the five-account workload, preferred homes cover R1–R5 evenly. Locality affects only the client coordinator; EPaxos still replicates protocol evidence to the full voter configuration.",
			"Never restrict consensus voters to the account's home pair: two replicas cannot form the three-of-five quorum required for this configuration.",
			"Hash or map the source account to an adjacent home pair.", "Filter offline, paused, and crashed homes.", "Choose minimum coordinated count, then stable home order.", "Send PreAccept to the pinned five-voter configuration.")
	case "optimization:Locality routes the debit":
		return lesson("05 · ATOMIC TRANSFER", "Debit and credit share one command",
			"The router sends Northwind's transfer to a local home replica. One command debits Northwind and credits Contoso; it is never split into two SET operations.",
			"A single replicated command lets every replica validate funds and update both balances at one deterministic position. Concurrent withdrawals overlap on the debit resource and therefore enter the dependency graph.",
			"The footprint includes acct/northwind, acct/contoso, a transaction result key, and a command-specific dedup key. Declared non-overlap must imply strong commutativity of state, response, and side effects.",
			"Validate account IDs and positive cents.", "Build one canonical multi-point footprint.", "Persist the full response or durable result handle with CommandID dedup state.", "On apply, decline insufficient funds or update both balances atomically.")
	case "optimization:RTT shapes the fast quorum":
		return lesson("05 · RTT", "Fast quorum latency follows the fastest matching paths",
			"Every packet receives a one-way availability time of ceil(RTT/2). R1 cannot consume a response before both request and response delays have elapsed; the first compatible three-voter evidence determines the fast-path latency.",
			"Flexible quorum composition lets a coordinator avoid a slow or crashed replica while a majority remains available. Link RTT affects latency but never quorum arithmetic.",
			"A packet is deliverable only after its simulated deadline, on a healthy link, to a running recipient. Manual delay is additional to the configured RTT.",
			"Stamp outbound packet at networkNow + ceil(linkRTT/2).", "Advance the deterministic network clock.", "Deliver ready packets while retaining in-flight and blocked envelopes.", "Observe which matching responses complete the fast quorum.")
	case "optimization:TOQ moves work to ProcessAt":
		return lesson("05 · TOQ OPTIMIZATION", "TOQ delays dependency assignment with explicit clock bounds",
			"The visible transaction used base EPaxos. In TOQ mode, the embedding samples now and supplies conservative one-way delay plus clock-skew bounds. The origin sends TOQ PreAccept with Seq=0 and empty dependencies, and each participant delays final dependency assignment until the computed ProcessAt time.",
			"If synchronized receivers process commands in timestamp order, fewer concurrent commands acquire inconsistent dependency attributes, increasing fast-path probability. The core does not synchronize clocks, measure delay, or quarantine drift; those are production embedding responsibilities.",
			"A TOQ command cannot commit while its local TOQPending assignment remains durable. At ProcessAt, the node computes ordinary attributes, persists PRE-ACCEPTED, and only then evaluates fast or slow completion.",
			"Measure a conservative one-way bound for every sync-group peer and add maximum clock uncertainty.", "Sample now outside the deterministic core; compute ProcessAt from the configured bound.", "Send TOQ PreAccept with Seq=0, empty deps, and the stable timing envelope on retries.", "At ProcessAt, order due entries by timestamp, instance ref, then sender; compute and persist dependencies.", "Evaluate the optimized matching quorum with committed-prefix evidence.", "Apply chain pruning before SCC execution when reciprocal higher-sequence structure makes an intermediate edge redundant.")
	default:
		return lesson("LEARNING EPAXOS", "Follow the durable state transition",
			"Use the frame controls to inspect the packet, persisted record, dependency edges, and application state produced by each action.",
			"Every visual state is generated from the real deterministic core rather than a hand-authored protocol outcome.",
			"Safety claims depend on durable records, exact quorum predicates, and deterministic dependency execution.",
			"Read the action.", "Inspect Ready persistence and outbound messages.", "Check the post-action local views before moving forward.")
	}
}

func lesson(phase, title, summary, why, invariant string, algorithm ...string) LearningView {
	return LearningView{Phase: phase, Title: title, Summary: summary, Why: why, Invariant: invariant, Algorithm: algorithm}
}
