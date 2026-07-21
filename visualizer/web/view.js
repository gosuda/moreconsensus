const SVG_NS = "http://www.w3.org/2000/svg";
const currencyFormatter = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });

const dom = {
  landing: document.querySelector("#landing"),
  workspace: document.querySelector("#workspace"),
  coreStatus: document.querySelector("#core-status"),
  share: document.querySelector("#share-button"),
  inspectorToggle: document.querySelector("#inspector-toggle"),
  modeLabel: document.querySelector("#mode-label"),
  title: document.querySelector("#workspace-title"),
  quorum: document.querySelector("#quorum-label"),
  frameCounter: document.querySelector("#frame-counter"),
  leftRail: document.querySelector("#left-rail"),
  stageFocus: document.querySelector("#stage-focus"),
  topology: document.querySelector("#topology"),
  network: document.querySelector("#network-svg"),
  replicas: document.querySelector("#replica-layer"),
  packets: document.querySelector("#packet-layer"),
  queue: document.querySelector("#message-queue"),
  queueCount: document.querySelector("#queue-count"),
  blockedHint: document.querySelector("#blocked-hint"),
  inspector: document.querySelector("#inspector"),
  inspectorTitle: document.querySelector("#inspector-title"),
  inspectorContent: document.querySelector("#inspector-content"),
  tabs: document.querySelector(".tabs"),
  playback: document.querySelector("#playback"),
};

const positions = {
  3: [[50, 18], [22, 73], [78, 73]],
  5: [[50, 13], [18, 36], [29, 78], [71, 78], [82, 36]],
};

const resizeObserver = new ResizeObserver(() => drawNetwork());
resizeObserver.observe(dom.topology);

export function setCoreReady() {
  dom.coreStatus.textContent = "CORE READY";
  dom.coreStatus.classList.add("ready");
  document.querySelector("#tour-cta").disabled = false;
  document.querySelector("#finance-cta").disabled = false;
  document.querySelector("#lab-cta").disabled = false;
}

export function render(state) {
  const inWorkspace = state.mode === "tour" || state.mode === "lab" || state.mode === "finance";
  dom.landing.hidden = inWorkspace;
  dom.workspace.hidden = !inWorkspace;
  dom.share.hidden = !inWorkspace;
  dom.inspectorToggle.hidden = !inWorkspace;
  dom.inspectorToggle.setAttribute("aria-expanded", String(state.inspectorOpen));
  dom.inspector.classList.toggle("open", state.inspectorOpen);
  if (!inWorkspace || !state.frame) {
    return;
  }

  const snapshot = state.frame.snapshot;
  const selectedReplica = snapshot.replicas.find((replica) => replica.id === state.selectedReplica) || snapshot.replicas[0];
  dom.modeLabel.textContent = state.mode === "tour" ? "LEARNING EPAXOS" : state.mode === "finance" ? "FINANCIAL K/V SYSTEM" : "PROTOCOL LAB";
  dom.title.textContent = state.mode === "tour" ? state.trace.scenario.title : state.mode === "finance" ? "Five-node transaction network" : `${snapshot.cluster.size}-replica lab`;
  dom.quorum.textContent = `FAST ${snapshot.cluster.fastQuorum} · SLOW ${snapshot.cluster.slowQuorum}`;
  dom.frameCounter.textContent = `FRAME ${state.frame.index}`;
  dom.stageFocus.textContent = `R${selectedReplica.id} SELECTED`;
  dom.inspectorTitle.textContent = `Replica ${selectedReplica.id} · local view`;

  renderLeftRail(state, selectedReplica);
  renderTopology(state, snapshot, selectedReplica);
  renderTransport(state, snapshot);
  renderInspector(state, selectedReplica);
  renderPlayback(state, snapshot);
}

function renderLeftRail(state, selectedReplica) {
  const content = document.createDocumentFragment();
  if (state.mode === "tour") {
    content.append(renderLearning(state));
  } else if (state.mode === "finance") {
    content.append(kicker("FINANCIAL K/V WORKLOAD"));
    content.append(el("h2", "", "Atomic account transfer"));
    content.append(el("p", "rail-copy", "The source account selects an adjacent home pair. The least-loaded running home coordinates one atomic debit-and-credit command."));
    content.append(renderFinanceForm(state));
    content.append(renderThroughputChart(state.throughput));
  } else {
    content.append(kicker("LAB INPUT"));
    content.append(el("h2", "", "Send one point command"));
    content.append(el("p", "rail-copy", "Choose any running coordinator. Messages move only when you deliver them or start Run."));
    content.append(renderLabForm(state, selectedReplica));
  }

  if (state.error) {
    const error = el("div", "error-banner", state.error);
    error.setAttribute("role", "alert");
    content.append(error);
  }
  dom.leftRail.replaceChildren(content);
}

function renderLearning(state) {
  const content = el("div", "learning-content");
  const learning = state.frame.learning;
  content.append(kicker(learning?.phase || "LEARNING EPAXOS"));
  content.append(el("h2", "", learning?.title || state.trace.scenario.title));
  content.append(el("p", "learning-summary", learning?.summary || state.trace.scenario.lede));

  if (state.frame.headline) {
    const current = el("div", "current-transition");
    current.append(kicker(`FRAME ${state.frame.index} · CURRENT TRANSITION`), el("strong", "", state.frame.headline), el("p", "", state.frame.explanation));
    content.append(current);
  }
  if (learning) {
    const why = el("section", "learning-section");
    why.append(el("h3", "", "WHY THIS IS SAFE"), el("p", "", learning.why));
    const invariant = el("section", "learning-section invariant");
    invariant.append(el("h3", "", "INVARIANT"), el("p", "", learning.invariant));
    const algorithm = el("section", "learning-section");
    algorithm.append(el("h3", "", "ALGORITHM · STEP BY STEP"));
    const steps = el("ol", "algorithm-steps");
    learning.algorithm.forEach((step) => steps.append(el("li", "", step)));
    algorithm.append(steps);
    content.append(why, invariant, algorithm);
  }
  if (state.trace.scenario.id === "optimization") {
    content.append(renderThroughputChart(state.throughput));
  }

  const chapters = el("nav", "chapter-list");
  chapters.setAttribute("aria-label", "Learning EPaxos chapters");
  state.catalog.forEach((scenario, index) => {
    const chapter = actionButton("", "open-scenario", "chapter-button");
    chapter.dataset.scenario = scenario.id;
    chapter.classList.toggle("active", scenario.id === state.trace.scenario.id);
    chapter.append(el("span", "chapter-number", String(index + 1).padStart(2, "0")), el("span", "", scenario.title));
    chapter.setAttribute("aria-current", scenario.id === state.trace.scenario.id ? "step" : "false");
    chapters.append(chapter);
  });
  content.append(chapters);

  if (state.frameIndex === state.trace.frames.length - 1) {
    content.append(el("div", "completion-card", state.trace.scenario.completion));
    const next = nextScenario(state);
    const action = next ? actionButton("Next chapter", "next-chapter", "button primary") : actionButton("Open financial system", "open-finance", "button primary");
    action.dataset.scenario = next?.id || "";
    content.append(action);
  }
  return content;
}

function renderFinanceForm(state) {
  const snapshot = state.frame.snapshot;
  const form = el("form", "lab-form finance-form");
  form.dataset.role = "finance-form";
  const allBooted = snapshot.replicas.every((replica) => replica.booted);
  if (!allBooted) {
    const bootstrap = el("div", "bootstrap-panel");
    bootstrap.append(el("strong", "", "CLUSTER BOOTSTRAP REQUIRED"), el("p", "", "Bring each replica online, persist configuration, and install the opening account snapshot."));
    const bootAll = actionButton("Bootstrap all five nodes", "bootstrap-all", "button primary");
    bootAll.disabled = state.busy;
    bootstrap.append(bootAll);
    form.append(bootstrap);
  }

  const from = accountField("from", "Debit account", state.financeDraft.from, snapshot.accounts, state.busy || !allBooted);
  const to = accountField("to", "Credit account", state.financeDraft.to, snapshot.accounts, state.busy || !allBooted);
  const amount = textField("amount", "Amount · USD", state.financeDraft.amount, 10, state.busy || !allBooted);
  amount.querySelector("input").inputMode = "decimal";
  const selected = snapshot.accounts.find((account) => account.id === state.financeDraft.from);
  const route = el("div", "route-preview");
  route.append(kicker("LOCALITY ROUTER"), el("strong", "", selected ? `${selected.name} → R${selected.home[0]} / R${selected.home[1]}` : "Choose an account"), el("p", "", "Least coordinated running home wins. Consensus messages still use all five voters."));
  const submit = actionButton("Route atomic transfer", "transfer", "button primary");
  submit.type = "submit";
  submit.disabled = state.busy || !allBooted;
  form.append(from, to, amount, route, submit);
  return form;
}

function accountField(name, labelText, value, accounts, disabled) {
  const field = el("div", "field");
  const label = el("label", "", labelText);
  label.htmlFor = `finance-${name}`;
  const select = el("select");
  select.id = `finance-${name}`;
  select.dataset.field = name;
  for (const account of accounts) {
    const option = el("option", "", `${account.name} · R${account.home[0]}/R${account.home[1]}`);
    option.value = account.id;
    option.selected = account.id === value;
    select.append(option);
  }
  select.disabled = disabled;
  field.append(label, select);
  return field;
}

function renderThroughputChart(points) {
  const figure = el("figure", "throughput-chart");
  const caption = el("figcaption");
  caption.append(kicker("ACTUAL CORE LOAD TRIAL"), el("strong", "", "Graceful degradation"), el("span", "", "Completed transfers in 120 deterministic RTT rounds."));
  figure.append(caption);
  const bars = el("div", "throughput-bars");
  for (const point of points || []) {
    const row = el("div", "throughput-row");
    const label = el("span", "", `${point.faults} fault${point.faults === 1 ? "" : "s"}`);
    const track = el("div", "throughput-track");
    const bar = el("div", "throughput-bar");
    bar.style.width = `${point.normalized}%`;
    track.append(bar);
    row.append(label, track, el("strong", "", `${point.normalized}% · ${point.committed}`));
    bars.append(row);
  }
  figure.append(bars, el("p", "chart-note", "Five independent account streams use the real core and link scheduler. At three faults, two voters cannot form the 3-of-5 quorum."));
  return figure;
}

function renderLabForm(state, selectedReplica) {
  const form = el("form", "lab-form");
  form.dataset.role = "lab-form";

  const sizeField = el("div", "field");
  sizeField.append(el("label", "", "Cluster reset"));
  const sizes = el("div", "segmented");
  for (const size of [3, 5]) {
    const control = actionButton(`${size} replicas`, "set-size", `button ${state.frame.snapshot.cluster.size === size ? "active" : ""}`);
    control.dataset.size = String(size);
    control.disabled = state.busy;
    sizes.append(control);
  }
  sizeField.append(sizes);

  const coordinatorField = el("div", "field");
  const coordinatorLabel = el("label", "", "Coordinator");
  coordinatorLabel.htmlFor = "coordinator";
  const coordinator = el("select");
  coordinator.id = "coordinator";
  coordinator.dataset.field = "coordinator";
  for (const replica of state.frame.snapshot.replicas) {
    const option = el("option", "", `R${replica.id}${replica.paused ? " · PAUSED" : ""}`);
    option.value = String(replica.id);
    option.selected = Number(state.labDraft.coordinator) === replica.id;
    option.disabled = replica.paused;
    coordinator.append(option);
  }
  coordinator.disabled = state.busy;
  coordinatorField.append(coordinatorLabel, coordinator);

  const keyField = textField("key", "Key", state.labDraft.key, 16, state.busy);
  const valueField = textField("value", "Value", state.labDraft.value, 16, state.busy);
  const propose = actionButton("Propose", "propose", "button primary");
  propose.type = "submit";
  propose.disabled = state.busy || selectedReplica.paused;
  propose.title = "Propose SET through the selected coordinator";

  form.append(sizeField, coordinatorField, keyField, valueField, propose);
  return form;
}

function textField(name, labelText, value, maxLength, disabled) {
  const field = el("div", "field");
  const label = el("label", "", labelText);
  label.htmlFor = `lab-${name}`;
  const input = el("input");
  input.id = `lab-${name}`;
  input.name = name;
  input.dataset.field = name;
  input.value = value;
  input.maxLength = maxLength;
  input.autocomplete = "off";
  input.spellcheck = false;
  input.disabled = disabled;
  field.append(label, input);
  return field;
}

function renderTopology(state, snapshot, selectedReplica) {
  const fragment = document.createDocumentFragment();
  const layout = positions[snapshot.cluster.size];
  snapshot.replicas.forEach((replica, index) => {
    const statusText = replicaStatus(replica);
    const card = el("div", "replica-card");
    card.dataset.replica = String(replica.id);
    card.dataset.action = "select-replica";
    card.tabIndex = 0;
    card.setAttribute("role", "button");
    card.setAttribute("aria-pressed", String(replica.id === selectedReplica.id));
    card.setAttribute("aria-label", `Select replica ${replica.id}, ${statusText.toLowerCase()}`);
    card.style.left = `${layout[index][0]}%`;
    card.style.top = `${layout[index][1]}%`;
    card.classList.toggle("selected", replica.id === selectedReplica.id);
    card.classList.toggle("focused", state.mode === "tour" && state.frame.focus?.replica === replica.id);
    card.classList.toggle("paused", replica.paused);
    card.classList.toggle("crashed", replica.crashed);
    card.classList.toggle("offline", !replica.booted);
    const latest = replica.instances.at(-1);
    card.dataset.latestStatus = latest ? `${latest.ref} · ${latest.status}` : "NO INSTANCES";

    const header = el("div", "replica-card-header");
    const identity = el("div", "replica-identity");
    identity.append(el("span", "replica-id", `R${replica.id}`));
    if (replica.region) {
      identity.append(el("span", "replica-region", replica.region));
    }
    const status = el("span", `replica-status ${statusText.toLowerCase()}`, statusText);
    header.append(identity, status);
    card.append(header);
    const telemetry = el("div", "replica-tick", `LOGICAL ${replica.tick}${snapshot.cluster.financial ? ` · COORD ${replica.coordinated}` : ""}`);
    card.append(telemetry);

    const instances = el("div", "instance-list");
    for (const instance of replica.instances.slice(-4)) {
      const row = el("div", "instance-summary");
      row.dataset.ref = instance.ref;
      row.append(el("span", "", instance.ref), el("span", `status ${instance.status}`, instance.status.toUpperCase()));
      instances.append(row);
    }
    if (replica.instances.length === 0) {
      instances.append(el("div", "instance-summary", replica.booted ? "NO LOCAL INSTANCES" : "AWAITING BOOTSTRAP"));
    }
    card.append(instances);

    const stateSummary = el("div", "kv-summary");
    if (snapshot.cluster.financial) {
      stateSummary.append(el("span", "", replica.booted ? `${replica.state.length} ACCOUNT KEYS` : "STATE OFFLINE"));
    } else {
      const latestState = replica.state.at(-1);
      stateSummary.append(el("span", "", latestState ? `${latestState.key}=${latestState.value}` : "STATE ∅"));
    }
    const nodeControl = nodeAction(state, replica);
    if (nodeControl) {
      stateSummary.append(nodeControl);
    }
    card.append(stateSummary);
    fragment.append(card);
  });
  dom.replicas.replaceChildren(fragment);

  const lines = document.createDocumentFragment();
  for (const link of snapshot.links) {
    const line = document.createElementNS(SVG_NS, "line");
    line.classList.add("network-line");
    line.classList.toggle("delayed", link.delayed);
    line.dataset.from = String(link.from);
    line.dataset.to = String(link.to);
    line.dataset.rtt = String(link.rttMs);
    lines.append(line);
  }
  dom.network.replaceChildren(lines);
  requestAnimationFrame(drawNetwork);
}

function replicaStatus(replica) {
  if (!replica.booted) {
    return "OFFLINE";
  }
  if (replica.crashed) {
    return "CRASHED";
  }
  if (replica.paused) {
    return "PAUSED";
  }
  return "RUNNING";
}

function nodeAction(state, replica) {
  if (state.mode === "finance") {
    let label = "Hammer crash";
    let action = "crash-node";
    let className = "button danger hammer-button";
    if (!replica.booted) {
      label = "Boot";
      action = "bootstrap-node";
      className = "button";
    } else if (replica.crashed) {
      label = "Restart";
      action = "restart-node";
      className = "button";
    }
    const control = actionButton(label, action, className);
    control.dataset.replica = String(replica.id);
    control.disabled = state.busy;
    control.title = replica.crashed ? `Restart R${replica.id} from durable storage` : !replica.booted ? `Bootstrap R${replica.id}` : `Crash R${replica.id} with the failure hammer`;
    return control;
  }
  if (state.mode === "lab") {
    const control = actionButton(replica.paused ? "Resume" : "Pause", replica.paused ? "resume-node" : "pause-node", `button ${replica.paused ? "" : "danger"}`);
    control.dataset.replica = String(replica.id);
    control.disabled = state.busy;
    control.title = replica.paused ? `Resume R${replica.id}` : `Pause R${replica.id}`;
    return control;
  }
  return null;
}

function drawNetwork() {
  const topologyRect = dom.topology.getBoundingClientRect();
  if (topologyRect.width === 0 || topologyRect.height === 0) {
    return;
  }
  dom.network.setAttribute("viewBox", `0 0 ${topologyRect.width} ${topologyRect.height}`);
  for (const line of dom.network.querySelectorAll("line")) {
    const from = replicaElement(line.dataset.from);
    const to = replicaElement(line.dataset.to);
    if (!from || !to) {
      continue;
    }
    const fromRect = from.getBoundingClientRect();
    const toRect = to.getBoundingClientRect();
    line.setAttribute("x1", String(fromRect.left + fromRect.width / 2 - topologyRect.left));
    line.setAttribute("y1", String(fromRect.top + fromRect.height / 2 - topologyRect.top));
    line.setAttribute("x2", String(toRect.left + toRect.width / 2 - topologyRect.left));
    line.setAttribute("y2", String(toRect.top + toRect.height / 2 - topologyRect.top));
  }
}

function renderTransport(state, snapshot) {
  const clock = snapshot.cluster.financial ? ` · T+${snapshot.cluster.networkMs}MS` : "";
  dom.queueCount.textContent = `${snapshot.messages.length} ${snapshot.messages.length === 1 ? "ENVELOPE" : "ENVELOPES"}${clock}`;
  const allBlocked = snapshot.messages.length > 0 && snapshot.messages.every((message) => message.blocked);
  dom.blockedHint.hidden = !allBlocked;
  if (allBlocked && snapshot.cluster.financial && snapshot.messages.some((message) => message.remainingMs > 0)) {
    dom.blockedHint.textContent = "Packets are in flight. Advance simulated RTT time, or press Run to follow the next deadline.";
  } else {
    dom.blockedHint.textContent = "No message can move. Heal a link, resume or restart a replica, drop a packet, or tick recovery.";
  }
  const fragment = document.createDocumentFragment();
  if (snapshot.messages.length === 0) {
    fragment.append(el("div", "empty-state", "NETWORK QUIET · NO QUEUED ENVELOPES"));
  }
  for (const message of snapshot.messages) {
    const row = el("div", `message-row ${message.blocked ? "blocked" : ""}`);
    row.dataset.envelope = message.id;
    const timing = message.rttMs > 0 ? `RTT ${message.rttMs}MS${message.remainingMs > 0 ? ` · ${message.remainingMs}MS LEFT` : " · READY"}` : "";
    row.append(
      el("span", "", `#${message.id}`),
      el("span", `message-type ${message.type}`, message.type.toUpperCase()),
      el("span", "", `R${message.from} → R${message.to}`),
      el("span", "message-ref", message.ref),
      el("span", "message-deps", message.deps.length ? `deps ${message.deps.join(", ")}` : "deps ∅"),
      el("span", "message-timing", timing),
    );
    if (state.mode === "lab" || state.mode === "finance") {
      const actions = el("div", "message-actions");
      const deliver = actionButton("Deliver", "deliver-envelope", "button");
      deliver.dataset.envelope = message.id;
      deliver.disabled = state.busy || message.blocked;
      deliver.title = `Deliver envelope ${message.id}`;
      const drop = actionButton("Drop", "drop-envelope", "button danger");
      drop.dataset.envelope = message.id;
      drop.disabled = state.busy;
      drop.title = `Drop envelope ${message.id}`;
      actions.append(deliver, drop);
      row.append(actions);
    }
    fragment.append(row);
  }
  dom.queue.replaceChildren(fragment);
}

function renderInspector(state, replica) {
  for (const tab of dom.tabs.querySelectorAll("button")) {
    tab.setAttribute("aria-selected", String(tab.dataset.tab === state.inspectorTab));
  }
  if (state.inspectorTab === "NETWORK") {
    renderNetworkInspector(state, replica);
  } else if (state.inspectorTab === "EVENT LOG") {
    renderEventLog(state);
  } else {
    renderInstanceInspector(state, replica);
  }
}

function renderInstanceInspector(state, replica) {
  const fragment = document.createDocumentFragment();
  if (replica.instances.length === 0) {
    const reason = !replica.booted ? `Replica ${replica.id} is offline. Bootstrap it to persist the initial configuration.` :
      replica.crashed ? `Replica ${replica.id} is crashed. Restart it from durable storage.` :
        `Replica ${replica.id} has no local instances yet.`;
    fragment.append(el("div", "empty-state", reason));
    fragment.append(renderApplication(replica));
    dom.inspectorContent.replaceChildren(fragment);
    return;
  }

  let selected = replica.instances.find((instance) => instance.ref === state.selectedInstanceRef);
  if (!selected && state.frame.focus?.ref) {
    selected = replica.instances.find((instance) => instance.ref === state.frame.focus.ref);
  }
  selected ||= replica.instances.at(-1);

  const picker = el("div", "instance-picker");
  for (const instance of replica.instances.slice(-8)) {
    const control = actionButton("", "select-instance", instance.ref === selected.ref ? "active" : "");
    control.dataset.ref = instance.ref;
    control.append(el("span", "", instance.ref), el("span", "instance-status", instance.status.toUpperCase()));
    picker.append(control);
  }
  fragment.append(picker, dependencyGraph(replica, selected));

  const details = el("dl", "detail-grid");
  detail(details, "Ref", selected.ref);
  detail(details, "Command", selected.command?.summary || "protocol-only");
  if (selected.command?.resources?.length) {
    detail(details, "Footprint", selected.command.resources.join(" · "));
  }
  detail(details, "Status", selected.status.toUpperCase());
  detail(details, "Seq", String(selected.seq));
  detail(details, "Deps", selected.depVector.length ? `[${selected.depVector.join(", ")}]` : "[]");
  detail(details, "Ballot", `{${selected.ballot.epoch}, ${selected.ballot.number}, ${selected.ballot.replica}}`);
  detail(details, "Cycle order", selected.order ? `ORDER ${selected.order}` : "—");
  const path = el("span", `path-badge ${selected.path.toLowerCase()}`, selected.path);
  const pathTerm = el("dt", "", "Path");
  const pathValue = el("dd");
  pathValue.append(path);
  details.append(pathTerm, pathValue);
  fragment.append(details, renderApplication(replica));
  dom.inspectorContent.replaceChildren(fragment);
}

function dependencyGraph(replica, instance) {
  const svg = document.createElementNS(SVG_NS, "svg");
  svg.classList.add("dependency-graph");
  svg.setAttribute("viewBox", "0 0 320 126");
  svg.setAttribute("role", "img");
  svg.setAttribute("aria-label", `${instance.ref} dependencies: ${instance.edges.length ? instance.edges.join(", ") : "none"}`);

  const rootX = 75;
  const rootY = 63;
  instance.edges.forEach((edge, index) => {
    const edgeX = 245;
    const edgeY = 24 + index * (78 / Math.max(1, instance.edges.length - 1));
    const known = replica.instances.some((candidate) => candidate.ref === edge);
    const line = document.createElementNS(SVG_NS, "line");
    line.setAttribute("x1", String(rootX + 27));
    line.setAttribute("y1", String(rootY));
    line.setAttribute("x2", String(edgeX - 27));
    line.setAttribute("y2", String(edgeY));
    line.classList.add("dependency-edge");
    line.classList.toggle("unknown", !known);
    svg.append(line, dependencyNode(edgeX, edgeY, edge, known));
  });
  svg.append(dependencyNode(rootX, rootY, instance.ref, true));
  if (instance.edges.length === 0) {
    const empty = document.createElementNS(SVG_NS, "text");
    empty.setAttribute("x", "225");
    empty.setAttribute("y", "67");
    empty.classList.add("dependency-label");
    empty.textContent = "NO DEPENDENCIES";
    svg.append(empty);
  }
  return svg;
}

function dependencyNode(x, y, label, known) {
  const group = document.createElementNS(SVG_NS, "g");
  const circle = document.createElementNS(SVG_NS, "circle");
  circle.setAttribute("cx", String(x));
  circle.setAttribute("cy", String(y));
  circle.setAttribute("r", "28");
  circle.classList.add("dependency-node");
  circle.classList.toggle("unknown", !known);
  const text = document.createElementNS(SVG_NS, "text");
  text.setAttribute("x", String(x));
  text.setAttribute("y", String(y + 3));
  text.classList.add("dependency-label");
  text.textContent = label;
  group.append(circle, text);
  return group;
}

function renderApplication(replica) {
  const panel = el("section", "application-panel");
  panel.append(el("h3", "", "APPLICATION STATE"));
  if (replica.state.length === 0) {
    panel.append(el("div", "empty-state", replica.booted ? "STATE ∅" : "STATE OFFLINE"));
  }
  for (const item of replica.state) {
    const row = el("div", "state-row");
    const financial = item.key.startsWith("acct_");
    const label = financial ? item.key.slice(5).replaceAll("_", " ").toUpperCase() : item.key;
    const value = financial ? formatCents(Number(item.value)) : item.value;
    row.append(el("span", "", label), el("strong", "", value));
    panel.append(row);
  }
  panel.append(el("h3", "", "APPLY ORDER"));
  if (replica.applied.length === 0) {
    panel.append(el("div", "empty-state", "NOTHING APPLIED"));
  }
  replica.applied.forEach((applied, index) => {
    const row = el("div", "applied-row");
    row.append(el("span", "", String(index + 1).padStart(2, "0")), el("span", "", `${applied.ref} · ${applied.summary}`));
    panel.append(row);
  });
  return panel;
}

function renderNetworkInspector(state, replica) {
  const panel = el("section", "network-panel");
  panel.append(el("h3", "", `LINKS FROM R${replica.id}`));
  if (replica.region) {
    panel.append(el("p", "network-location", replica.region));
  }
  const links = state.frame.snapshot.links.filter((link) => link.from === replica.id || link.to === replica.id);
  for (const link of links) {
    const row = el("div", `network-link-row ${link.delayed ? "delayed" : ""}`);
    const stateText = link.delayed ? "DELAYED" : "HEALTHY";
    row.append(el("span", "", `R${link.from} ↔ R${link.to}`), el("span", "link-rtt", link.rttMs ? `RTT ${link.rttMs}MS · ${stateText}` : stateText));
    if (state.mode === "lab" || state.mode === "finance") {
      const control = actionButton(link.delayed ? "Heal" : "Delay", link.delayed ? "heal-link" : "delay-link", `button ${link.delayed ? "" : "danger"}`);
      control.dataset.from = String(link.from);
      control.dataset.to = String(link.to);
      control.disabled = state.busy;
      row.append(control);
    }
    panel.append(row);
  }
  dom.inspectorContent.replaceChildren(panel);
}

function renderEventLog(state) {
  const panel = el("section", "event-panel");
  panel.append(el("h3", "", "TEXT EVENT LOG"));
  const events = state.eventLog.flatMap((entry) => entry.events.map((event) => ({ index: entry.index, event })));
  if (events.length === 0) {
    panel.append(el("div", "empty-state", "FRAME 0 · NO EVENTS"));
  }
  for (const item of events) {
    const row = el("div", "event-row");
    row.append(el("span", `event-kind ${item.event.kind}`, `${item.index} · ${item.event.kind}`), el("span", "", item.event.detail));
    panel.append(row);
  }
  dom.inspectorContent.replaceChildren(panel);
  panel.scrollTop = panel.scrollHeight;
}

function renderPlayback(state, snapshot) {
  const left = el("div", "playback-group");
  const right = el("div", "speed-controls");
  if (state.mode === "tour") {
    left.append(
      playbackButton("Back", "back", state.busy || state.frameIndex === 0, "B"),
      playbackButton(state.tourPlaying ? "Pause" : "Play", "toggle-play", !state.tourPlaying && state.busy, "Space"),
      playbackButton("Next", "next", state.busy || state.frameIndex >= state.trace.frames.length - 1, "N"),
      playbackButton("Reset", "reset-tour", state.busy || state.frameIndex === 0, "R"),
    );
  } else {
    const deliverable = snapshot.messages.some((message) => !message.blocked);
    const wait = nextRTTWait(snapshot);
    const runnable = deliverable || (state.mode === "finance" && wait > 0);
    const allUnavailable = snapshot.replicas.every((replica) => !replica.booted || replica.paused || replica.crashed);
    left.append(
      playbackButton("Back", "lab-back", state.busy || !state.canBack, "B"),
      playbackButton("Forward", "lab-forward", state.busy || !state.canForward, "N"),
      playbackButton("Deliver next", "deliver-next", state.busy || !deliverable, ""),
    );
    if (state.mode === "finance") {
      const advance = playbackButton(wait > 0 ? `Advance ${wait}ms` : "Advance RTT", "advance-network", state.busy || wait === 0, "");
      advance.dataset.milliseconds = String(wait);
      left.append(advance);
    }
    left.append(
      playbackButton(state.labRunning ? "Pause" : "Run", "toggle-run", !state.labRunning && (state.busy || !runnable), "Space"),
      playbackButton("Tick", "tick", state.busy || allUnavailable, ""),
      playbackButton("Reset", "reset-lab", state.busy, "R"),
    );
  }
  for (const speed of [0.5, 1, 2]) {
    const control = actionButton(`${speed}×`, "set-speed", `button ${state.speed === speed ? "active" : ""}`);
    control.dataset.speed = String(speed);
    control.disabled = state.busy;
    right.append(control);
  }
  const sessionStatus = state.mode === "finance" ? `T+${snapshot.cluster.networkMs}MS · HISTORY ${state.frame.index}` : `History ${state.frame.index}`;
  const status = el("div", "playback-status", state.mode === "tour" ? `${state.frameIndex + 1} / ${state.trace.frames.length}` : sessionStatus);
  dom.playback.replaceChildren(left, status, right);
}

function nextRTTWait(snapshot) {
  let wait = 0;
  for (const message of snapshot.messages) {
    if (message.remainingMs > 0 && (wait === 0 || message.remainingMs < wait)) {
      wait = message.remainingMs;
    }
  }
  return wait;
}

function formatCents(cents) {
  return currencyFormatter.format(cents / 100);
}

function playbackButton(label, action, disabled, shortcut) {
  const control = actionButton(label, action, "button");
  control.disabled = disabled;
  control.title = shortcut ? `${label} (${shortcut})` : label;
  return control;
}

function nextScenario(state) {
  const index = state.catalog.findIndex((scenario) => scenario.id === state.trace.scenario.id);
  return state.catalog[index + 1] || null;
}

function detail(list, term, value) {
  list.append(el("dt", "", term), el("dd", "", value));
}

function kicker(text) {
  return el("span", "panel-kicker", text);
}

function actionButton(text, action, className) {
  const control = el("button", className, text);
  control.type = "button";
  control.dataset.action = action;
  return control;
}

function el(tag, className = "", text = "") {
  const element = document.createElement(tag);
  if (className) {
    element.className = className;
  }
  if (text) {
    element.textContent = text;
  }
  return element;
}

function replicaElement(id) {
  return Array.from(dom.replicas.querySelectorAll("[data-replica]")).find((element) => element.dataset.replica === String(id));
}

export async function animateBeforeFrame(frame, speed) {
  if (reducedMotion()) {
    return;
  }
  const animations = [];
  for (const event of frame.events) {
    if (event.kind === "delivered" && event.message) {
      const from = replicaElement(event.message.from);
      const to = replicaElement(event.message.to);
      if (!from || !to) {
        continue;
      }
      const topologyRect = dom.topology.getBoundingClientRect();
      const fromRect = from.getBoundingClientRect();
      const toRect = to.getBoundingClientRect();
      const packet = el("div", `packet ${packetClass(event.message.type)}`, event.message.type.toUpperCase());
      packet.setAttribute("aria-hidden", "true");
      packet.style.left = `${fromRect.left + fromRect.width / 2 - topologyRect.left}px`;
      packet.style.top = `${fromRect.top + fromRect.height / 2 - topologyRect.top}px`;
      dom.packets.append(packet);
      const deltaX = toRect.left + toRect.width / 2 - fromRect.left - fromRect.width / 2;
      const deltaY = toRect.top + toRect.height / 2 - fromRect.top - fromRect.height / 2;
      const animation = packet.animate([
        { transform: "translate(-50%, -50%) scale(.78)", opacity: 0 },
        { opacity: 1, offset: 0.18 },
        { transform: `translate(calc(-50% + ${deltaX}px), calc(-50% + ${deltaY}px)) scale(1)`, opacity: 1 },
      ], { duration: 520 / speed, easing: "cubic-bezier(.22,.8,.22,1)", fill: "forwards" });
      animations.push(animation.finished.finally(() => packet.remove()));
    }
    if (event.kind === "dropped" && event.message) {
      const row = Array.from(dom.queue.querySelectorAll("[data-envelope]")).find((element) => element.dataset.envelope === event.message.id);
      if (row) {
        const animation = row.animate([
          { opacity: 1, borderColor: "#26313d", transform: "translateX(0)" },
          { opacity: 0, borderColor: "#ff6370", transform: "translateX(18px)" },
        ], { duration: 340 / speed, easing: "ease-out" });
        animations.push(animation.finished);
      }
    }
  }
  await Promise.allSettled(animations);
}

export async function animateAfterFrame(frame) {
  if (reducedMotion()) {
    return;
  }
  const animations = [];
  for (const event of frame.events) {
    if (event.kind === "persisted" && event.record) {
      const replica = replicaElement(event.replica);
      const row = replica && Array.from(replica.querySelectorAll("[data-ref]")).find((element) => element.dataset.ref === event.record.ref);
      const animation = row?.animate([
        { borderColor: "#62dff3", boxShadow: "0 0 22px rgba(98,223,243,.4)" },
        { borderColor: "rgba(38,49,61,.75)", boxShadow: "none" },
      ], { duration: 500, easing: "ease-out" });
      if (animation) {
        animations.push(animation.finished);
      }
    }
    if (event.kind === "applied") {
      const animation = replicaElement(event.replica)?.animate([
        { backgroundColor: "rgba(112,233,154,.34)" },
        { backgroundColor: "rgba(13,18,25,.96)" },
      ], { duration: 620, easing: "ease-out" });
      if (animation) {
        animations.push(animation.finished);
      }
    }
  }
  await Promise.allSettled(animations);
}

function packetClass(type) {
  if (type.includes("accept") && !type.includes("pre-accept")) {
    return "accept";
  }
  if (type.includes("prepare") || type.includes("try-pre-accept")) {
    return "prepare";
  }
  return "";
}

function reducedMotion() {
  return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}
