const SVG_NS = "http://www.w3.org/2000/svg";

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
  document.querySelector("#lab-cta").disabled = false;
}

export function render(state) {
  const inWorkspace = state.mode === "tour" || state.mode === "lab";
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
  dom.modeLabel.textContent = state.mode === "tour" ? "GUIDED TOUR" : "PROTOCOL LAB";
  dom.title.textContent = state.mode === "tour" ? state.trace.scenario.title : `${snapshot.cluster.size}-replica lab`;
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
  content.append(kicker(state.mode === "tour" ? "GUIDED CHAPTER" : "LAB INPUT"));

  if (state.mode === "tour") {
    const headline = el("h2", "", state.frame.headline || state.trace.scenario.title);
    const explanation = el("p", "rail-copy", state.frame.explanation || state.trace.scenario.lede);
    content.append(headline, explanation);

    const chapters = el("div", "chapter-list");
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
      const action = next ? actionButton("Next chapter", "next-chapter", "button primary") : actionButton("Open the lab", "open-lab", "button primary");
      action.dataset.scenario = next?.id || "";
      if (!next) {
        content.append(el("p", "rail-copy", "Now break it yourself."));
      }
      content.append(action);
    }
  } else {
    const heading = el("h2", "", "Send one point command");
    const copy = el("p", "rail-copy", "Choose any running coordinator. Messages move only when you deliver them or start Run.");
    content.append(heading, copy);
    content.append(renderLabForm(state, selectedReplica));
  }

  if (state.error) {
    const error = el("div", "error-banner", state.error);
    error.setAttribute("role", "alert");
    content.append(error);
  }
  dom.leftRail.replaceChildren(content);
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
    const card = el("div", "replica-card");
    card.dataset.replica = String(replica.id);
    card.dataset.action = "select-replica";
    card.tabIndex = 0;
    card.setAttribute("role", "button");
    card.setAttribute("aria-pressed", String(replica.id === selectedReplica.id));
    card.setAttribute("aria-label", `Select replica ${replica.id}, ${replica.paused ? "paused" : "running"}`);
    card.style.left = `${layout[index][0]}%`;
    card.style.top = `${layout[index][1]}%`;
    card.classList.toggle("selected", replica.id === selectedReplica.id);
    card.classList.toggle("focused", state.mode === "tour" && state.frame.focus?.replica === replica.id);
    card.classList.toggle("paused", replica.paused);
    const latest = replica.instances.at(-1);
    card.dataset.latestStatus = latest ? `${latest.ref} · ${latest.status}` : "NO INSTANCES";

    const header = el("div", "replica-card-header");
    const id = el("span", "replica-id", `R${replica.id}`);
    const status = el("span", `replica-status ${replica.paused ? "paused" : ""}`, replica.paused ? "PAUSED" : "RUNNING");
    header.append(id, status);
    card.append(header, el("div", "replica-tick", `LOGICAL TICK ${replica.tick}`));

    const instances = el("div", "instance-list");
    for (const instance of replica.instances.slice(-4)) {
      const row = el("div", "instance-summary");
      row.dataset.ref = instance.ref;
      row.append(el("span", "", instance.ref), el("span", `status ${instance.status}`, instance.status.toUpperCase()));
      instances.append(row);
    }
    if (replica.instances.length === 0) {
      instances.append(el("div", "instance-summary", "NO LOCAL INSTANCES"));
    }
    card.append(instances);

    const stateSummary = el("div", "kv-summary");
    const latestState = replica.state.at(-1);
    stateSummary.append(el("span", "", latestState ? `${latestState.key}=${latestState.value}` : "STATE ∅"));
    const nodeControl = actionButton(replica.paused ? "Resume" : "Pause", replica.paused ? "resume-node" : "pause-node", `button ${replica.paused ? "" : "danger"}`);
    nodeControl.dataset.replica = String(replica.id);
    nodeControl.disabled = state.mode !== "lab" || state.busy;
    nodeControl.title = replica.paused ? `Resume R${replica.id}` : `Pause R${replica.id}`;
    stateSummary.append(nodeControl);
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
    lines.append(line);
  }
  dom.network.replaceChildren(lines);
  requestAnimationFrame(drawNetwork);
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
  dom.queueCount.textContent = `${snapshot.messages.length} ${snapshot.messages.length === 1 ? "ENVELOPE" : "ENVELOPES"}`;
  const allBlocked = snapshot.messages.length > 0 && snapshot.messages.every((message) => message.blocked);
  dom.blockedHint.hidden = !allBlocked;
  const fragment = document.createDocumentFragment();
  if (snapshot.messages.length === 0) {
    fragment.append(el("div", "empty-state", "NETWORK QUIET · NO QUEUED ENVELOPES"));
  }
  for (const message of snapshot.messages) {
    const row = el("div", `message-row ${message.blocked ? "blocked" : ""}`);
    row.dataset.envelope = message.id;
    row.append(
      el("span", "", `#${message.id}`),
      el("span", `message-type ${message.type}`, message.type.toUpperCase()),
      el("span", "", `R${message.from} → R${message.to}`),
      el("span", "message-ref", message.ref),
      el("span", "message-deps", message.deps.length ? `deps ${message.deps.join(", ")}` : "deps ∅"),
    );
    if (state.mode === "lab") {
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
    fragment.append(el("div", "empty-state", `Replica ${replica.id} has no local instances yet.`));
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
  detail(details, "Command", selected.command ? `SET ${selected.command.key}=${selected.command.value}` : "protocol-only");
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
    panel.append(el("div", "empty-state", "STATE ∅"));
  }
  for (const item of replica.state) {
    const row = el("div", "state-row");
    row.append(el("span", "", item.key), el("strong", "", item.value));
    panel.append(row);
  }
  panel.append(el("h3", "", "APPLY ORDER"));
  if (replica.applied.length === 0) {
    panel.append(el("div", "empty-state", "NOTHING APPLIED"));
  }
  replica.applied.forEach((applied, index) => {
    const row = el("div", "applied-row");
    row.append(el("span", "", String(index + 1).padStart(2, "0")), el("span", "", `${applied.ref} · SET ${applied.key}=${applied.value}`));
    panel.append(row);
  });
  return panel;
}

function renderNetworkInspector(state, replica) {
  const panel = el("section", "network-panel");
  panel.append(el("h3", "", `LINKS FROM R${replica.id}`));
  const links = state.frame.snapshot.links.filter((link) => link.from === replica.id || link.to === replica.id);
  for (const link of links) {
    const row = el("div", `network-link-row ${link.delayed ? "delayed" : ""}`);
    row.append(el("span", "", `R${link.from} ↔ R${link.to}`), el("span", "", link.delayed ? "DELAYED" : "HEALTHY"));
    if (state.mode === "lab") {
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
    const allPaused = snapshot.replicas.every((replica) => replica.paused);
    left.append(
      playbackButton("Back", "lab-back", state.busy || !state.canBack, "B"),
      playbackButton("Forward", "lab-forward", state.busy || !state.canForward, "N"),
      playbackButton("Deliver next", "deliver-next", state.busy || !deliverable, ""),
      playbackButton(state.labRunning ? "Pause" : "Run", "toggle-run", !state.labRunning && (state.busy || !deliverable), "Space"),
      playbackButton("Tick", "tick", state.busy || allPaused, ""),
      playbackButton("Reset", "reset-lab", state.busy, "R"),
    );
  }
  for (const speed of [0.5, 1, 2]) {
    const control = actionButton(`${speed}×`, "set-speed", `button ${state.speed === speed ? "active" : ""}`);
    control.dataset.speed = String(speed);
    control.disabled = state.busy;
    right.append(control);
  }
  const status = el("div", "playback-status", state.mode === "tour" ? `${state.frameIndex + 1} / ${state.trace.frames.length}` : `History ${state.frame.index}`);
  dom.playback.replaceChildren(left, status, right);
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
