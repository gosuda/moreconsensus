import { startCore } from "./wasm.js";
import { animateAfterFrame, animateBeforeFrame, render, setCoreReady } from "./view.js";

const state = {
  dispatch: null,
  catalog: [],
  mode: "landing",
  trace: null,
  frame: null,
  frameIndex: 0,
  selectedReplica: 1,
  selectedInstanceRef: "",
  inspectorTab: "INSTANCE",
  inspectorOpen: false,
  tourPlaying: false,
  labRunning: false,
  speed: 1,
  busy: false,
  error: "",
  canBack: false,
  canForward: false,
  eventLog: [],
  labFrames: new Map(),
  labDraft: { coordinator: "1", key: "cart", value: "reserved" },
  chain: Promise.resolve(),
  timer: 0,
};

const liveRegion = document.querySelector("#live-region");
const blockingError = document.querySelector("#blocking-error");
const shareDialog = document.querySelector("#share-dialog");
const shareURL = document.querySelector("#share-url");

void initialize();

async function initialize() {
  installEvents();
  try {
    state.dispatch = await startCore(showBlockingError);
    state.catalog = state.dispatch({ op: "catalog" }).scenarios;
    setCoreReady();
    await openRoute({ replaceInvalid: true });
  } catch {
    // startCore already exposes the required blocking message in the document.
  }
}

function installEvents() {
  document.addEventListener("click", (event) => {
    const control = event.target.closest("[data-action]");
    if (!control || control.disabled) {
      return;
    }
    void handleAction(control.dataset.action, control);
  });

  document.addEventListener("submit", (event) => {
    if (event.target.matches('[data-role="lab-form"]')) {
      event.preventDefault();
      void propose();
    }
  });

  document.addEventListener("input", (event) => {
    const field = event.target.dataset.field;
    if (field === "key" || field === "value" || field === "coordinator") {
      state.labDraft[field] = event.target.value;
    }
  });

  document.addEventListener("change", (event) => {
    if (event.target.dataset.field === "coordinator") {
      state.labDraft.coordinator = event.target.value;
    }
  });

  document.addEventListener("keydown", (event) => {
    if ((event.key === "Enter" || event.key === " ") && event.target.matches('.replica-card[role="button"]')) {
      event.preventDefault();
      selectReplica(Number(event.target.dataset.replica));
      return;
    }
    if (isEditing(event.target) || event.metaKey || event.ctrlKey || event.altKey) {
      return;
    }
    const key = event.key.toLowerCase();
    if (key === " ") {
      event.preventDefault();
      if (state.mode === "tour") {
        toggleTourPlayback();
      } else if (state.mode === "lab") {
        toggleLabRun();
      }
    } else if (key === "n") {
      event.preventDefault();
      if (state.mode === "tour") {
        void goToTourFrame(state.frameIndex + 1);
      } else if (state.mode === "lab") {
        void seekLab(state.frame.index + 1);
      }
    } else if (key === "b") {
      event.preventDefault();
      if (state.mode === "tour") {
        void goToTourFrame(state.frameIndex - 1);
      } else if (state.mode === "lab") {
        void seekLab(state.frame.index - 1);
      }
    } else if (key === "r") {
      event.preventDefault();
      if (state.mode === "tour") {
        void goToTourFrame(0);
      } else if (state.mode === "lab") {
        void resetLab(state.frame.snapshot.cluster.size, true);
      }
    }
  });

  document.querySelector(".tabs").addEventListener("click", (event) => {
    const tab = event.target.closest("[data-tab]");
    if (!tab) {
      return;
    }
    state.inspectorTab = tab.dataset.tab;
    render(state);
  });

  window.addEventListener("popstate", () => {
    stopPlayback();
    void openRoute({ replaceInvalid: false });
  });
}

async function handleAction(action, control) {
  switch (action) {
    case "home":
      goHome();
      break;
    case "start-tour":
      await openTour("parallel", { autoplay: true, updateURL: true });
      break;
    case "open-lab":
      await resetLab(3, false);
      break;
    case "open-scenario":
      await openTour(control.dataset.scenario, { autoplay: false, updateURL: true });
      break;
    case "next-chapter":
      await openTour(control.dataset.scenario, { autoplay: false, updateURL: true });
      break;
    case "toggle-play":
      toggleTourPlayback();
      break;
    case "next":
      await goToTourFrame(state.frameIndex + 1);
      break;
    case "back":
      await goToTourFrame(state.frameIndex - 1);
      break;
    case "reset-tour":
      await goToTourFrame(0);
      break;
    case "select-replica":
      selectReplica(Number(control.dataset.replica));
      break;
    case "select-instance":
      state.selectedInstanceRef = control.dataset.ref;
      render(state);
      break;
    case "toggle-inspector":
      state.inspectorOpen = !state.inspectorOpen;
      render(state);
      break;
    case "close-inspector":
      state.inspectorOpen = false;
      render(state);
      break;
    case "set-speed":
      state.speed = Number(control.dataset.speed);
      render(state);
      break;
    case "set-size":
      await resetLab(Number(control.dataset.size), true);
      break;
    case "propose":
      await propose();
      break;
    case "deliver-next":
      await dispatchLab({ kind: "deliver-next" });
      break;
    case "deliver-envelope":
      await dispatchLab({ kind: "deliver", envelope: control.dataset.envelope });
      break;
    case "drop-envelope":
      await dispatchLab({ kind: "drop", envelope: control.dataset.envelope });
      break;
    case "pause-node":
      await dispatchLab({ kind: "pause", replica: Number(control.dataset.replica) });
      break;
    case "resume-node":
      await dispatchLab({ kind: "resume", replica: Number(control.dataset.replica) });
      break;
    case "delay-link":
      await dispatchLab({ kind: "delay-link", replica: Number(control.dataset.from), peer: Number(control.dataset.to) });
      break;
    case "heal-link":
      await dispatchLab({ kind: "heal-link", replica: Number(control.dataset.from), peer: Number(control.dataset.to) });
      break;
    case "tick":
      await dispatchLab({ kind: "tick", replica: 0 });
      break;
    case "toggle-run":
      toggleLabRun();
      break;
    case "lab-back":
      await seekLab(state.frame.index - 1);
      break;
    case "lab-forward":
      await seekLab(state.frame.index + 1);
      break;
    case "reset-lab":
      await resetLab(state.frame.snapshot.cluster.size, true);
      break;
    case "share":
      await shareCurrentURL();
      break;
    default:
      break;
  }
}

async function openRoute({ replaceInvalid }) {
  if (!state.dispatch) {
    return;
  }
  const params = new URLSearchParams(location.search);
  const mode = params.get("mode");
  if (!mode) {
    goHome(false);
    return;
  }
  if (mode === "tour") {
    const scenario = params.get("scenario");
    if (state.catalog.some((item) => item.id === scenario) && params.size === 2) {
      await openTour(scenario, { autoplay: false, updateURL: false });
      return;
    }
  }
  if (mode === "lab") {
    const size = Number(params.get("size"));
    if ((size === 3 || size === 5) && params.size === 2) {
      await resetLab(size, false, false);
      return;
    }
  }
  goHome(replaceInvalid);
}

async function openTour(scenario, { autoplay, updateURL }) {
  stopPlayback();
  await runSerialized(async () => {
    const trace = state.dispatch({ op: "scenario", scenario }).trace;
    state.mode = "tour";
    state.trace = trace;
    state.frame = trace.frames[0];
    state.frameIndex = 0;
    state.selectedReplica = 1;
    state.selectedInstanceRef = "";
    state.eventLog = [];
    state.error = "";
    state.inspectorOpen = false;
    if (updateURL) {
      setURL({ mode: "tour", scenario });
    }
    render(state);
  });
  if (autoplay) {
    state.tourPlaying = true;
    render(state);
    scheduleTourStep();
  }
}

async function goToTourFrame(index) {
  if (state.mode !== "tour" || state.busy || index < 0 || index >= state.trace.frames.length || index === state.frameIndex) {
    return;
  }
  await runSerialized(async () => {
    await transitionFrame(state.trace.frames[index], "tour");
  });
}

function toggleTourPlayback() {
  if (state.mode !== "tour") {
    return;
  }
  if (state.tourPlaying) {
    state.tourPlaying = false;
    clearTimeout(state.timer);
    render(state);
    return;
  }
  if (state.frameIndex >= state.trace.frames.length - 1) {
    void goToTourFrame(0).then(() => {
      state.tourPlaying = true;
      render(state);
      scheduleTourStep();
    });
    return;
  }
  state.tourPlaying = true;
  render(state);
  scheduleTourStep();
}

function scheduleTourStep() {
  clearTimeout(state.timer);
  if (!state.tourPlaying) {
    return;
  }
  state.timer = window.setTimeout(async () => {
    if (!state.tourPlaying) {
      return;
    }
    await goToTourFrame(state.frameIndex + 1);
    if (state.frameIndex >= state.trace.frames.length - 1) {
      state.tourPlaying = false;
      render(state);
      return;
    }
    scheduleTourStep();
  }, 760 / state.speed);
}

async function resetLab(size, confirmIfNeeded, updateURL = true) {
  if (state.busy) {
    return;
  }
  const nonempty = state.mode === "lab" && state.frame?.index > 0;
  if (confirmIfNeeded && nonempty && !window.confirm("Start a new lab? This clears the current trace.")) {
    return;
  }
  stopPlayback();
  await runSerialized(async () => {
    const result = state.dispatch({ op: "lab.reset", size });
    state.mode = "lab";
    state.trace = null;
    state.frame = result.frame;
    state.frameIndex = 0;
    state.selectedReplica = 1;
    state.selectedInstanceRef = "";
    state.canBack = result.canBack;
    state.canForward = result.canForward;
    state.eventLog = [];
    state.labFrames = new Map([[0, result.frame]]);
    state.labDraft = { coordinator: "1", key: "cart", value: "reserved" };
    state.error = "";
    state.inspectorOpen = false;
    if (updateURL) {
      setURL({ mode: "lab", size: String(size) });
    }
    render(state);
  });
}

async function propose() {
  if (state.mode !== "lab") {
    return;
  }
  await dispatchLab({
    kind: "propose",
    replica: Number(state.labDraft.coordinator),
    key: state.labDraft.key,
    value: state.labDraft.value,
  });
}

async function dispatchLab(action, { quietBlocked = false } = {}) {
  if (state.mode !== "lab" || state.busy) {
    return false;
  }
  let succeeded = false;
  await runSerialized(async () => {
    try {
      const result = state.dispatch({ op: "lab.action", action: completeAction(action) });
      for (const index of Array.from(state.labFrames.keys())) {
        if (index >= result.frame.index) {
          state.labFrames.delete(index);
        }
      }
      state.labFrames.set(result.frame.index, result.frame);
      state.canBack = result.canBack;
      state.canForward = result.canForward;
      state.error = "";
      await transitionFrame(result.frame, "lab");
      succeeded = true;
    } catch (error) {
      if (quietBlocked && error.code === "blocked") {
        state.labRunning = false;
        state.error = "";
        return;
      }
      throw error;
    }
  });
  return succeeded;
}

async function seekLab(index) {
  if (state.mode !== "lab" || state.busy || index < 0 || (index > state.frame.index && !state.canForward)) {
    return;
  }
  stopPlayback();
  await runSerialized(async () => {
    const result = state.dispatch({ op: "lab.seek", index });
    state.canBack = result.canBack;
    state.canForward = result.canForward;
    state.error = "";
    await transitionFrame(result.frame, "lab");
  });
}

function toggleLabRun() {
  if (state.mode !== "lab") {
    return;
  }
  if (state.labRunning) {
    state.labRunning = false;
    clearTimeout(state.timer);
    render(state);
    return;
  }
  const deliverable = state.frame.snapshot.messages.some((message) => !message.blocked);
  if (!deliverable || state.busy) {
    return;
  }
  state.labRunning = true;
  render(state);
  scheduleLabStep();
}

function scheduleLabStep() {
  clearTimeout(state.timer);
  if (!state.labRunning) {
    return;
  }
  state.timer = window.setTimeout(async () => {
    if (!state.labRunning) {
      return;
    }
    const beforeTicks = state.frame.snapshot.replicas.map((replica) => replica.tick);
    const moved = await dispatchLab({ kind: "deliver-next" }, { quietBlocked: true });
    const afterTicks = state.frame.snapshot.replicas.map((replica) => replica.tick);
    if (beforeTicks.some((tick, index) => tick !== afterTicks[index])) {
      state.labRunning = false;
      state.error = "Run stopped because logical ticks changed unexpectedly.";
      render(state);
      return;
    }
    if (!moved || !state.labRunning || !state.frame.snapshot.messages.some((message) => !message.blocked)) {
      state.labRunning = false;
      render(state);
      return;
    }
    scheduleLabStep();
  }, 620 / state.speed);
}

async function transitionFrame(frame, mode) {
  await animateBeforeFrame(frame, state.speed);
  state.frame = frame;
  state.frameIndex = frame.index;
  if (mode === "tour" && frame.focus?.replica) {
    state.selectedReplica = frame.focus.replica;
    state.selectedInstanceRef = frame.focus.ref || state.selectedInstanceRef;
  }
  if (mode === "tour") {
    state.eventLog = state.trace.frames.slice(1, frame.index + 1).map((item) => ({ index: item.index, events: item.events }));
  } else {
    state.eventLog = Array.from(state.labFrames.entries())
      .filter(([index]) => index > 0 && index <= frame.index)
      .sort(([left], [right]) => left - right)
      .map(([index, item]) => ({ index, events: item.events }));
  }
  render(state);
  await animateAfterFrame(frame);
  const details = frame.events.map((event) => event.detail).join(" ");
  if (details) {
    announce(details);
  }
}

async function runSerialized(operation) {
  if (state.busy) {
    return;
  }
  state.busy = true;
  render(state);
  const next = state.chain.then(operation);
  state.chain = next.catch(() => {});
  try {
    await next;
  } catch (error) {
    state.error = error.message || "The requested action failed.";
    announce(state.error);
  } finally {
    state.busy = false;
    render(state);
  }
}

function completeAction(action) {
  return {
    kind: action.kind || "",
    replica: action.replica || 0,
    peer: action.peer || 0,
    envelope: action.envelope || "",
    key: action.key || "",
    value: action.value || "",
  };
}

function selectReplica(replica) {
  if (!state.frame?.snapshot.replicas.some((item) => item.id === replica)) {
    return;
  }
  state.selectedReplica = replica;
  state.selectedInstanceRef = "";
  if (window.innerWidth < 1100) {
    state.inspectorOpen = true;
  }
  render(state);
}

async function shareCurrentURL() {
  const url = location.href;
  if (navigator.share) {
    try {
      await navigator.share({ title: document.title, url });
      return;
    } catch (error) {
      if (error.name === "AbortError") {
        return;
      }
    }
  }
  try {
    await navigator.clipboard.writeText(url);
    announce("Link copied.");
  } catch {
    shareURL.value = url;
    shareDialog.showModal();
    shareURL.focus();
    shareURL.select();
    announce("Copy this link:");
  }
}

function goHome(replace = false) {
  stopPlayback();
  state.mode = "landing";
  state.trace = null;
  state.frame = null;
  state.error = "";
  const url = new URL(location.href);
  url.search = "";
  history[replace ? "replaceState" : "pushState"]({}, "", url);
  render(state);
}

function setURL(values) {
  const url = new URL(location.href);
  url.search = "";
  for (const [key, value] of Object.entries(values)) {
    url.searchParams.set(key, value);
  }
  history.pushState({}, "", url);
}

function stopPlayback() {
  state.tourPlaying = false;
  state.labRunning = false;
  clearTimeout(state.timer);
}

function announce(message) {
  liveRegion.textContent = "";
  requestAnimationFrame(() => {
    liveRegion.textContent = message;
  });
}

function showBlockingError(message) {
  stopPlayback();
  document.querySelector("#landing").hidden = true;
  document.querySelector("#workspace").hidden = true;
  blockingError.hidden = false;
  blockingError.textContent = message;
}

function isEditing(target) {
  return target instanceof Element &&
    (target.matches("input, select, textarea, [contenteditable='true']") || Boolean(target.closest("dialog[open]")));
}
