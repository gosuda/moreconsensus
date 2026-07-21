const BLOCKING_MESSAGE = "The EPaxos core could not start. Use a current browser and reload.";

export async function startCore(onBlockingError) {
  try {
    if (typeof WebAssembly === "undefined" || typeof Go === "undefined") {
      throw new Error("WebAssembly runtime unavailable");
    }

    const go = new Go();
    const response = await fetch("./epaxos.wasm");
    if (!response.ok) {
      throw new Error(`Wasm request failed with ${response.status}`);
    }

    let instantiated;
    try {
      instantiated = await WebAssembly.instantiateStreaming(response.clone(), go.importObject);
    } catch {
      instantiated = await WebAssembly.instantiate(await response.arrayBuffer(), go.importObject);
    }

    let readyResolve;
    const ready = new Promise((resolve) => {
      readyResolve = resolve;
    });
    document.addEventListener("epaxos-viz-ready", readyResolve, { once: true });

    const runtime = Promise.resolve().then(() => go.run(instantiated.instance));
    await Promise.race([
      ready,
      runtime.then(
        () => Promise.reject(new Error("Wasm runtime stopped")),
        (error) => Promise.reject(error),
      ),
    ]);

    return function dispatch(request) {
      const raw = globalThis.epaxosVizDispatch(JSON.stringify(request));
      const result = JSON.parse(raw);
      if (!result.ok) {
        const error = new Error(result.error.message);
        error.code = result.error.code;
        throw error;
      }
      return result;
    };
  } catch {
    onBlockingError?.(BLOCKING_MESSAGE);
    throw new Error(BLOCKING_MESSAGE);
  }
}

export { BLOCKING_MESSAGE };
