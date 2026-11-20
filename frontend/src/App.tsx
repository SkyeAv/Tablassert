import { createSignal } from "solid-js";
import { For, Show } from "@solidjs/web";

interface ValidationResult {
  valid: boolean;
  errors: string[];
  table_yaml: string;
  graph_yaml: string;
}

export default function App() {
  const [kind, setKind] = createSignal<"text" | "excel" | "parquet" | "ipc" | "ndjson">("text");
  const [local, setLocal] = createSignal("data/sample.tsv");
  const [url, setUrl] = createSignal("https://example.org/sample.tsv");
  const [delimiter, setDelimiter] = createSignal("\t");
  const [sheet, setSheet] = createSignal("Sheet1");
  const [rowSliceStart, setRowSliceStart] = createSignal(1);

  const [subjectCol, setSubjectCol] = createSignal("A");
  const [subjectPrio, setSubjectPrio] = createSignal("Gene");
  const [predicate, setPredicate] = createSignal("associated_with");
  const [objectCol, setObjectCol] = createSignal("B");
  const [objectPrio, setObjectPrio] = createSignal("Disease");
  const [repo, setRepo] = createSignal("PMID");
  const [publication, setPublication] = createSignal("12345678");

  const [name, setName] = createSignal("my-kg");
  const [version, setVersion] = createSignal("0.1.0");
  const [fullmap, setFullmap] = createSignal(".fullmap");
  const [inforesId, setInforesId] = createSignal("infores:tablassert-draft");
  const [description, setDescription] = createSignal("Knowledge graph built with Tablassert");
  const [licenseName, setLicenseName] = createSignal("CC0 1.0 Universal");
  const [accessUrl, setAccessUrl] = createSignal("https://example.org/data");
  const [contribution, setContribution] = createSignal("Curator - modeling");
  const [artifactUrl, setArtifactUrl] = createSignal("https://example.org/kg");
  const [artifactPath, setArtifactPath] = createSignal("./output");

  const [validation, setValidation] = createSignal<ValidationResult | null>(null);
  const [buildLog, setBuildLog] = createSignal<string[]>([]);
  const [busy, setBusy] = createSignal(false);

  const buildPayload = () => ({
    source: {
      kind: kind(),
      local: local(),
      url: [url()],
      delimiter: delimiter(),
      sheet: sheet(),
      row_slice_start: rowSliceStart(),
    },
    statement: {
      subject_column: subjectCol(),
      subject_prioritize: subjectPrio(),
      predicate: predicate(),
      object_column: objectCol(),
      object_prioritize: objectPrio(),
    },
    provenance: {
      repo: repo(),
      publication: publication(),
    },
    graph: {
      name: name(),
      version: version(),
      fullmap: fullmap(),
      infores_id: inforesId(),
      description: description(),
      license_name: licenseName(),
      data_access_url: accessUrl(),
      contribution: contribution(),
      artifact_base_url: artifactUrl(),
      artifact_base_path: artifactPath(),
    },
  });

  async function runValidate() {
    setBusy(true);
    try {
      const res = await fetch("/api/validate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(buildPayload()),
      });
      const data = (await res.json()) as ValidationResult;
      setValidation(data);
    } finally {
      setBusy(false);
    }
  }

  async function runBuild() {
    setBusy(true);
    setBuildLog(["Starting build-kg..."]);
    try {
      const res = await fetch("/api/build", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(buildPayload()),
      });
      const reader = res.body?.getReader();
      if (!reader) return;
      const decoder = new TextDecoder();
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        const chunk = decoder.decode(value, { stream: true });
        for (const line of chunk.split("\n")) {
          if (line.trim()) {
            setBuildLog((prev) => [...prev, line]);
          }
        }
      }
    } finally {
      setBusy(false);
    }
  }

  const inputClass =
    "mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100 focus:border-sky-400 focus:outline-none";

  return (
    <main class="mx-auto max-w-5xl space-y-6 px-6 py-8">
      <header class="space-y-1 border-b border-slate-800 pb-4">
        <h1 class="text-2xl font-bold tracking-tight text-slate-50">Tablassert Web UI</h1>
        <p class="text-sm text-slate-400">
          Localhost-only wizard powered by SolidJS 2.0 + Tailwind and typed FastAPI validation.
        </p>
      </header>

      <section class="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
        <h2 class="mb-4 text-lg font-semibold text-sky-300">1. Source</h2>
        <div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          <label class="text-xs font-medium text-slate-400">
            Kind
            <select
              class={inputClass}
              value={kind()}
              onChange={(e) => setKind(e.currentTarget.value as "text" | "excel" | "parquet" | "ipc" | "ndjson")}
            >
              <option value="text">text (CSV/TSV)</option>
              <option value="excel">excel</option>
              <option value="parquet">parquet</option>
              <option value="ipc">ipc</option>
              <option value="ndjson">ndjson</option>
            </select>
          </label>
          <label class="text-xs font-medium text-slate-400">
            Local Path
            <input class={inputClass} value={local()} onInput={(e) => setLocal(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Source URL
            <input class={inputClass} value={url()} onInput={(e) => setUrl(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Delimiter (text)
            <input class={inputClass} value={delimiter()} onInput={(e) => setDelimiter(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Sheet (excel)
            <input class={inputClass} value={sheet()} onInput={(e) => setSheet(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Row Slice Start
            <input
              type="number"
              min="0"
              class={inputClass}
              value={rowSliceStart()}
              onInput={(e) => setRowSliceStart(Number(e.currentTarget.value || 0))}
            />
          </label>
        </div>
      </section>

      <section class="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
        <h2 class="mb-4 text-lg font-semibold text-sky-300">2. Column Mapping &amp; Provenance</h2>
        <div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          <label class="text-xs font-medium text-slate-400">
            Subject Column
            <input class={inputClass} value={subjectCol()} onInput={(e) => setSubjectCol(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Subject Prioritize
            <input class={inputClass} value={subjectPrio()} onInput={(e) => setSubjectPrio(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Predicate
            <input class={inputClass} value={predicate()} onInput={(e) => setPredicate(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Object Column
            <input class={inputClass} value={objectCol()} onInput={(e) => setObjectCol(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Object Prioritize
            <input class={inputClass} value={objectPrio()} onInput={(e) => setObjectPrio(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Provenance Repo
            <input class={inputClass} value={repo()} onInput={(e) => setRepo(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Publication ID
            <input class={inputClass} value={publication()} onInput={(e) => setPublication(e.currentTarget.value)} />
          </label>
        </div>
      </section>

      <section class="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
        <h2 class="mb-4 text-lg font-semibold text-sky-300">3. Graph &amp; RIG Metadata</h2>
        <div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          <label class="text-xs font-medium text-slate-400">
            Graph Name
            <input class={inputClass} value={name()} onInput={(e) => setName(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Version
            <input class={inputClass} value={version()} onInput={(e) => setVersion(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Fullmap Path
            <input class={inputClass} value={fullmap()} onInput={(e) => setFullmap(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Infores ID
            <input class={inputClass} value={inforesId()} onInput={(e) => setInforesId(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Description
            <input class={inputClass} value={description()} onInput={(e) => setDescription(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            License Name
            <input class={inputClass} value={licenseName()} onInput={(e) => setLicenseName(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Data Access URL
            <input class={inputClass} value={accessUrl()} onInput={(e) => setAccessUrl(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Contribution
            <input class={inputClass} value={contribution()} onInput={(e) => setContribution(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Artifact Base URL
            <input class={inputClass} value={artifactUrl()} onInput={(e) => setArtifactUrl(e.currentTarget.value)} />
          </label>
          <label class="text-xs font-medium text-slate-400">
            Artifact Output Dir
            <input class={inputClass} value={artifactPath()} onInput={(e) => setArtifactPath(e.currentTarget.value)} />
          </label>
        </div>
      </section>

      <section class="space-y-4 rounded-xl border border-slate-800 bg-slate-900/70 p-5">
        <div class="flex flex-wrap gap-3">
          <button
            type="button"
            disabled={busy()}
            onClick={runValidate}
            class="rounded-lg bg-sky-400 px-4 py-2 text-sm font-semibold text-slate-950 hover:bg-sky-300 disabled:opacity-50"
          >
            Generate &amp; Validate YAML
          </button>
          <button
            type="button"
            disabled={busy()}
            onClick={runBuild}
            class="rounded-lg bg-slate-700 px-4 py-2 text-sm font-semibold text-slate-100 hover:bg-slate-600 disabled:opacity-50"
          >
            Write YAML &amp; Run build-kg
          </button>
        </div>

        <Show when={validation()}>
          {(res) => (
            <div class="space-y-3">
              <p class={res().valid ? "text-sm font-medium text-emerald-400" : "text-sm font-medium text-rose-400"}>
                {res().valid ? "Valid: tables/table.yaml and graph.yaml pass schema validation." : "Validation failed:"}
              </p>
              <Show when={!res().valid}>
                <ul class="list-disc pl-5 text-xs text-rose-300">
                  <For each={res().errors}>{(err) => <li>{err}</li>}</For>
                </ul>
              </Show>
              <div class="grid gap-4 md:grid-cols-2">
                <div>
                  <h3 class="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-400">tables/table.yaml</h3>
                  <pre class="overflow-x-auto rounded-lg bg-slate-950 p-3 text-xs text-slate-200">
                    {res().table_yaml}
                  </pre>
                </div>
                <div>
                  <h3 class="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-400">graph.yaml</h3>
                  <pre class="overflow-x-auto rounded-lg bg-slate-950 p-3 text-xs text-slate-200">
                    {res().graph_yaml}
                  </pre>
                </div>
              </div>
            </div>
          )}
        </Show>

        <Show when={buildLog().length > 0}>
          <div>
            <h3 class="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-400">Build Stream</h3>
            <pre class="overflow-x-auto rounded-lg bg-slate-950 p-3 text-xs text-slate-300">
              {buildLog().join("\n")}
            </pre>
          </div>
        </Show>
      </section>
    </main>
  );
}
