const RELATE_API = "https://api.relate.so/v1";
const EXCLUDE_PROCESS_KEYWORDS = ["영업프로세스", "잠재고객"];

function corsHeaders(request, env) {
  const origin = request.headers.get("Origin") || "*";
  const configured = String(env.CORS_ORIGIN || "*").split(",").map(s => s.trim()).filter(Boolean);
  const allowedOrigin = configured.includes("*") || configured.includes(origin) ? origin : configured[0] || "*";
  return {
    "Access-Control-Allow-Origin": allowedOrigin,
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Vary": "Origin",
  };
}

function json(data, status = 200, request, env, extraHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      ...corsHeaders(request, env),
      ...extraHeaders,
    },
  });
}

function normalizeBrand(rawName) {
  if (!rawName) return "";
  const s = String(rawName);
  if (/석민이앤씨/.test(s)) return "석민이앤씨";
  if (/POUR\s*솔루션|POUR/i.test(s)) return "POUR솔루션";
  if (/아파트.*스퀘어/.test(s)) return "아파트스퀘어";
  return s;
}

function mapStatusToStage(statusName, statusType) {
  if (statusName) return statusName;
  if (statusType === "won") return "수주 성공";
  if (statusType === "lost") return "수주 실패";
  return "영업·관계";
}

function isExcludedProcess(name) {
  const s = String(name || "");
  return EXCLUDE_PROCESS_KEYWORDS.some(keyword => s.includes(keyword));
}

async function relateFetch(path, apiKey) {
  const url = path.startsWith("http") ? path : `${RELATE_API}${path}`;
  const response = await fetch(url, {
    method: "GET",
    headers: {
      "Authorization": `Bearer ${apiKey}`,
      "Accept": "application/json",
      "Content-Type": "application/json",
    },
  });
  if (!response.ok) throw new Error(`Relate API ${response.status}: ${path}`);
  return response.json();
}

async function relateFetchAll(path, apiKey, maxItems = 10000, pageSize = 100) {
  const all = [];
  let after = 0;
  while (all.length < maxItems) {
    const sep = path.includes("?") ? "&" : "?";
    const result = await relateFetch(`${path}${sep}first=${pageSize}&after=${after}`, apiKey);
    if (!Array.isArray(result.data) || result.data.length === 0) break;
    all.push(...result.data);
    if (!result.pagination?.has_next_page) break;
    after = result.pagination.end_cursor;
  }
  return all;
}

async function parallelLimit(items, limit, fn) {
  const results = new Array(items.length);
  let index = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (index < items.length) {
      const current = index++;
      try {
        results[current] = await fn(items[current], current);
      } catch {
        results[current] = null;
      }
    }
  });
  await Promise.all(workers);
  return results;
}

async function findProcessLists(apiKey) {
  const lists = await relateFetchAll("/lists", apiKey);
  return lists.filter(list => list.process === true);
}

function configuredProcessIds(env) {
  return String(env.RELATE_PROCESS_IDS || "")
    .split(",")
    .map(value => value.trim())
    .filter(Boolean);
}

async function buildRelatePipeline(env) {
  const apiKey = env.RELATE_API_KEY;
  if (!apiKey) throw new Error("RELATE_API_KEY secret is not configured");

  let processIds = configuredProcessIds(env);
  let skippedProcesses = [];
  if (!processIds.length) {
    const processes = await findProcessLists(apiKey);
    const kept = [];
    processes.forEach(process => {
      const name = process.name || process.title || "";
      if (isExcludedProcess(name)) skippedProcesses.push(name);
      else kept.push(process);
    });
    if (!kept.length) throw new Error("No usable Relate process lists found");
    processIds = kept.map(process => process.id);
  }

  const processMeta = await Promise.all(processIds.map(async pid => {
    try {
      const result = await relateFetch(`/lists/${pid}`, apiKey);
      return { pid, data: result.data || result };
    } catch {
      return { pid, data: null };
    }
  }));
  const entriesByProcess = await Promise.all(processIds.map(async pid => {
    try {
      return { pid, entries: await relateFetchAll(`/lists/${pid}/entries`, apiKey) };
    } catch {
      return { pid, entries: [] };
    }
  }));

  const statusTypeMap = {};
  const processNameMap = {};
  const excludedPids = new Set();

  processMeta.forEach(({ pid, data }) => {
    if (!data) {
      processNameMap[pid] = pid;
      return;
    }
    const processName = data.name || data.title || pid;
    processNameMap[pid] = processName;
    if (isExcludedProcess(processName)) {
      excludedPids.add(pid);
      skippedProcesses.push(processName);
      return;
    }
    if (Array.isArray(data.statuses)) {
      data.statuses.forEach(status => {
        statusTypeMap[status.name] = status.type;
      });
    }
  });

  const allEntries = [];
  entriesByProcess.forEach(({ pid, entries }) => {
    if (excludedPids.has(pid)) return;
    const processName = processNameMap[pid] || pid;
    entries.forEach(entry => allEntries.push({ ...entry, _processName: processName, _listId: pid }));
  });

  // organization 이름 조회는 별도 /relate/organizations endpoint에서 paginate 일괄 fetch
  // (pipeline 안에서 individual /organizations/{id} 호출은 Cloudflare 50 subrequest 한도 무조건 초과 → 시간만 낭비)
  const orgCache = {};
  const neededOrgIds = [...new Set(
    allEntries
      .filter(entry => entry.entryable_type === "Organization" && entry.entryable_id)
      .map(entry => entry.entryable_id)
  )];
  // 노트도 별도 endpoint
  const allNotes = [];
  const noteDiag = { mode: 'separate-endpoint', message: '클라이언트가 /relate/organizations, /relate/notes 를 병렬 호출' };

  const today = new Date().toISOString().split("T")[0];
  // 가능한 모든 이름 후보 필드를 순회 — 비어있지 않은 첫 값을 반환
  const pickName = (...candidates) => {
    for (const c of candidates) {
      if (c == null) continue;
      const s = String(c).trim();
      if (s) return s;
    }
    return "";
  };
  const pipeline = allEntries.map(entry => {
    const amount = (entry.one_time_value_cents || 0) + (entry.recurring_value_cents || 0);
    const cachedOrg = entry.entryable_type === "Organization" ? orgCache[entry.entryable_id] : null;
    const fallbackName = pickName(
      cachedOrg,
      entry.entryable?.name,
      entry.entryable?.title,
      entry.organization?.name,
      entry.title,
      entry.name,
      entry.key,
      entry.label,
      entry.summary
    );
    const orgName = fallbackName
      || (entry.entryable_type === "Organization"
        ? `(현장명 미등록 · ${entry.entryable_id})`
        : `(미확인 · entry ${entry.id})`);
    const statusName = entry.status || "";
    const statusType = statusTypeMap[statusName] || "active";
    const updatedDate = entry.updated_at ? entry.updated_at.split("T")[0] : today;
    const createdDate = entry.created_at ? entry.created_at.split("T")[0] : updatedDate;
    return {
      entryId: String(entry.id),
      listId: String(entry._listId || ""),
      organizationId: entry.entryable_type === "Organization" ? String(entry.entryable_id || "") : "",
      assigneeName: entry.assignee?.name || entry.assignee?.email || "미정",
      stage: mapStatusToStage(statusName, statusType),
      orgName,
      lastUpdated: updatedDate,
      createdDate,
      estAmount: amount,
      brand: normalizeBrand(entry._processName || ""),
    };
  }).sort((a, b) => String(b.lastUpdated).localeCompare(String(a.lastUpdated)));

  return {
    ok: true,
    source: "relate-worker",
    updatedAt: new Date().toISOString(),
    processCount: processIds.length - excludedPids.size,
    skippedProcesses: [...new Set(skippedProcesses)].filter(Boolean),
    entryCount: pipeline.length,
    organizationCount: neededOrgIds.length,
    noteCount: allNotes.length,
    noteDiag,
    pipeline,
    notes: allNotes,
  };
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders(request, env) });
    }

    const url = new URL(request.url);
    if (url.pathname === "/" || url.pathname === "/health") {
      return json({ ok: true, service: "netform-relate-proxy" }, 200, request, env);
    }

    // 전체 organization 목록 fetch — 클라이언트에서 organizationId → name 매핑
    // (개별 /organizations/{id} 호출은 subrequest 한도(50) 때문에 pipeline 안에서 모두 못 함)
    if (url.pathname === "/relate/organizations") {
      if (!env.RELATE_API_KEY) return json({ ok: false, error: "RELATE_API_KEY missing" }, 500, request, env);
      try {
        const raw = await relateFetchAll('/organizations', env.RELATE_API_KEY);
        const orgs = raw.map(o => ({ id: String(o.id || ""), name: o.name || o.title || "" })).filter(o => o.id);
        return json({ ok: true, count: orgs.length, orgs }, 200, request, env, {
          "Cache-Control": "public, max-age=600",
        });
      } catch (err) {
        return json({ ok: false, error: String(err?.message || err).slice(0, 300) }, 502, request, env);
      }
    }

    // 전체 노트 fetch — orgId 별 그룹화는 클라이언트에서 (Relate API 의 /notes 는 organization_id 로 묶임)
    if (url.pathname === "/relate/notes") {
      if (!env.RELATE_API_KEY) return json({ ok: false, error: "RELATE_API_KEY missing" }, 500, request, env);
      try {
        const raw = await relateFetchAll('/notes', env.RELATE_API_KEY);
        const cleaned = raw.map(note => {
          const rawContent = note.body || note.content || note.text || note.note || "";
          const content = String(rawContent)
            .replace(/<br\s*\/?>/gi, "\n")
            .replace(/<\/(p|div|li)>/gi, "\n")
            .replace(/<[^>]+>/g, "")
            .replace(/&nbsp;/g, " ")
            .replace(/&amp;/g, "&")
            .replace(/&lt;/g, "<")
            .replace(/&gt;/g, ">")
            .trim();
          const at = note.posted_at || note.created_at || note.updated_at || "";
          return {
            id: String(note.id || ""),
            organizationId: String(note.organization_id || ""),
            date: at ? at.split("T")[0] : "",
            time: at ? (at.split("T")[1] || "").slice(0, 5) : "",
            content,
            author: note.user?.name || note.user?.email || note.author || "",
          };
        }).filter(n => n.content && n.organizationId);
        cleaned.sort((a, b) => String(b.date + " " + (b.time || "")).localeCompare(String(a.date + " " + (a.time || ""))));
        return json({ ok: true, count: cleaned.length, notes: cleaned }, 200, request, env, {
          "Cache-Control": "public, max-age=300",
        });
      } catch (err) {
        return json({ ok: false, error: String(err?.message || err).slice(0, 300) }, 502, request, env);
      }
    }

    if (url.pathname !== "/relate/pipeline") {
      return json({ ok: false, error: "not found" }, 404, request, env);
    }

    try {
      if (!env.RELATE_API_KEY) throw new Error("RELATE_API_KEY secret is not configured");
      const edgeCache = globalThis.caches?.default;
      const cacheKey = new Request(`${url.origin}${url.pathname}?v=1&process=${encodeURIComponent(env.RELATE_PROCESS_IDS || "")}`, request);
      if (edgeCache && url.searchParams.get("refresh") !== "1") {
        const cached = await edgeCache.match(cacheKey);
        if (cached) {
          const response = new Response(cached.body, cached);
          response.headers.set("X-Cache", "HIT");
          Object.entries(corsHeaders(request, env)).forEach(([key, value]) => response.headers.set(key, value));
          return response;
        }
      }

      const payload = await buildRelatePipeline(env);
      const response = json(payload, 200, request, env, {
        "Cache-Control": "public, max-age=300",
        "X-Cache": "MISS",
      });
      if (edgeCache) ctx.waitUntil(edgeCache.put(cacheKey, response.clone()));
      return response;
    } catch (error) {
      return json({ ok: false, error: error.message || "Relate proxy failed" }, 500, request, env);
    }
  },
};
