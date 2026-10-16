// ============================================================
// REPORTLI AI PLANNER
// Cloudflare Worker + Supabase + Sarvam AI
//
// Endpoints:
// GET  /health
// POST /scheduled
// POST /create-task
//
// Supabase Cron calls POST /scheduled every 5 minutes.
// ============================================================

const SARVAM_API_URL = "https://api.sarvam.ai/v1/chat/completions";
const SARVAM_MODEL = "sarvam-105b";

const SOURCES = ["reddit", "apollo"];
const MAX_APPLICATIONS = 1000;
const REQUEST_TIMEOUT_MS = 20000;

// ============================================================
// MAIN ROUTER
// ============================================================

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    try {
      if (request.method === "GET" && url.pathname === "/health") {
        return jsonResponse({
          success: true,
          worker: "reportli-ai-planner",
          status: "healthy",
          endpoints: [
            "GET /health",
            "POST /scheduled",
            "POST /create-task"
          ],
          timestamp: new Date().toISOString()
        });
      }

      if (request.method === "POST" && url.pathname === "/scheduled") {
        return await handleScheduled(env);
      }

      if (request.method === "POST" && url.pathname === "/create-task") {
        return await handleCreateTask(request, env);
      }

      return jsonResponse(
        {
          success: false,
          error: "Route not found"
        },
        404
      );
    } catch (error) {
      console.error("Worker request failed:", error);

      return jsonResponse(
        {
          success: false,
          error: error.message || "Internal server error"
        },
        500
      );
    }
  }
};

// ============================================================
// SCHEDULED HANDLER
//
// Supabase Cron sends:
//
// POST /scheduled
// Content-Type: application/json
// Body: {}
//
// This function checks every application and both sources.
// ============================================================

async function handleScheduled(env) {
  validateEnvironment(env);

  const startedAt = new Date().toISOString();

  const summary = {
    success: true,
    worker: "reportli-ai-planner",
    started_at: startedAt,
    applications_checked: 0,
    source_checks: 0,
    tasks_created: 0,
    tasks_skipped: 0,
    errors: []
  };

  // ----------------------------------------------------------
  // STEP 1: Load applications
  // ----------------------------------------------------------

  let applications;

  try {
    applications = await supabaseRequest(
      env,
      `/rest/v1/applications?select=*&limit=${MAX_APPLICATIONS}`
    );
  } catch (error) {
    return jsonResponse(
      {
        success: false,
        error: "Could not load applications from Supabase",
        details: error.message,
        ...summary
      },
      500
    );
  }

  if (!Array.isArray(applications)) {
    throw new Error("Supabase applications response was not an array");
  }

  summary.applications_checked = applications.length;

  // ----------------------------------------------------------
  // STEP 2: Process each application and source
  //
  // Process sequentially to reduce API pressure and avoid
  // launching many Sarvam requests simultaneously.
  // ----------------------------------------------------------

  for (const application of applications) {
    const applicationId = application.id;

    if (!applicationId) {
      summary.errors.push({
        application_id: null,
        error: "Application row has no id"
      });

      continue;
    }

    for (const source of SOURCES) {
      summary.source_checks++;

      try {
        const result = await processApplicationSource(
          env,
          application,
          source
        );

        if (result.created) {
          summary.tasks_created++;
        } else {
          summary.tasks_skipped++;
        }
      } catch (error) {
        console.error(
          "Application/source failed:",
          applicationId,
          source,
          error
        );

        summary.errors.push({
          application_id: applicationId,
          source,
          error: error.message
        });
      }
    }
  }

  summary.finished_at = new Date().toISOString();
  summary.success = summary.errors.length === 0;

  return jsonResponse(summary, summary.success ? 200 : 207);
}

// ============================================================
// PROCESS ONE APPLICATION + SOURCE
// ============================================================

async function processApplicationSource(
  env,
  application,
  source
) {
  const applicationId = application.id;

  // ----------------------------------------------------------
  // STEP 1: Read the latest planner_run for this source
  // ----------------------------------------------------------

  const latestRows = await supabaseRequest(
    env,
    "/rest/v1/planner_runs" +
      "?select=id,application_id,user_id,worker_type,source," +
      "pending_count,error,niche,created_at,started_at" +
      `&application_id=eq.${encodeURIComponent(applicationId)}` +
      `&source=eq.${encodeURIComponent(source)}` +
      "&order=created_at.desc" +
      "&limit=1"
  );

  const latest = Array.isArray(latestRows)
    ? latestRows[0]
    : null;

  // ----------------------------------------------------------
  // STEP 2: Decide whether a new task is needed
  // ----------------------------------------------------------

  if (latest) {
    const pendingCount = latest.pending_count;

    // A task is already pending.
    if (
      pendingCount !== null &&
      pendingCount !== undefined &&
      Number(pendingCount) > 0
    ) {
      return {
        created: false,
        reason: "Task already has pending work"
      };
    }

    // An errored task can be retried.
    if (latest.error) {
      console.log(
        "Retrying source after previous error:",
        applicationId,
        source
      );
    } else if (
      pendingCount === 0 ||
      pendingCount === "0"
    ) {
      // The previous task has no pending work.
      console.log(
        "Previous task completed; creating next task:",
        applicationId,
        source
      );
    } else {
      // Unknown state: do not create a duplicate accidentally.
      return {
        created: false,
        reason: "Latest task has an unknown pending_count"
      };
    }
  }

  // ----------------------------------------------------------
  // STEP 3: Find the user's ID
  //
  // The planner_runs table in your example contains user_id.
  // Prefer applications.user_id; otherwise use the latest
  // planner_run's user_id if available.
  // ----------------------------------------------------------

  const userId =
    application.user_id ||
    (latest && latest.user_id);

  if (!userId) {
    throw new Error(
      "Cannot create planner_run: user_id is missing from " +
      "the application and its previous planner_run"
    );
  }

  // ----------------------------------------------------------
  // STEP 4: Determine the application's niche
  // ----------------------------------------------------------

  const niche = await getApplicationNiche(
    env,
    application,
    latest
  );

  // ----------------------------------------------------------
  // STEP 5: Ask Sarvam to generate the task
  // ----------------------------------------------------------

  const generated = await generateTaskWithSarvam(
    env,
    niche,
    source,
    application
  );

  // ----------------------------------------------------------
  // STEP 6: Insert planner_run
  // ----------------------------------------------------------

  const workerType =
    source === "reddit"
      ? "reddit_research"
      : "apollo_lead_research";

  const newPlannerRun = {
    application_id: applicationId,
    user_id: userId,
    worker_type: workerType,
    source,
    task: generated.task,
    pending_count: 1,
    error: null,
    niche,
    started_at: null
  };

  const insertedRows = await supabaseRequest(
    env,
    "/rest/v1/planner_runs",
    {
      method: "POST",
      headers: {
        "Prefer": "return=representation"
      },
      body: JSON.stringify(newPlannerRun)
    }
  );

  const inserted = Array.isArray(insertedRows)
    ? insertedRows[0]
    : insertedRows;

  if (!inserted || !inserted.id) {
    throw new Error(
      "Supabase did not return the inserted planner_run"
    );
  }

  console.log("Planner task created:", {
    application_id: applicationId,
    source,
    planner_run_id: inserted.id
  });

  return {
    created: true,
    application_id: applicationId,
    source,
    planner_run_id: inserted.id,
    niche,
    task: generated.task
  };
}

// ============================================================
// FIND APPLICATION NICHE
//
// First use niche information on the application row.
// If unavailable, inspect business_knowledge and business_data.
//
// Expected knowledge format may be:
// { field: "niche", data: { niche: "..." } }
// or other business-related fields.
// ============================================================

async function getApplicationNiche(
  env,
  application,
  latest
) {
  const directCandidates = [
    application.niche,
    application.business_niche,
    application.store_niche,
    application.industry,
    application.business_type,
    latest && latest.niche
  ];

  for (const candidate of directCandidates) {
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate.trim();
    }
  }

  const applicationId = application.id;

  const tables = [
    "business_knowledge",
    "business_data"
  ];

  for (const table of tables) {
    try {
      const rows = await supabaseRequest(
        env,
        `/rest/v1/${table}` +
          "?select=field,data" +
          `&application_id=eq.${encodeURIComponent(applicationId)}` +
          "&limit=100"
      );

      if (!Array.isArray(rows)) {
        continue;
      }

      for (const row of rows) {
        const data = row.data;

        if (typeof data === "string" && data.trim()) {
          // Raw page text is not necessarily a niche.
          // Only use it as a last resort if it is a short value.
          if (data.length < 250) {
            return data.trim();
          }

          continue;
        }

        if (!data || typeof data !== "object") {
          continue;
        }

        const candidates = [
          data.niche,
          data.business_niche,
          data.store_niche,
          data.industry,
          data.business_type,
          data.business_name,
          data.name
        ];

        for (const candidate of candidates) {
          if (
            typeof candidate === "string" &&
            candidate.trim()
          ) {
            return candidate.trim();
          }
        }
      }
    } catch (error) {
      // A missing optional knowledge table should not prevent
      // trying the next table.
      console.warn(
        `Could not read optional table ${table}:`,
        error.message
      );
    }
  }

  // Last-resort fallback.
  if (application.name) {
    return application.name;
  }

  throw new Error(
    "Could not determine the application's niche. " +
    "Add a niche to applications or business_knowledge."
  );
}

// ============================================================
// GENERATE A TASK USING SARVAM
//
// Sarvam generates the task instructions.
// It does not perform the actual Reddit research or Apollo
// lead collection in this worker.
// ============================================================

async function generateTaskWithSarvam(
  env,
  niche,
  source,
  application
) {
  const sourceInstructions =
    source === "reddit"
      ? `
Create a Reddit research task.

The task should request up to 5 relevant Reddit posts.
Prefer posts published within the last 7 days.
Require the researcher to return:
- Real post title
- Original Reddit URL
- Publication date
- Relevant exact quote
- Customer problem
- Evidence of buying intent

Focus on genuine complaints, requests for solutions,
repeated pain points, and signs that people may pay
to solve the problem.

Use relevant subreddits and search terms.
Never invent posts, quotes, usernames, URLs, dates,
or engagement metrics.
If fewer than 5 relevant posts can be verified,
return only those actually found and state the count.
`
      : `
Create an Apollo lead-research task.

The task should request qualified prospects relevant to
the specified niche and target customer profile.

Ask the researcher to specify:
- Ideal customer profile
- Relevant company characteristics
- Relevant job titles and decision-makers
- Qualification criteria
- Available company and contact information
- Evidence supporting each prospect's relevance

Use only data available through authorized tools or sources.
Never invent companies, people, emails, job titles,
or engagement information.
If no reliable leads are available, report that honestly.
`;

  const systemPrompt = `
You are the task planner for Reportli AI.

Generate exactly one useful task instruction for the
requested research source and business niche.

The task is an instruction for a separate execution worker.
Do not pretend that research has already been performed.
Do not invent findings or results.

Return valid JSON only, in this format:
{
  "niche": "string",
  "task": "string"
}

The task must be specific to the supplied niche.
`;

  const userPrompt = `
Application name: ${application.name || "Unnamed application"}
Business niche: ${niche}
Source: ${source}

Requirements:
${sourceInstructions}

Return JSON with:
- niche
- task
`;

  const response = await fetchWithTimeout(
    SARVAM_API_URL,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "api-subscription-key": env.SARVAM_API_KEY
      },
      body: JSON.stringify({
        model: SARVAM_MODEL,
        messages: [
          {
            role: "system",
            content: systemPrompt
          },
          {
            role: "user",
            content: userPrompt
          }
        ],
        temperature: 0.2,
        max_tokens: 1200,
        response_format: {
          type: "json_object"
        }
      })
    },
    REQUEST_TIMEOUT_MS
  );

  const responseText = await response.text();

  if (!response.ok) {
    throw new Error(
      `Sarvam API returned HTTP ${response.status}: ` +
      responseText.slice(0, 1000)
    );
  }

  let apiResult;

  try {
    apiResult = JSON.parse(responseText);
  } catch {
    throw new Error("Sarvam returned invalid API JSON");
  }

  const content =
    apiResult &&
    apiResult.choices &&
    apiResult.choices[0] &&
    apiResult.choices[0].message &&
    apiResult.choices[0].message.content;

  if (!content) {
    throw new Error("Sarvam returned no message content");
  }

  let generated;

  try {
    generated = JSON.parse(content);
  } catch {
    throw new Error(
      "Sarvam task output was not valid JSON: " +
      String(content).slice(0, 500)
    );
  }

  if (
    typeof generated.task !== "string" ||
    !generated.task.trim()
  ) {
    throw new Error("Sarvam did not generate a valid task");
  }

  return {
    niche:
      typeof generated.niche === "string" &&
      generated.niche.trim()
        ? generated.niche.trim()
        : niche,
    task: generated.task.trim()
  };
}

// ============================================================
// MANUAL CREATE-TASK ENDPOINT
//
// POST /create-task
//
// Body:
// {
//   "application_id": "app-123",
//   "source": "reddit"
// }
//
// This endpoint intentionally creates one task for the
// specified application/source without running the whole
// scheduled scan.
// ============================================================

async function handleCreateTask(request, env) {
  validateEnvironment(env);

  let body;

  try {
    body = await request.json();
  } catch {
    return jsonResponse(
      {
        success: false,
        error: "Request body must be valid JSON"
      },
      400
    );
  }

  const applicationId = body.application_id;
  const source = body.source;

  if (!applicationId) {
    return jsonResponse(
      {
        success: false,
        error: "application_id is required"
      },
      400
    );
  }

  if (!SOURCES.includes(source)) {
    return jsonResponse(
      {
        success: false,
        error: "source must be reddit or apollo"
      },
      400
    );
  }

  const applications = await supabaseRequest(
    env,
    "/rest/v1/applications" +
      "?select=*" +
      `&id=eq.${encodeURIComponent(applicationId)}` +
      "&limit=1"
  );

  if (!Array.isArray(applications) || !applications[0]) {
    return jsonResponse(
      {
        success: false,
        error: "Application not found",
        application_id: applicationId
      },
      404
    );
  }

  const result = await createTaskForApplication(
    env,
    applications[0],
    source
  );

  return jsonResponse({
    success: true,
    application_id: applicationId,
    source,
    niche: result.niche,
    task: result.task,
    planner_run: result.planner_run
  });
}

// ============================================================
// CREATE ONE TASK WITHOUT THE SCHEDULED PENDING CHECK
// ============================================================

async function createTaskForApplication(
  env,
  application,
  source
) {
  const latestRows = await supabaseRequest(
    env,
    "/rest/v1/planner_runs" +
      "?select=id,user_id,niche" +
      `&application_id=eq.${encodeURIComponent(application.id)}` +
      "&order=created_at.desc&limit=1"
  );

  const latest = Array.isArray(latestRows)
    ? latestRows[0]
    : null;

  const userId =
    application.user_id ||
    (latest && latest.user_id);

  if (!userId) {
    throw new Error("user_id is missing for this application");
  }

  const niche = await getApplicationNiche(
    env,
    application,
    latest
  );

  const generated = await generateTaskWithSarvam(
    env,
    niche,
    source,
    application
  );

  const workerType =
    source === "reddit"
      ? "reddit_research"
      : "apollo_lead_research";

  const insertedRows = await supabaseRequest(
    env,
    "/rest/v1/planner_runs",
    {
      method: "POST",
      headers: {
        "Prefer": "return=representation"
      },
      body: JSON.stringify({
        application_id: application.id,
        user_id: userId,
        worker_type: workerType,
        source,
        task: generated.task,
        pending_count: 1,
        error: null,
        niche: generated.niche,
        started_at: null
      })
    }
  );

  const plannerRun = Array.isArray(insertedRows)
    ? insertedRows[0]
    : insertedRows;

  if (!plannerRun || !plannerRun.id) {
    throw new Error("Planner task was not saved successfully");
  }

  return {
    niche: generated.niche,
    task: generated.task,
    planner_run: plannerRun
  };
}

// ============================================================
// SUPABASE REST HELPER
// ============================================================

async function supabaseRequest(
  env,
  path,
  options = {}
) {
  const baseUrl = env.SUPABASE_URL.replace(/\/+$/, "");
  const url = `${baseUrl}${path}`;

  const headers = new Headers(options.headers || {});

  headers.set(
    "apikey",
    env.SUPABASE_SERVICE_ROLE_KEY
  );

  headers.set(
    "Authorization",
    `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`
  );

  if (options.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const response = await fetchWithTimeout(
    url,
    {
      ...options,
      headers
    },
    REQUEST_TIMEOUT_MS
  );

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Supabase returned HTTP ${response.status}: ` +
      text.slice(0, 1000)
    );
  }

  if (!text) {
    return null;
  }

  try {
    return JSON.parse(text);
  } catch {
    throw new Error("Supabase returned invalid JSON");
  }
}

// ============================================================
// FETCH WITH TIMEOUT
// ============================================================

async function fetchWithTimeout(
  url,
  options = {},
  timeoutMs = REQUEST_TIMEOUT_MS
) {
  const controller = new AbortController();

  const timeout = setTimeout(
    () => controller.abort(),
    timeoutMs
  );

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal
    });
  } finally {
    clearTimeout(timeout);
  }
}

// ============================================================
// ENVIRONMENT VALIDATION
// ============================================================

function validateEnvironment(env) {
  const required = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "SARVAM_API_KEY"
  ];

  const missing = required.filter(
    (name) => !env[name]
  );

  if (missing.length) {
    throw new Error(
      `Missing environment variables: ${missing.join(", ")}`
    );
  }
}

// ============================================================
// JSON RESPONSE
// ============================================================

function jsonResponse(data, status = 200) {
  return new Response(
    JSON.stringify(data, null, 2),
    {
      status,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store"
      }
    }
  );
          }
