require("dotenv").config();

const cron = require("node-cron");
const crypto = require("crypto");
const mysql = require("mysql2/promise");
const nodemailer = require("nodemailer");
const fs = require("fs");
const path = require("path");
const { runLogSync } = require("./log-sync");

const {
  GeneralMailTemplate,
} = require("./templates/email/GeneralMailTemplate");

const { TaskMailTemplate } = require("./templates/email/TaskEmailTemplate");

// ─────────────────────────────────────────────
// Expo Push Notification helper
// ─────────────────────────────────────────────

async function sendExpoPushNotifications(conn, userId, title, body, data = {}) {
  try {
    // Join to `user` and check status server-side (rather than trusting the
    // mobile app to always successfully call DELETE /api/profile/devices on
    // every logout/deactivation path) — a deactivated or otherwise
    // inactive user's device(s) must never keep receiving push
    // notifications, regardless of whether client-side cleanup happened.
    const [userRows] = await conn.execute(
      "SELECT status FROM user WHERE user_id = ? LIMIT 1",
      [userId],
    ).catch(() => [[]]);
    const userStatus = parseJsonArraySafe(userRows?.[0]?.status)?.[0]?.id;
    if (!userRows.length || userStatus !== 12) {
      return;
    }

    const [devices] = await conn.execute(
      "SELECT push_token FROM user_devices WHERE user_id = ? AND push_token IS NOT NULL AND push_token != ''",
      [userId],
    ).catch(() => [[]]);

    if (!devices.length) return;

    const messages = devices.map((d) => ({
      to: d.push_token,
      sound: "default",
      title,
      body,
      data,
    }));

    const response = await fetch("https://exp.host/--/api/v2/push/send", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json",
      },
      body: JSON.stringify(messages),
    });

    if (!response.ok) {
      console.warn(`[ExpoPush] HTTP ${response.status} for user ${userId}`);
      return;
    }

    // Expo returns one receipt per message, in the same order as `messages`.
    // A token whose device uninstalled the app (or reinstalled, invalidating
    // it) comes back "DeviceNotRegistered" forever — without pruning it here,
    // every future cron tick keeps re-sending to (and re-logging warnings
    // for) a token Expo will never deliver to again.
    const body = await response.json().catch(() => null);
    const receipts = Array.isArray(body?.data) ? body.data : [];

    const deadTokens = receipts
      .map((receipt, i) =>
        receipt?.status === "error" && receipt?.details?.error === "DeviceNotRegistered"
          ? devices[i]?.push_token
          : null,
      )
      .filter(Boolean);

    if (deadTokens.length) {
      await conn.query(
        `DELETE FROM user_devices WHERE push_token IN (${deadTokens.map(() => "?").join(",")})`,
        deadTokens,
      ).catch((err) => {
        console.warn(`[ExpoPush] Failed to prune dead tokens:`, err.message);
      });
    }
  } catch (err) {
    console.warn(`[ExpoPush] Failed for user ${userId}:`, err.message);
  }
}

const DEFAULT_CRON_SCHEDULE = "* * * * *"; // every minute default
const DEFAULT_CLIENT_DB_PREFIX = "saleserp_";

function getLocalIsoString(dateObj = new Date()) {
  const tz = runtimeState.activeTimezone || "UTC";
  try {
    const formatter = new Intl.DateTimeFormat("sv-SE", {
      timeZone: tz,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
      timeZoneName: "longOffset",
    });
    const parts = formatter.formatToParts(dateObj);
    const get = (type) => parts.find((p) => p.type === type)?.value;

    let offset = get("timeZoneName");
    if (!offset || offset === "GMT") {
      offset = "Z";
    } else if (offset.startsWith("GMT")) {
      offset = offset.replace("GMT", ""); // e.g., converts 'GMT+05:30' to '+05:30'
    } else {
      offset = "Z";
    }

    return `${get("year")}-${get("month")}-${get("day")}T${get("hour")}:${get("minute")}:${get("second")}${offset}`;
  } catch (e) {
    return dateObj.toISOString();
  }
}

function formatDbDate(date) {
  const d = new Date(date);
  if (isNaN(d.getTime())) return null;
  return getLocalIsoString(d).slice(0, 19).replace("T", " ");
}
const DEFAULT_MAX_PARALLEL_CLIENTS = 3;
const SCHEDULE_REFRESH_INTERVAL_MS = 30000;

// ─────────────────────────────────────────────
// Run-history log (data/recurring_jobs/<YYYY-MM-DD>.jsonl)
// One line per cron tick (job statuses + timing), one file per day — never
// rewritten once the day rolls over. Nothing about writing here talks to S3
// at all; log-sync.js (a separate, LOG_S3_SYNC-gated daily job) is what
// uploads yesterday-and-older day-files to S3 and deletes them locally,
// leaving today's file alone. See log-sync.js for that half.
// ─────────────────────────────────────────────

const LOG_FILE_DIR = process.env.LOG_FILE_DIR || path.join(__dirname, "data", "recurring_jobs");

function todayLogFilePath() {
  const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
  return path.join(LOG_FILE_DIR, `${today}.jsonl`);
}

async function appendRunLog(entry) {
  try {
    if (!fs.existsSync(LOG_FILE_DIR)) {
      fs.mkdirSync(LOG_FILE_DIR, { recursive: true });
    }
    fs.appendFileSync(todayLogFilePath(), JSON.stringify(entry) + "\n");
  } catch (err) {
    console.error("[CronLog] Failed to write run log:", err.message);
  }
}
const MAX_HISTORY = 50;
// Key names are unchanged from when this schedule only drove recurring-invoice
// processing — the daemon now uses the SAME schedule/timezone/max-parallel
// settings to drive deal task reminders, overdue/due-today notifications, and
// document-expiry checks instead. Renaming the master_settings keys would
// require a data migration, so the names stay as-is; only the admin-facing
// label was generalized (see CronScheduleSettings.jsx).
const MASTER_SETTING_CRON_SCHEDULE = "recurring_invoice_cron_schedule";
const MASTER_SETTING_CRON_TIMEZONE = "recurring_invoice_cron_timezone";
const MASTER_SETTING_MAX_PARALLEL_CLIENTS =
  "recurring_invoice_max_parallel_clients";

const runtimeState = {
  runInProgress: false,
  activeSchedule: null,
  activeTimezone: null,
  activeCronTask: null,
  scheduleRefreshInFlight: false,
  maxParallelClients: DEFAULT_MAX_PARALLEL_CLIENTS,
};

// Map<poolKey, { pool, lastUsed }> — lastUsed is refreshed on every
// getClientPool() call so cleanupStalePools() can close pools for clients
// that are no longer active AND, as a safety net, pools that simply haven't
// been touched in a while even if still nominally active.
const clientPools = new Map();
const POOL_STALE_TTL_MS = 60 * 60 * 1000; // 1 hour

// ─────────────────────────────────────────────
// Utility helpers
// ─────────────────────────────────────────────

function requiredEnv(name) {
  const value = process.env[name];
  if (!value || !String(value).trim()) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return String(value).trim();
}

function toPositiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function sanitizeSubdomain(subdomain) {
  return String(subdomain || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "");
}

/**
 * Processes `items` in sequential chunks of at most `chunkSize`, running
 * `handler` concurrently WITHIN each chunk via Promise.allSettled (so one
 * item's failure never aborts its siblings — same isolation guarantee the
 * call sites already relied on) and awaiting the whole chunk before starting
 * the next. This is what actually enforces a max-parallel-clients cap;
 * Promise.allSettled alone (the previous pattern) provides error isolation
 * but no concurrency limit at all, since it fires every item at once.
 *
 * Shared by every per-client job loop (recurring/notification jobs) so the
 * cap only needs to be implemented once.
 */
async function runInBatches(items, chunkSize, handler) {
  const size =
    Number.isFinite(chunkSize) && chunkSize > 0
      ? Math.floor(chunkSize)
      : DEFAULT_MAX_PARALLEL_CLIENTS;
  const results = [];
  for (let i = 0; i < items.length; i += size) {
    const chunk = items.slice(i, i + size);
    const chunkResults = await Promise.allSettled(chunk.map(handler));
    results.push(...chunkResults);
  }
  return results;
}

function normalizeMaybeJson(value, fallback, schemaCheck = null) {
  if (value == null || value === "") return fallback;
  if (typeof value !== "string") {
    if (schemaCheck && !schemaCheck(value)) return fallback;
    return value;
  }
  try {
    const parsed = JSON.parse(value);
    if (schemaCheck && !schemaCheck(parsed)) {
      console.warn("JSON schema check failed for value, using fallback");
      return fallback;
    }
    return parsed;
  } catch (err) {
    console.error("JSON parsing failed:", err.message);
    return fallback;
  }
}

function isTruthy(value) {
  return (
    value === true ||
    value === 1 ||
    value === "1" ||
    String(value || "").toLowerCase() === "true"
  );
}

function parseJsonArraySafe(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  if (typeof value !== "string") return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

// Resolves the last element of an assigned_to ownership-history array,
// falling back to created_by. The last element can be the '*' sentinel
// ("assigned to all" — see CLAUDE.md's ownership pattern), which is not a
// user id at all. Previously this was Number()-cast and filtered out with
// the rest of the array BEFORE picking the last element, so with e.g.
// assigned_to = [5, "*"] the filtered array became [5] and history[0]=5 was
// returned — a STALE prior owner, not the "everyone" intent '*' represents.
// Checking the raw last element for '*' first and falling back to
// created_by avoids silently misrouting to that stale id.
function resolveOwnerFromAssignedTo(assignedTo, createdBy) {
  const raw = parseJsonArraySafe(assignedTo);
  const last = raw[raw.length - 1];
  if (last !== undefined && last !== "*") {
    const n = Number(last);
    if (Number.isFinite(n)) return n;
  }
  const fallback = Number(createdBy);
  return Number.isFinite(fallback) ? fallback : null;
}

function getDealOwnerUserId(deal) {
  return resolveOwnerFromAssignedTo(deal?.assigned_to, deal?.created_by);
}

// Same ownership resolution as getDealOwnerUserId, generalized for any
// table with that same convention (customer/supplier included) — the
// doc-expiry job notifies this owner.
function getRecordOwnerUserId(row) {
  return resolveOwnerFromAssignedTo(row?.assigned_to, row?.created_by);
}

// A deal's real "who's working this" list is assigned_users (JSON array of
// user ids, collaborators) — separate from assigned_to (ownership transfer
// HISTORY, whose last element is just the current single owner). Deal-level
// notifications (expiring soon / overdue) previously only ever notified the
// single resolved owner, silently never reaching any other assignee on a
// multi-assignee deal.
function getDealRecipientUserIds(deal) {
  const recipients = new Set();
  const owner = getDealOwnerUserId(deal);
  if (owner) recipients.add(owner);
  parseJsonArraySafe(deal?.assigned_users)
    .map(Number)
    .filter(Number.isFinite)
    .forEach((id) => recipients.add(id));
  return [...recipients];
}

function slugifyDocName(name) {
  return String(name || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "") || "doc";
}

async function upsertNotification(conn, payload) {
  const {
    type,
    category = "deals",
    entityType,
    entityId,
    parentEntityType = null,
    parentEntityId = null,
    userId,
    title,
    message = null,
    meta = {},
    groupKey,
    status = "unread",
  } = payload;

  if (!userId || !groupKey || !type || !entityType || !entityId || !title) {
    return { created: false };
  }

  const [rows] = await conn.execute(
    "SELECT id FROM notifications WHERE user_id = ? AND group_key = ? LIMIT 1",
    [String(userId), groupKey],
  );

  if (rows.length > 0) {
    await conn.execute(
      `
        UPDATE notifications
        SET title = ?, message = ?, meta_json = ?, status = ?, read_at = NULL, updated_at = NOW()
        WHERE id = ?
      `,
      [title, message, JSON.stringify(meta || {}), status, rows[0].id],
    );
    return { created: false };
  }

  await conn.execute(
    `
      INSERT INTO notifications
      (id, type, category, entity_type, entity_id, parent_entity_type, parent_entity_id, user_id, title, message, meta_json, group_key, status, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
    `,
    [
      crypto.randomUUID(),
      type,
      category,
      entityType,
      String(entityId),
      parentEntityType,
      parentEntityId ? String(parentEntityId) : null,
      String(userId),
      title,
      message,
      JSON.stringify(meta || {}),
      groupKey,
      status,
    ],
  );
  return { created: true };
}

// ─────────────────────────────────────────────
// WhatsApp reminders (queued here, sent by the CRM web process)
// ─────────────────────────────────────────────
// This daemon is a separate process and cannot reach the live WhatsApp
// connection, which lives inside the CRM web server. So a CRM alert is written
// to the tenant's `whatsapp_outbox` table; the web process drains it. Called
// ONLY when upsertNotification just CREATED the in-app row, so each alert is
// queued once (plus a UNIQUE dedupe_key as a second guard).

const WA_PLAN_TTL_MS = 60 * 1000;
const WA_MAX_AGE_HOURS = 2; // a reminder still unsent this long AFTER it was due is dropped, never sent late
const waPlanCache = new Map(); // plan_id -> { on, at }
let waOutboxWarned = false;

// When CRM reminders reach WhatsApp. The in-app/push/email alerts still fire at the
// first run of each day; only the WhatsApp message is HELD until this time of day
// (cron timezone). Master -> Settings -> Cron Schedule -> "WhatsApp reminder time".
//   never saved -> 08:00   |   saved blank -> send as soon as created   |   "HH:mm" -> that time
const WA_TIME_KEY = "whatsapp_reminder_time";
const WA_DEFAULT_TIME = "08:00";
const WA_TIME_RE = /^([01]\d|2[0-3]):[0-5]\d$/;
let waTimeCache = { at: 0, value: WA_DEFAULT_TIME };

async function getWhatsappSendTime() {
  if (Date.now() - waTimeCache.at < WA_PLAN_TTL_MS) return waTimeCache.value;
  let value = WA_DEFAULT_TIME;
  try {
    const [rows] = await masterPool.query(
      "SELECT `value` FROM master_settings WHERE `key` = ? LIMIT 1",
      [WA_TIME_KEY],
    );
    if (rows.length) {
      const v = String(rows[0].value ?? "").trim();
      if (v === "") value = null;            // blank = immediate
      else if (WA_TIME_RE.test(v)) value = v; // valid HH:mm
      // anything else: keep the default rather than guess
    }
  } catch (err) {
    console.error("[WhatsApp] send-time lookup failed:", err.message);
  }
  waTimeCache = { at: Date.now(), value };
  return value;
}

// Whole minutes from now until the next HH:mm today in the cron timezone; 0 if that time
// has already passed today (a reminder due today is sent now, never pushed to tomorrow).
function minutesUntilToday(hhmm, now = new Date()) {
  if (!hhmm) return 0;
  const [H, M, S] = getLocalIsoString(now).slice(11, 19).split(":").map(Number);
  const nowMin = H * 60 + M + S / 60;
  const [th, tm] = hhmm.split(":").map(Number);
  const target = th * 60 + tm;
  return target > nowMin ? Math.ceil(target - nowMin) : 0;
}

async function isWhatsappPlanOn(client) {
  const planId = client?.plan_id;
  if (!planId) return false;
  const hit = waPlanCache.get(planId);
  if (hit && Date.now() - hit.at < WA_PLAN_TTL_MS) return hit.on;
  let on = false;
  try {
    const [rows] = await masterPool.query(
      "SELECT status, plans FROM plans WHERE id = ? LIMIT 1",
      [planId],
    );
    const plan = rows[0];
    if (plan && plan.status === "active") {
      let flags = plan.plans;
      if (typeof flags === "string") {
        try { flags = JSON.parse(flags); } catch { flags = null; }
      }
      on = Boolean(flags && flags.whatsapp_notifications === true); // strict opt-in
    }
  } catch (err) {
    console.error("[WhatsApp] plan lookup failed:", err.message);
  }
  waPlanCache.set(planId, { on, at: Date.now() });
  return on;
}

// International digits only (e.g. "+971 50 123 4567" -> "971501234567"); null if
// it cannot be a valid international number. Mirrors src/app/utils/whatsappNumber.js.
function normalizeWhatsappNumber(raw) {
  if (raw === null || raw === undefined) return null;
  let v = String(raw).trim().replace(/[\s\-().]/g, "");
  if (!v) return null;
  if (v.startsWith("+")) v = v.slice(1);
  else if (v.startsWith("00")) v = v.slice(2);
  if (!/^\d+$/.test(v) || v.startsWith("0")) return null;
  if (v.length < 8 || v.length > 15) return null;
  return v;
}

async function queueWhatsappReminder(conn, client, { userId, groupKey, emoji, title, message, context, href }) {
  try {
    if (!(await isWhatsappPlanOn(client))) return;

    // Only users who actually linked WhatsApp, and turned reminders ON.
    const [users] = await conn.execute(
      `SELECT u.mobile_number, u.whatsapp_reminders_enabled, u.whatsapp_reminder_number
         FROM \`user\` u
         JOIN whatsapp_instances wi ON wi.user_id = u.user_id
        WHERE u.user_id = ? LIMIT 1`,
      [userId],
    );
    const u = users[0];
    if (!u || Number(u.whatsapp_reminders_enabled) !== 1) return;

    const to = normalizeWhatsappNumber(u.whatsapp_reminder_number) || normalizeWhatsappNumber(u.mobile_number);
    if (!to) return;

    let link = "";
    try { link = href ? `${getClientBaseUrl(client.subdomain)}${href}` : ""; } catch { link = ""; }
    const body = [`${emoji} *${title}*`, message, context, link].filter(Boolean).join("\n");

    // Hold until the configured time of day (relative minutes, so DB-server timezone is irrelevant).
    const holdMinutes = minutesUntilToday(await getWhatsappSendTime());

    await conn.execute(
      `INSERT IGNORE INTO whatsapp_outbox (user_id, to_number, body, kind, dedupe_key, status, next_attempt_at)
       VALUES (?, ?, ?, 'reminder', ?, 'pending', NOW() + INTERVAL ? MINUTE)`,
      [userId, to, body, `rem:${groupKey}:${userId}`.slice(0, 190), holdMinutes],
    );
  } catch (err) {
    // Missing table/columns (migration not run yet) must never break the cron.
    if (!waOutboxWarned) {
      waOutboxWarned = true;
      console.error("[WhatsApp] could not queue reminder (is client_changes.sql entry 58 applied?):", err.message);
    }
  }
}

// Drop reminders that missed their send window (WhatsApp offline for hours): a stale
// "due today" arriving the next morning is worse than none. Measured from the time the
// message was DUE (next_attempt_at), not from creation, so a message deliberately held
// overnight until 08:00 is not expired before it is sent.
async function sweepWhatsappOutbox(conn) {
  try {
    await conn.execute(
      `UPDATE whatsapp_outbox SET status = 'expired', last_error = 'too old to send'
        WHERE status = 'pending' AND next_attempt_at < (NOW() - INTERVAL ${WA_MAX_AGE_HOURS} HOUR)`,
    );
  } catch (_) { /* table not migrated yet */ }
}

async function resolveDealsNotifications(conn, todayStr, expiringSoonDateStr) {
  await conn.execute(
    `
      UPDATE notifications n
      LEFT JOIN deals d
        ON n.entity_type = 'deal'
       AND n.entity_id = d.id
      SET n.status = 'resolved', n.updated_at = NOW()
      WHERE n.category = 'deals'
        AND n.status = 'unread'
        AND n.type = 'deal_expiring_soon'
        AND (
          d.id IS NULL
          OR LOWER(COALESCE(d.status, '')) <> 'open'
          OR d.expected_close_date IS NULL
          OR DATE(d.expected_close_date) < ?
          OR DATE(d.expected_close_date) > ?
        )
    `,
    [todayStr, expiringSoonDateStr],
  );

  await conn.execute(
    `
      UPDATE notifications n
      LEFT JOIN deals d
        ON n.entity_type = 'deal'
       AND n.entity_id = d.id
      SET n.status = 'resolved', n.updated_at = NOW()
      WHERE n.category = 'deals'
        AND n.status = 'unread'
        AND n.type = 'deal_overdue'
        AND (
          d.id IS NULL
          OR LOWER(COALESCE(d.status, '')) <> 'open'
          OR d.expected_close_date IS NULL
          OR DATE(d.expected_close_date) >= ?
        )
    `,
    [todayStr],
  );
}

// Auto-resolves stale task_reminder / task_due_today / task_overdue
// notifications created by processAllTaskReminders() below, for BOTH
// deal_task and lead_task entities. This used to live only in
// resolveDealsNotifications() above, matching the OLD type strings
// ('deal_task_reminder' etc.) from before deal/lead task reminders were
// unified into a single job — since processAllTaskReminders() now writes
// 'task_reminder'/'task_due_today'/'task_overdue' (per getNotificationType())
// and stamps lead task notifications with category:'leads', those old
// blocks never matched a single row: a completed/deleted/reassigned task's
// reminder notification stayed "unread" forever. Matches both the old and
// new type strings so any historical rows from before the migration still
// get cleaned up too, and covers lead_task/category:'leads', which the
// deals-only resolver never touched.
async function resolveTaskReminderNotifications(conn, todayStr) {
  await conn.execute(
    `
      UPDATE notifications n
      LEFT JOIN deal_tasks dt
        ON n.entity_type = 'deal_task'
       AND n.entity_id = dt.id
      SET n.status = 'resolved', n.updated_at = NOW()
      WHERE n.category = 'deals'
        AND n.status = 'unread'
        AND n.type IN ('task_reminder', 'deal_task_reminder')
        AND (
          dt.id IS NULL
          OR dt.status <> 'pending'
          OR DATE(dt.due_date) <= ?
        )
    `,
    [todayStr],
  );

  await conn.execute(
    `
      UPDATE notifications n
      LEFT JOIN deal_tasks dt
        ON n.entity_type = 'deal_task'
       AND n.entity_id = dt.id
      SET n.status = 'resolved', n.updated_at = NOW()
      WHERE n.category = 'deals'
        AND n.status = 'unread'
        AND n.type IN ('task_due_today', 'deal_task_due_today')
        AND (
          dt.id IS NULL
          OR dt.status <> 'pending'
          OR DATE(dt.due_date) <> ?
        )
    `,
    [todayStr],
  );

  await conn.execute(
    `
      UPDATE notifications n
      LEFT JOIN deal_tasks dt
        ON n.entity_type = 'deal_task'
       AND n.entity_id = dt.id
      SET n.status = 'resolved', n.updated_at = NOW()
      WHERE n.category = 'deals'
        AND n.status = 'unread'
        AND n.type IN ('task_overdue', 'deal_task_overdue')
        AND (
          dt.id IS NULL
          OR dt.status <> 'pending'
          OR DATE(dt.due_date) >= ?
        )
    `,
    [todayStr],
  );

  await conn.execute(
    `
      UPDATE notifications n
      LEFT JOIN lead_tasks lt
        ON n.entity_type = 'lead_task'
       AND n.entity_id = lt.id
      SET n.status = 'resolved', n.updated_at = NOW()
      WHERE n.category = 'leads'
        AND n.status = 'unread'
        AND n.type = 'task_reminder'
        AND (
          lt.id IS NULL
          OR lt.status <> 'pending'
          OR DATE(lt.due_date) <= ?
        )
    `,
    [todayStr],
  );

  await conn.execute(
    `
      UPDATE notifications n
      LEFT JOIN lead_tasks lt
        ON n.entity_type = 'lead_task'
       AND n.entity_id = lt.id
      SET n.status = 'resolved', n.updated_at = NOW()
      WHERE n.category = 'leads'
        AND n.status = 'unread'
        AND n.type = 'task_due_today'
        AND (
          lt.id IS NULL
          OR lt.status <> 'pending'
          OR DATE(lt.due_date) <> ?
        )
    `,
    [todayStr],
  );

  await conn.execute(
    `
      UPDATE notifications n
      LEFT JOIN lead_tasks lt
        ON n.entity_type = 'lead_task'
       AND n.entity_id = lt.id
      SET n.status = 'resolved', n.updated_at = NOW()
      WHERE n.category = 'leads'
        AND n.status = 'unread'
        AND n.type = 'task_overdue'
        AND (
          lt.id IS NULL
          OR lt.status <> 'pending'
          OR DATE(lt.due_date) >= ?
        )
    `,
    [todayStr],
  );
}

// ─────────────────────────────────────────────
// Document expiry notifications (customer_docs / supplier_docs)
//
// Unlike task reminders (discrete exact-day triggers), this is a continuous
// WINDOW: a document becomes notification-worthy the moment
// daysUntilExpiry <= max(configured doc_expiry_days), and keeps resurfacing
// every day (at most once/day) until it's renewed or removed — including
// already-expired documents, which should keep nagging until fixed.
//
// customer_docs/supplier_docs entries have no per-entry primary key, so the
// group_key uses doc.docId (stamped client-side on new uploads going
// forward) when present, falling back to a slug of docName (with a
// duplicate-name tiebreak) for older entries that predate docId.
// ─────────────────────────────────────────────

const DEFAULT_DOC_EXPIRY_WINDOW_DAYS = 30;

async function processDocumentExpiryNotifications() {
  let masterConnection;
  try {
    masterConnection = await masterPool.getConnection();
    const [clients] = await masterConnection.execute(
      "SELECT * FROM clients WHERE status = 'active'",
    );
    masterConnection.release();
    masterConnection = null;

    const todayStr = getLocalIsoString(new Date()).slice(0, 10);
    const today = new Date(todayStr);

    await runInBatches(
      clients.filter(shouldProcessClient),
      runtimeState.maxParallelClients,
      async (client) => {
        let conn;
        try {
          const cfg = getClientDbConfig(client);
          conn = await getClientPool(cfg).getConnection();

          const [nsRows] = await conn
            .execute(
              "SELECT doc_expiry_enabled, doc_expiry_days FROM notification_settings ORDER BY id DESC LIMIT 1",
            )
            .catch(() => [[]]);

          const enabled = nsRows?.[0]
            ? Number(nsRows[0].doc_expiry_enabled) !== 0
            : true;
          const configuredDays = parseJsonArraySafe(nsRows?.[0]?.doc_expiry_days)
            .map(Number)
            .filter(Number.isFinite);
          const windowDays = configuredDays.length
            ? Math.max(...configuredDays)
            : DEFAULT_DOC_EXPIRY_WINDOW_DAYS;

          // Every group_key still within the window this run — the resolver
          // uses this to clear anything that's fallen out of it (renewed,
          // removed, or the feature just got disabled).
          const validGroupKeys = new Set();

          if (enabled) {
            const entityConfigs = [
              { table: "customer", docsCol: "customer_docs", idCol: "customer_id", entityType: "customer_doc", notifType: "customer_doc_expiring" },
              { table: "supplier", docsCol: "supplier_docs", idCol: "supplier_id", entityType: "supplier_doc", notifType: "supplier_doc_expiring" },
            ];

            for (const { table, docsCol, idCol, entityType, notifType } of entityConfigs) {
              const [rows] = await conn.execute(
                `SELECT ${idCol} AS id, name, ${docsCol} AS docs, assigned_to, created_by
                 FROM \`${table}\` WHERE ${docsCol} IS NOT NULL`,
              );

              for (const row of rows) {
                const docs = parseJsonArraySafe(row.docs);
                if (!docs.length) continue;

                const ownerUserId = getRecordOwnerUserId(row);
                if (!ownerUserId) continue;

                const seenSlugs = new Map(); // dedup counter for same-name docs on this record

                for (const doc of docs) {
                  if (!doc?.docExpiryDate) continue;
                  // docExpiryDate is a plain "YYYY-MM-DD" string, which
                  // Date parses as UTC midnight — same basis as `today`
                  // above (parsed from todayStr the same way). The removed
                  // .setHours(0,0,0,0) call mutated this using the SERVER's
                  // LOCAL timezone, shifting expiry.getTime() by the local
                  // UTC offset and desyncing it from `today`'s pure-UTC
                  // basis — on any non-UTC server this rounded
                  // daysUntilExpiry to the wrong day near midnight
                  // boundaries.
                  const expiry = new Date(doc.docExpiryDate);
                  if (isNaN(expiry.getTime())) continue;

                  const daysUntilExpiry = Math.round(
                    (expiry.getTime() - today.getTime()) / 86400000,
                  );
                  if (daysUntilExpiry > windowDays) continue;

                  const baseSlug = slugifyDocName(doc.docName);
                  const dupIndex = seenSlugs.get(baseSlug) || 0;
                  seenSlugs.set(baseSlug, dupIndex + 1);
                  const slugKey = dupIndex > 0 ? `${baseSlug}-${dupIndex}` : baseSlug;

                  const groupKey = `docexpiry:${entityType}:${row.id}:${doc.docId || slugKey}`;
                  validGroupKeys.add(groupKey);

                  // Dedup — at most one upsert per calendar day per document,
                  // mirroring last_notified_at's guarantee for tasks, but
                  // there's no per-doc-entry timestamp column to gate on, so
                  // check the notification row's own updated_at instead.
                  const [existing] = await conn.execute(
                    "SELECT id, DATE(updated_at) AS updated_date FROM notifications WHERE user_id = ? AND group_key = ? LIMIT 1",
                    [String(ownerUserId), groupKey],
                  );
                  if (existing.length && existing[0].updated_date === todayStr) continue;

                  const isExpired = daysUntilExpiry < 0;
                  const docLabel = doc.docName || "Document";
                  const entityName = row.name || (table === "customer" ? "customer" : "supplier");

                  await upsertNotification(conn, {
                    type: notifType,
                    category: "documents",
                    entityType,
                    entityId: row.id,
                    userId: ownerUserId,
                    title: isExpired ? "Document expired" : "Document expiring soon",
                    message: isExpired
                      ? `${docLabel} for ${entityName} expired ${Math.abs(daysUntilExpiry)} day(s) ago`
                      : `${docLabel} for ${entityName} expires in ${daysUntilExpiry} day(s)`,
                    meta: {
                      docName: doc.docName || "",
                      docExpiryDate: doc.docExpiryDate,
                      daysUntilExpiry,
                      uploadedPath: doc.uploadedPath || "",
                      entityName,
                      href: `/${table}/view/${row.id}`,
                    },
                    groupKey,
                  });
                }
              }
            }
          }

          await resolveDocumentExpiryNotifications(conn, validGroupKeys);
        } catch (err) {
          console.error(
            `[DocExpiry] Error processing client ${client.subdomain}:`,
            err.message,
          );
        } finally {
          if (conn) conn.release();
        }
      },
    );
  } catch (err) {
    console.error("[DocExpiry] processDocumentExpiryNotifications error:", err.message);
  } finally {
    if (masterConnection) masterConnection.release();
  }
}

// ─────────────────────────────────────────────
// HR: employee passport / Emirates ID / visa expiry alerts
// ─────────────────────────────────────────────
// In-app only (no push / WhatsApp / email — these concern staff documents, not sales work).
// Runs only for tenants whose plan ticks HR (`employee` flag, strict opt-in, same rule as the web app).
// Recipients: superadmins + users whose role grants employee.view. One alert per document per user,
// refreshed at most once a day, auto-resolved when the date is renewed / the employee leaves.
const HR_EXPIRY_WINDOW_DAYS = 60; // visas need a longer lead time than the 30-day customer/supplier default
const HR_EXPIRY_FIELDS = [
  { col: "visa_expiry", label: "Residence visa" },
  { col: "emirates_id_expiry", label: "Emirates ID" },
  { col: "passport_expiry", label: "Passport" },
];
const hrPlanCache = new Map(); // plan_id -> { on, at }

async function isHrPlanOn(client) {
  const planId = client?.plan_id;
  if (!planId) return false;
  const hit = hrPlanCache.get(planId);
  if (hit && Date.now() - hit.at < WA_PLAN_TTL_MS) return hit.on;
  let on = false;
  try {
    const [rows] = await masterPool.query("SELECT status, plans FROM plans WHERE id = ? LIMIT 1", [planId]);
    const plan = rows[0];
    if (plan && plan.status === "active") {
      let flags = plan.plans;
      if (typeof flags === "string") {
        try { flags = JSON.parse(flags); } catch { flags = null; }
      }
      on = Boolean(flags && (flags.employee === true || flags.employee === 1)); // strict opt-in
    }
  } catch (err) {
    console.error("[HR expiry] plan lookup failed:", err.message);
  }
  hrPlanCache.set(planId, { on, at: Date.now() });
  return on;
}

// Users who may see employee data: superadmin, or a role whose privileges grant employee.view.
async function getEmployeeViewers(conn) {
  const [users] = await conn.execute(
    `SELECT u.user_id, u.role_id, u.status, r.privileges
       FROM \`user\` u LEFT JOIN role r ON CAST(r.role_id AS CHAR) = CAST(u.role_id AS CHAR)`,
  );
  const out = [];
  for (const u of users) {
    const st = parseJsonArraySafe(u.status)?.[0];
    if (st && Number(st.id) !== 12) continue; // inactive user
    const isSuper = Number(u.user_id) === 1 || String(u.role_id).toLowerCase() === "superadmin";
    let canView = isSuper;
    if (!canView) {
      try {
        const privs = typeof u.privileges === "string" ? JSON.parse(u.privileges || "{}") : u.privileges || {};
        canView = !!privs?.employee?.view;
      } catch { canView = false; }
    }
    if (canView) out.push(u.user_id);
  }
  return out;
}

async function processHrExpiryNotifications() {
  let masterConnection;
  try {
    masterConnection = await masterPool.getConnection();
    const [clients] = await masterConnection.execute("SELECT * FROM clients WHERE status = 'active'");
    masterConnection.release();
    masterConnection = null;

    const todayStr = getLocalIsoString(new Date()).slice(0, 10);
    const today = new Date(todayStr);
    const summary = { clients: 0, alerts: 0 };

    await runInBatches(
      clients.filter(shouldProcessClient),
      runtimeState.maxParallelClients,
      async (client) => {
        if (!(await isHrPlanOn(client))) return;
        let conn;
        try {
          const cfg = getClientDbConfig(client);
          conn = await getClientPool(cfg).getConnection();
          summary.clients += 1;

          const [nsRows] = await conn
            .execute("SELECT doc_expiry_enabled, doc_expiry_days FROM notification_settings ORDER BY id DESC LIMIT 1")
            .catch(() => [[]]);
          const enabled = nsRows?.[0] ? Number(nsRows[0].doc_expiry_enabled) !== 0 : true;
          const configured = parseJsonArraySafe(nsRows?.[0]?.doc_expiry_days).map(Number).filter(Number.isFinite);
          const windowDays = Math.max(HR_EXPIRY_WINDOW_DAYS, ...(configured.length ? configured : [0]));

          const validGroupKeys = new Set();
          if (enabled) {
            const [employees] = await conn
              .execute(
                `SELECT employee_id, full_name, employee_number, ${HR_EXPIRY_FIELDS.map((f) => f.col).join(", ")}
                   FROM employee
                  WHERE (CASE WHEN JSON_VALID(status) THEN JSON_UNQUOTE(JSON_EXTRACT(status, '$[0].key')) = 'active' ELSE 0 END)
                    AND (last_working_date IS NULL OR last_working_date >= CURDATE())`,
              )
              .catch((e) => {
                if (e?.code === "ER_NO_SUCH_TABLE") return [[]]; // HR not migrated for this tenant yet
                throw e;
              });

            if (employees.length) {
              const viewers = await getEmployeeViewers(conn);
              for (const emp of employees) {
                for (const { col, label } of HR_EXPIRY_FIELDS) {
                  if (!emp[col]) continue;
                  const d = emp[col] instanceof Date ? getLocalIsoString(emp[col]).slice(0, 10) : String(emp[col]).slice(0, 10);
                  const expiry = new Date(d);
                  if (Number.isNaN(expiry.getTime())) continue;
                  const daysUntil = Math.round((expiry.getTime() - today.getTime()) / 86400000);
                  if (daysUntil > windowDays) continue;

                  const groupKey = `hrexpiry:${emp.employee_id}:${col}`;
                  validGroupKeys.add(groupKey);
                  const who = emp.employee_number ? `${emp.full_name} (${emp.employee_number})` : emp.full_name;
                  const expired = daysUntil < 0;
                  for (const userId of viewers) {
                    const [existing] = await conn.execute(
                      "SELECT id, DATE(updated_at) AS updated_date FROM notifications WHERE user_id = ? AND group_key = ? LIMIT 1",
                      [String(userId), groupKey],
                    );
                    if (existing.length && String(existing[0].updated_date).slice(0, 10) === todayStr) continue;
                    const r = await upsertNotification(conn, {
                      type: "employee_doc_expiring",
                      category: "documents",
                      entityType: "employee_doc",
                      entityId: emp.employee_id,
                      userId,
                      title: expired ? "Employee document expired" : "Employee document expiring soon",
                      message: expired
                        ? `${label} of ${who} expired ${Math.abs(daysUntil)} day(s) ago (${d})`
                        : `${label} of ${who} expires in ${daysUntil} day(s) (${d})`,
                      meta: { docName: label, docExpiryDate: d, daysUntilExpiry: daysUntil, entityName: emp.full_name, href: `/employee/view/${emp.employee_id}` },
                      groupKey,
                    });
                    if (r.created) summary.alerts += 1;
                  }
                }
              }
            }
          }

          // clear alerts for renewed dates, leavers, or when the feature was switched off
          const [open] = await conn.execute(
            "SELECT id, group_key FROM notifications WHERE type = 'employee_doc_expiring' AND status IN ('unread', 'read')",
          );
          const stale = open.filter((r) => !validGroupKeys.has(r.group_key)).map((r) => r.id);
          if (stale.length) {
            await conn.execute(
              `UPDATE notifications SET status = 'resolved', updated_at = NOW() WHERE id IN (${stale.map(() => "?").join(",")})`,
              stale,
            );
          }
        } catch (err) {
          console.error(`[HR expiry] Error processing client ${client.subdomain}:`, err.message);
        } finally {
          if (conn) conn.release();
        }
      },
    );
    return summary;
  } catch (err) {
    console.error("[HR expiry] processHrExpiryNotifications error:", err.message);
    throw err;
  } finally {
    if (masterConnection) masterConnection.release();
  }
}

async function resolveDocumentExpiryNotifications(conn, validGroupKeys) {
  const [rows] = await conn.execute(
    `SELECT id, group_key FROM notifications
     WHERE type IN ('customer_doc_expiring', 'supplier_doc_expiring')
       AND status IN ('unread', 'read')`,
  );

  const staleIds = rows
    .filter((r) => !validGroupKeys.has(r.group_key))
    .map((r) => r.id);

  if (!staleIds.length) return;

  const placeholders = staleIds.map(() => "?").join(",");
  await conn.execute(
    `UPDATE notifications SET status = 'resolved', updated_at = NOW() WHERE id IN (${placeholders})`,
    staleIds,
  );
}

/**
 * FIX #11 – Sanitize user-supplied strings before embedding in HTML templates
 * to prevent XSS injection via customer_name, invoice_number, etc.
 */
function escapeHtml(str) {
  return String(str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

/**
 * FIX #6 – Validate that a timezone string is a recognised IANA timezone.
 * Falls back to "UTC" and logs a warning when invalid.
 */
function resolveTimezone(tz) {
  const candidate = String(tz || "").trim();
  if (!candidate) return "UTC";
  try {
    // Intl.DateTimeFormat throws a RangeError for unknown timezones
    Intl.DateTimeFormat(undefined, { timeZone: candidate });
    return candidate;
  } catch {
    console.warn(
      `Invalid timezone "${candidate}" in master_settings – falling back to UTC`,
    );
    return "UTC";
  }
}
function shouldProcessClient(client) {
  if (!client || !client.subdomain) return false;

  const status = String(client.status || "")
    .trim()
    .toLowerCase();
  if (status && status !== "active") return false;

  if (
    Object.prototype.hasOwnProperty.call(client, "is_onboarded") &&
    client.is_onboarded != null &&
    !isTruthy(client.is_onboarded)
  ) {
    return false;
  }

  return true;
}

// ─────────────────────────────────────────────
// DB config / pool helpers
// ─────────────────────────────────────────────

function getClientDbConfig(client) {
  const subdomain = sanitizeSubdomain(client.subdomain);
  if (!subdomain) {
    throw new Error(
      "Cannot resolve client DB config without a valid subdomain",
    );
  }

  const defaultPrefix =
    process.env.TENANT_DB_NAME_PREFIX || DEFAULT_CLIENT_DB_PREFIX;

  return {
    host: client.tenant_db_host || requiredEnv("TENANT_DB_HOST"),
    port: toPositiveInt(
      client.tenant_db_port || process.env.TENANT_DB_PORT,
      3306,
    ),
    user: client.tenant_db_user || requiredEnv("TENANT_DB_USER"),
    password: client.tenant_db_password || requiredEnv("TENANT_DB_PASSWORD"),
    database:
      client.tenant_db_name ||
      `${defaultPrefix}${subdomain.replace(/[^a-z0-9]/g, "")}`,
  };
}

function getClientPoolKey(config) {
  return [config.host, config.port, config.user, config.database].join(":");
}

function getClientPool(config) {
  const key = getClientPoolKey(config);
  const existing = clientPools.get(key);
  if (existing) {
    existing.lastUsed = Date.now();
    return existing.pool;
  }

  const pool = mysql.createPool({
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    waitForConnections: true,
    connectionLimit: 4,
    queueLimit: 0,
    enableKeepAlive: true,
    dateStrings: true,
  });

  clientPools.set(key, { pool, lastUsed: Date.now() });
  return pool;
}

/**
 * Closes and removes connection pools for tenants that are no longer in the
 * current active-clients list (deactivated/deleted/un-onboarded), plus — as
 * a safety net — any pool that simply hasn't been used in POOL_STALE_TTL_MS
 * even though its client is still active (e.g. a client with zero due tasks
 * for a long stretch). Without this, clientPools grows for the lifetime of
 * the daemon process since nothing ever removed an entry once created.
 *
 * Called once per cron cycle from runJob(), after all per-tick jobs finish.
 */
async function cleanupStalePools() {
  if (!clientPools.size) return;

  const validKeys = new Set();
  try {
    const [clients] = await masterPool.query(
      "SELECT * FROM clients WHERE status = 'active'",
    );
    for (const client of clients.filter(shouldProcessClient)) {
      try {
        validKeys.add(getClientPoolKey(getClientDbConfig(client)));
      } catch {
        // Client has unresolvable DB config (e.g. bad subdomain) — nothing
        // to key against, just skip it for this pass.
      }
    }
  } catch (err) {
    console.error(
      "[PoolCleanup] Failed to load active clients, skipping cleanup this cycle:",
      err.message,
    );
    return;
  }

  const now = Date.now();
  const staleKeys = [];
  for (const [key, entry] of clientPools.entries()) {
    const isActive = validKeys.has(key);
    const isStale = now - (entry.lastUsed || 0) > POOL_STALE_TTL_MS;
    if (!isActive || isStale) {
      staleKeys.push(key);
    }
  }

  for (const key of staleKeys) {
    const entry = clientPools.get(key);
    clientPools.delete(key);
    try {
      await entry.pool.end();
    } catch (err) {
      console.warn(`[PoolCleanup] Failed to close pool for ${key}:`, err.message);
    }
  }

  if (staleKeys.length) {
    console.log(
      `[PoolCleanup] Closed ${staleKeys.length} stale pool(s); ${clientPools.size} active pool(s) remain.`,
    );
  }
}

const masterPool = mysql.createPool({
  host: requiredEnv("SAAS_DB_HOST"),
  port: toPositiveInt(process.env.SAAS_DB_PORT, 3306),
  user: requiredEnv("SAAS_DB_USER"),
  password: requiredEnv("SAAS_DB_PASSWORD"),
  database: requiredEnv("SAAS_DB_NAME"),
  waitForConnections: true,
  connectionLimit: 4,
  queueLimit: 0,
  enableKeepAlive: true,
  dateStrings: true,
});

function getClientBaseUrl(subdomain) {
  const cleanSubdomain = sanitizeSubdomain(subdomain);
  if (!cleanSubdomain) {
    throw new Error("Cannot build client URL without a valid subdomain");
  }

  const template = String(process.env.TENANT_APP_URL_TEMPLATE || "").trim();
  if (template) {
    if (!template.includes("{{subdomain}}")) {
      throw new Error(
        "TENANT_APP_URL_TEMPLATE must contain {{subdomain}} placeholder",
      );
    }
    const resolved = template.replace(/\{\{subdomain\}\}/g, cleanSubdomain);
    if (!/^https?:\/\//i.test(resolved)) {
      throw new Error(
        "TENANT_APP_URL_TEMPLATE must resolve to an absolute http/https URL",
      );
    }
    return resolved.replace(/\/+$/, "");
  }

  const protocol = String(process.env.TENANT_APP_PROTOCOL || "https").trim();
  const rootDomain = String(process.env.TENANT_APP_ROOT_DOMAIN || "").trim();

  if (!rootDomain) {
    throw new Error(
      "Set TENANT_APP_URL_TEMPLATE or TENANT_APP_ROOT_DOMAIN for client URLs",
    );
  }
  if (!/^[a-z]+$/i.test(protocol)) {
    throw new Error("TENANT_APP_PROTOCOL must be a valid URL protocol");
  }

  return `${protocol}://${cleanSubdomain}.${rootDomain}`.replace(/\/+$/, "");
}

// ─────────────────────────────────────────────
// Master DB – table / settings / log helpers
// ─────────────────────────────────────────────

async function ensureMasterTables() {
  // No master-DB tables currently need bootstrapping here — kept as a hook
  // for future master-DB schema setup, called once per run from runJob's
  // predecessors.
}

async function getMasterSettingsMap(connection, keys = []) {
  if (!keys.length) {
    const [rows] = await connection.execute(
      "SELECT `key`, `value` FROM master_settings",
    );
    return rows.reduce((acc, row) => {
      acc[row.key] = row.value;
      return acc;
    }, {});
  }

  const placeholders = keys.map(() => "?").join(", ");
  const [rows] = await connection.execute(
    `SELECT \`key\`, \`value\` FROM master_settings WHERE \`key\` IN (${placeholders})`,
    keys,
  );
  return rows.reduce((acc, row) => {
    acc[row.key] = row.value;
    return acc;
  }, {});
}

// ─────────────────────────────────────────────
// Tenant DB – settings helpers
// ─────────────────────────────────────────────

/**
 * FIX #5 – Return a safe default instead of throwing when email_settings is
 * absent so that invoices can still be created even if sending is impossible.
 * The caller decides whether to bail on auto-send.
 */
async function getEmailSettings(connection) {
  // Only an enabled configuration should auto-send. Verification is an
  // optional, ad-hoc sanity check (the "Verify" button) — not a precondition
  // for sending, so it is deliberately not part of this filter.
  const [rows] = await connection.execute(
    "SELECT * FROM email_settings WHERE active = 1 LIMIT 1",
  );
  const settings = rows?.[0];

  if (!settings) {
    console.warn(
      "No active email_settings row found – auto-send will be disabled for this tenant",
    );
    // Return a sentinel so callers know mail is not configured
    return null;
  }

  return settings;
}

async function getBrandSettings(connection) {
  const [rows] = await connection.execute(
    "SELECT * FROM brand_settings LIMIT 1",
  );
  const settings = rows?.[0] || {};
  settings.global_tax = normalizeMaybeJson(settings.global_tax, []);
  return settings;
}

// Standard STARTTLS-vs-implicit-TLS convention: secure only on port 465.
// Replaces the previous ad-hoc "encryption set && !=none && port!=587" rule
// (which incorrectly set secure:true on e.g. port 25) — mirrors the same
// reconciled heuristic used by the Next.js side's sendTenantEmail.js.
function cronSmtpSecure(port) {
  return String(port) === "465";
}

/**
 * Provider-aware send for the cron daemon — mirrors src/app/lib/sendTenantEmail.js
 * (a separate Node process can't import from the Next.js app). Branches on
 * mailCfg.provider: smtp/maileroo use nodemailer with a fixed shared
 * from-identity; brevo uses its REST API via native fetch. There is never a
 * logged-in user in cron context, so Brevo always sends as
 * mailCfg.default_from_email.
 */
async function sendTenantEmailCron(mailCfg, { toEmail, subject, html, attachments = [], fromName, brand }) {
  const displayName = fromName || mailCfg.fromName || brand?.company_name || "SalesERP";

  if (mailCfg.provider === "brevo") {
    if (!mailCfg.default_from_email) {
      throw new Error("No Default From Email configured for Brevo");
    }

    const res = await fetch("https://api.brevo.com/v3/smtp/email", {
      method: "POST",
      headers: {
        "api-key": mailCfg.api_key,
        "content-type": "application/json",
        accept: "application/json",
      },
      body: JSON.stringify({
        sender: { email: mailCfg.default_from_email, name: displayName },
        to: [{ email: toEmail }],
        subject,
        htmlContent: html,
        attachment: attachments.map((a) => ({
          name: a.filename,
          content: Buffer.isBuffer(a.content)
            ? a.content.toString("base64")
            : Buffer.from(a.content || "").toString("base64"),
        })),
      }),
    });

    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw new Error(body?.message || `Brevo API error (${res.status})`);
    }
    return;
  }

  // smtp / maileroo — Maileroo is SMTP-shaped, identical transport, the only
  // difference is the from-address is always the fixed account username
  // rather than a configurable fromEmail (enforced by the caller passing the
  // right value through `mailCfg.fromEmail`/`mailCfg.username` as today).
  const transporter = nodemailer.createTransport({
    host: mailCfg.host,
    port: Number(mailCfg.port) || 587,
    secure: cronSmtpSecure(mailCfg.port),
    auth: {
      user: mailCfg.username,
      pass: mailCfg.password,
    },
  });

  await transporter.sendMail({
    from: `"${displayName}" <${mailCfg.provider === "maileroo" ? mailCfg.username : (mailCfg.fromEmail || mailCfg.username)}>`,
    to: toEmail,
    subject,
    html,
    attachments,
  });
}

// ─────────────────────────────────────────────
// Unified Multi-Stage Task Reminders
// Covers both deal_tasks and lead_tasks.
//
// Reminder schedule (fires once per day max, via last_notified_at dedup):
//   +3 days before due  → in-app + email  ("Due in 3 days")
//   +1 day before due   → in-app + email  ("Due tomorrow")
//    0 (due today)      → in-app + email  ("Due today")
//   -1 day overdue      → in-app + email  ("Overdue - 1 day")
//   -2 days overdue     → in-app only     ("Overdue - 2 days")
//   -3 days overdue     → in-app only     ("Overdue - 3 days")
//   >3 days overdue     → set reminder_sent=1 (stop forever)
//
// reminder_sent semantics:
//   0 = actively reminding
//   1 = permanently stopped (>3 days overdue, or task completed/cancelled)
//
// last_notified_at deduplication: at most ONE reminder per calendar day
// per task, regardless of how often the cron fires.
// ─────────────────────────────────────────────

// Trigger days relative to due_date (DATEDIFF = due_date - today).
// The positive ("days before due") portion is now per-client configurable
// via notification_settings.task_reminder_days — these are only the
// fallback used when a tenant has no settings row yet. The always-on tail
// (due-today, 1/2/3-days-overdue) is never configurable, unchanged from the
// original design.
const DEFAULT_TASK_REMINDER_DAYS = [7, 1];
const OVERDUE_STOP_THRESHOLD = -3;               // stop after 3 days overdue

function getReminderLabel(daysUntilDue) {
  if (daysUntilDue === 1)  return { subject: "Due Tomorrow",   tag: "due_tomorrow",   color: "#f59e0b" };
  if (daysUntilDue === 0)  return { subject: "Due Today",      tag: "due_today",      color: "#ef4444" };
  if (daysUntilDue === -1) return { subject: "Overdue",        tag: "overdue",        color: "#dc2626" };
  if (daysUntilDue === -2) return { subject: "Overdue (2 days)", tag: "overdue",      color: "#dc2626" };
  if (daysUntilDue <= -3)  return { subject: "Overdue (3 days)", tag: "overdue",       color: "#dc2626" };
  // Any other positive "days before due" value — covers the configurable
  // set (e.g. 2, 7) that isn't one of the fixed 1/0/-1/-2/-3 stages above.
  return { subject: `Due in ${daysUntilDue} Days`, tag: `due_in_${daysUntilDue}_days`, color: "#6366f1" };
}

function getNotificationType(daysUntilDue) {
  if (daysUntilDue >= 1)  return "task_reminder";
  if (daysUntilDue === 0) return "task_due_today";
  return "task_overdue";
}

async function sendTaskReminderEmail({ emailSettings, brandSettings, task, daysUntilDue, recipientEmail, recipientName, taskContext, baseUrl }) {
  if (!emailSettings || !recipientEmail) return false;
  const { subject: stageLabel, color } = getReminderLabel(daysUntilDue);
  const dueDateFormatted = new Date(task.due_date).toLocaleDateString("en-US", {
    weekday: "long", year: "numeric", month: "long", day: "numeric",
  });

  // Previously this sentence was hardcoded to "due tomorrow" in the template
  // regardless of the actual stage — a task 7 days out, due today, or 2 days
  // overdue all got the same false "due tomorrow" body text while only the
  // subject/heading reflected the real stage.
  const reminderSentence =
    daysUntilDue >= 2
      ? `This is a reminder that the following task is due in ${daysUntilDue} days.`
      : daysUntilDue === 1
      ? "This is a reminder that the following task is due tomorrow."
      : daysUntilDue === 0
      ? "This is a reminder that the following task is due today."
      : `This is a reminder that the following task is overdue by ${Math.abs(daysUntilDue)} day${Math.abs(daysUntilDue) > 1 ? "s" : ""}.`;
  const contextLine = taskContext.deal_name
    ? `Deal: <strong>${escapeHtml(taskContext.deal_name)}</strong>`
    : taskContext.lead_name
    ? `Lead: <strong>${escapeHtml(taskContext.lead_name)}</strong>`
    : "";

  const taskBody = `
    <table cellpadding="0" cellspacing="0" style="width:100%;font-size:14px;color:#4b5563;">
      <tr><td style="padding:8px 0;width:140px;"><strong>Task</strong></td><td style="padding:8px 0;">${escapeHtml(task.title)}</td></tr>
      ${contextLine ? `<tr><td style="padding:8px 0;"><strong>${taskContext.deal_name ? "Deal" : "Lead"}</strong></td><td style="padding:8px 0;">${contextLine}</td></tr>` : ""}
      <tr><td style="padding:8px 0;"><strong>Task Type</strong></td><td style="padding:8px 0;">${escapeHtml(task.task_type || "To Do")}</td></tr>
      <tr><td style="padding:8px 0;"><strong>Priority</strong></td><td style="padding:8px 0;">${escapeHtml(task.priority || "Normal")}</td></tr>
      <tr><td style="padding:8px 0;"><strong>Due Date</strong></td><td style="padding:8px 0;color:${color};font-weight:600;">${dueDateFormatted}${daysUntilDue < 0 ? " (Overdue)" : ""}</td></tr>
    </table>`;

  const html = TaskMailTemplate(
    taskBody, brandSettings,
    stageLabel.toUpperCase(),
    { user: { email_signature: "" } },
    baseUrl,
    { customerName: recipientName || "Team Member", userName: brandSettings.company_name || "Team", reminderSentence },
  );

  try {
    await sendTenantEmailCron(emailSettings, {
      toEmail: recipientEmail,
      subject: `${stageLabel}: ${escapeHtml(task.title)}`,
      html,
      brand: brandSettings,
    });
    return true;
  } catch (err) {
    console.warn(`[TaskReminder] Email failed for task ${task.id} → ${recipientEmail}:`, err.message);
    return false;
  }
}

async function processAllTaskReminders() {
  const summary = { sent: 0, skipped: 0, errors: 0 };
  let masterConnection;
  try {
    masterConnection = await masterPool.getConnection();
    const [clients] = await masterConnection.execute(
      "SELECT * FROM clients WHERE status = 'active'",
    );
    masterConnection.release();
    masterConnection = null;

    const todayStr = getLocalIsoString(new Date()).slice(0, 10);
    console.log(`[TaskReminder] Running for ${todayStr}`);

    await runInBatches(
      clients.filter(shouldProcessClient),
      runtimeState.maxParallelClients,
      async (client) => {
        let conn;
        try {
          const cfg = getClientDbConfig(client);
          conn = await getClientPool(cfg).getConnection();

          await sweepWhatsappOutbox(conn);

          // ── 0. Per-client configurable "days before due" ─────────────────────
          // Only the positive before-due offsets are configurable; the
          // always-on tail (due-today, 1/2/3-days-overdue) never changes.
          const [nsRows] = await conn.execute(
            "SELECT task_reminder_email_enabled, task_reminder_days FROM notification_settings ORDER BY id DESC LIMIT 1",
          ).catch(() => [[]]);
          const cfgDaysBefore = (
            parseJsonArraySafe(nsRows?.[0]?.task_reminder_days).length
              ? parseJsonArraySafe(nsRows?.[0]?.task_reminder_days)
              : DEFAULT_TASK_REMINDER_DAYS
          )
            .map(Number)
            .filter((d) => Number.isFinite(d) && d > 0);
          const emailEnabled = nsRows?.[0]?.task_reminder_email_enabled ?? 1;

          const reminderDays = new Set([...cfgDaysBefore, 0, -1, -2, -3]);
          const emailDays = new Set(
            Number(emailEnabled) === 0 ? [0, -1] : [...cfgDaysBefore, 0, -1],
          );
          const dayList = [...reminderDays];
          const inClause = dayList.map(() => "?").join(",");

          // ── 0b. Auto-resolve stale task reminder notifications ───────────────
          await resolveTaskReminderNotifications(conn, todayStr);

          // ── 1. Stop reminders for tasks that have been overdue too long ──────
          // Uses the tenant-timezone-aware todayStr (not SQL CURDATE(), which
          // follows the DB server's own timezone) so this agrees with the
          // due-today/overdue classification computed below and in
          // processDealTasksReminders().
          await conn.execute(
            `UPDATE deal_tasks SET reminder_sent = 1
             WHERE status = 'pending' AND reminder_enabled = 1 AND reminder_sent = 0
               AND DATEDIFF(?, DATE(due_date)) > ?`,
            [todayStr, Math.abs(OVERDUE_STOP_THRESHOLD)],
          ).catch(() => {});

          await conn.execute(
            `UPDATE lead_tasks SET reminder_sent = 1
             WHERE status = 'pending' AND reminder_enabled = 1 AND reminder_sent = 0
               AND DATEDIFF(?, DATE(due_date)) > ?`,
            [todayStr, Math.abs(OVERDUE_STOP_THRESHOLD)],
          ).catch(() => {});

          // ── 2. Fetch eligible deal_tasks ─────────────────────────────────────
          const [dealTasks] = await conn.execute(
            `SELECT
               dt.id, dt.deal_id, dt.title, dt.due_date, dt.assigned_to,
               dt.priority, dt.task_type,
               DATEDIFF(DATE(dt.due_date), ?) AS days_until_due,
               d.deal_name,
               d.created_by AS deal_created_by,
               d.assigned_to AS deal_assigned_to,
               c.name AS customer_name
             FROM deal_tasks dt
             JOIN deals d ON d.id = dt.deal_id
             LEFT JOIN customer c ON c.customer_id = d.customer_id
             WHERE dt.status = 'pending'
               AND dt.reminder_enabled = 1
               AND dt.reminder_sent = 0
               AND (dt.last_notified_at IS NULL OR DATE(dt.last_notified_at) < ?)
               AND DATEDIFF(DATE(dt.due_date), ?) IN (${inClause})
             LIMIT 300`,
            [todayStr, todayStr, todayStr, ...dayList],
          );

          // ── 3. Fetch eligible lead_tasks ─────────────────────────────────────
          const [leadTasks] = await conn.execute(
            `SELECT
               lt.id, lt.lead_id, lt.title, lt.due_date, lt.created_by,
               lt.priority, lt.task_type,
               DATEDIFF(DATE(lt.due_date), ?) AS days_until_due,
               l.name AS lead_name,
               l.assigned_to AS lead_assigned_to
             FROM lead_tasks lt
             JOIN \`lead\` l ON l.lead_id = lt.lead_id
             WHERE lt.status = 'pending'
               AND lt.reminder_enabled = 1
               AND lt.reminder_sent = 0
               AND (lt.last_notified_at IS NULL OR DATE(lt.last_notified_at) < ?)
               AND DATEDIFF(DATE(lt.due_date), ?) IN (${inClause})
             LIMIT 300`,
            [todayStr, todayStr, todayStr, ...dayList],
          ).catch(() => [[]]);

          if (!dealTasks.length && !leadTasks.length) return;

          const emailSettings = await getEmailSettings(conn);
          const brandSettings = await getBrandSettings(conn);
          const baseUrl = getClientBaseUrl(client.subdomain);

          // Batch-fetch all recipient users to avoid N+1 queries
          const allRecipientIds = new Set();
          for (const t of dealTasks) {
            const uid = resolveOwnerFromAssignedTo(t.assigned_to, t.deal_created_by);
            if (uid) allRecipientIds.add(uid);
          }
          for (const t of leadTasks) {
            // Was always t.created_by (whoever created the TASK) — lead_tasks
            // has no assigned_to column of its own, so the only sensible
            // "current owner" is the LEAD's own assigned_to (lead_assigned_to,
            // already selected above but previously never read), falling back
            // to created_by exactly like every other ownership resolution in
            // this file.
            const uid = resolveOwnerFromAssignedTo(t.lead_assigned_to, t.created_by);
            if (uid) allRecipientIds.add(uid);
          }
          const userMap = new Map();
          if (allRecipientIds.size > 0) {
            const idList = [...allRecipientIds];
            const placeholders = idList.map(() => '?').join(',');
            const [userRows] = await conn.execute(
              `SELECT user_id, email, full_name FROM user WHERE user_id IN (${placeholders})`,
              idList,
            );
            for (const u of userRows) userMap.set(u.user_id, u);
          }

          // ── Helper: process one task ─────────────────────────────────────────
          const handleTask = async (task, kind /* "deal" | "lead" */) => {
            const days = Number(task.days_until_due);
            if (!reminderDays.has(days)) return;

            // Resolve recipient user ID
            const recipientUserId = kind === "deal"
              ? resolveOwnerFromAssignedTo(task.assigned_to, task.deal_created_by)
              : resolveOwnerFromAssignedTo(task.lead_assigned_to, task.created_by);
            if (!recipientUserId) return;

            // Look up recipient user from pre-fetched map
            const user = userMap.get(recipientUserId);
            if (!user) return;

            const notifType    = getNotificationType(days);
            const { tag }      = getReminderLabel(days);
            const sendEmail    = emailDays.has(days);
            // "overdue" spans multiple calendar days (-1 to -3) before
            // reminder_sent stops it — a groupKey embedding todayStr there
            // created a brand-new unread notification row every day instead
            // of updating the same one in place, so a single overdue task
            // piled up to 3 separate unread rows. The forward-looking
            // stages (due_in_N_days/due_tomorrow/due_today) are each
            // naturally one-shot per task (DATEDIFF only equals that exact
            // offset on one calendar day), so todayStr there is harmless
            // and unchanged.
            const entityKey    = kind === "deal" ? `deal-task:${task.id}` : `lead-task:${task.id}`;
            const groupKey     = tag === "overdue" ? `${tag}:${entityKey}` : `${tag}:${entityKey}:${todayStr}`;

            const message = days >= 1
              ? `Task due in ${days} day${days > 1 ? "s" : ""}`
              : days === 0
              ? "Task is due today"
              : `Task overdue by ${Math.abs(days)} day${Math.abs(days) > 1 ? "s" : ""}`;

            // In-app notification
            const upResult = await upsertNotification(conn, {
              type:             notifType,
              category:         kind === "deal" ? "deals" : "leads",
              entityType:       kind === "deal" ? "deal_task" : "lead_task",
              entityId:         task.id,
              parentEntityType: kind === "deal" ? "deal" : "lead",
              parentEntityId:   kind === "deal" ? task.deal_id : task.lead_id,
              userId:           recipientUserId,
              title:            task.title,
              message,
              meta: {
                kind:          tag,
                deal_id:       task.deal_id   || null,
                lead_id:       task.lead_id   || null,
                deal_name:     task.deal_name  || null,
                lead_name:     task.lead_name  || null,
                customer_name: task.customer_name || "",
                due_date:      task.due_date,
                priority:      task.priority  || "Normal",
                task_type:     task.task_type || "To Do",
                days_until_due: days,
                href: kind === "deal" ? `/deals/${task.deal_id}` : `/lead/view/${task.lead_id}`,
              },
              groupKey,
            });

            // Expo push notification
            await sendExpoPushNotifications(conn, recipientUserId, task.title, message, {
              type: notifType,
              entityType: kind === "deal" ? "deal_task" : "lead_task",
              entityId: String(task.id),
              href: kind === "deal" ? `/deals/${task.deal_id}` : `/lead/view/${task.lead_id}`,
            });

            // WhatsApp (queued once, only when the in-app row was just created)
            if (upResult && upResult.created) {
              await queueWhatsappReminder(conn, client, {
                userId:  recipientUserId,
                groupKey,
                emoji:   days < 0 ? "⚠️" : "⏰",
                title:   task.title,
                message,
                context: [
                  kind === "deal" ? (task.deal_name && `Deal: ${task.deal_name}`) : (task.lead_name && `Lead: ${task.lead_name}`),
                  task.customer_name && `Customer: ${task.customer_name}`,
                ].filter(Boolean).join(" · "),
                href:    kind === "deal" ? `/deals/${task.deal_id}` : `/lead/view/${task.lead_id}`,
              });
            }

            // Email (only on selected stages). sendTaskReminderEmail resolves
            // to false on any send failure (missing settings, transport
            // error, etc.) — track that so the dedup stamp below is skipped
            // on failure, letting the next cron tick retry this SAME stage
            // instead of losing it once due_date's DATEDIFF rolls over to a
            // different stage tomorrow. Stages with no email requirement
            // default emailOk=true so their (unchanged) always-stamp
            // behavior is preserved exactly as before.
            let emailOk = true;
            if (sendEmail) {
              emailOk = await sendTaskReminderEmail({
                emailSettings, brandSettings,
                task, daysUntilDue: days,
                recipientEmail: user.email,
                recipientName:  user.full_name,
                taskContext: { deal_name: task.deal_name || null, lead_name: task.lead_name || null },
                baseUrl,
              });
            }

            // Stamp last_notified_at so this task won't fire again today —
            // but only once the required email (if any) actually went out.
            // Leaving it unstamped on failure is safe to retry: `days` (and
            // therefore the stage/groupKey) is recomputed fresh from
            // due_date vs. today on every tick, so re-running this same task
            // again re-targets the exact same stage (idempotent in-app
            // upsert, at-worst-redundant push) until the email succeeds or
            // the day rolls over and the task naturally moves to its next
            // stage.
            if (emailOk) {
              const table = kind === "deal" ? "deal_tasks" : "lead_tasks";
              await conn.execute(
                `UPDATE ${table} SET last_notified_at = NOW() WHERE id = ?`,
                [task.id],
              );
            }

            summary.sent++;
          };

          await Promise.allSettled(dealTasks.map((t) => handleTask(t, "deal")));
          await Promise.allSettled(leadTasks.map((t) => handleTask(t, "lead")));

        } catch (err) {
          summary.errors++;
          console.error(`[TaskReminder] Client ${client.subdomain} error:`, err.message);
        } finally {
          if (conn) conn.release();
        }
      },
    );

    console.log(`[TaskReminder] Done — sent=${summary.sent} errors=${summary.errors}`);
    return summary;
  } catch (err) {
    console.error("[TaskReminder] Fatal error:", err.message);
    return summary;
  } finally {
    if (masterConnection) masterConnection.release();
  }
}

// ─────────────────────────────────────────────
// Main job
// ─────────────────────────────────────────────

async function runJob() {
  const startedAt = new Date();
  console.log(
    "\x1b[1m\x1b[35m-----------------------------------------\x1b[0m",
  );
  console.log(
    `\x1b[1m\x1b[35mCRON START (Ver 4.2): ${startedAt.toLocaleString()}\x1b[0m`,
  );
  console.log(
    "\x1b[1m\x1b[35m-----------------------------------------\x1b[0m",
  );

  if (runtimeState.runInProgress) {
    console.warn("Previous scheduler run is still active; skipping");
    await appendRunLog({
      started_at: startedAt.toISOString(),
      finished_at: new Date().toISOString(),
      skipped: true,
      reason: "previous run still in progress",
    });
    return;
  }
  runtimeState.runInProgress = true;

  const jobs = [
    { name: "deal_task_reminders", run: processDealTasksReminders },
    { name: "task_reminders", run: processAllTaskReminders },
    { name: "document_expiry_notifications", run: processDocumentExpiryNotifications },
    { name: "hr_expiry_notifications", run: processHrExpiryNotifications },
  ];

  try {
    // ── BACKGROUND JOB DISPATCH ───────────────────────────────────────────
    // Each job is fully isolated. Add new jobs here; never mix logic between them.
    // Run concurrently but await all of them so runInProgress stays true for
    // the full duration of the run — otherwise the overlap guard above is a no-op.
    const settled = await Promise.allSettled(jobs.map((job) => job.run()));

    const jobResults = jobs.map((job, i) => {
      const result = settled[i];
      if (result.status === "fulfilled") {
        console.log(`[${job.name}] completed`);
        return { name: job.name, status: "success", summary: result.value || null };
      }
      console.error(`[${job.name}] Run failed:`, result.reason?.message);
      return { name: job.name, status: "error", error: result.reason?.message || String(result.reason) };
    });

    // Close/drop pools for clients no longer active (or simply idle too
    // long) — keeps clientPools from growing unbounded over the daemon's
    // lifetime. Isolated in its own try/catch via the function itself so a
    // failure here never marks the whole cron tick as failed.
    let poolCleanupStatus = { name: "pool_cleanup", status: "success" };
    try {
      await cleanupStalePools();
    } catch (err) {
      console.error("[PoolCleanup] Run failed:", err.message);
      poolCleanupStatus = { name: "pool_cleanup", status: "error", error: err.message };
    }

    const finishedAt = new Date();
    await appendRunLog({
      started_at: startedAt.toISOString(),
      finished_at: finishedAt.toISOString(),
      duration_ms: finishedAt.getTime() - startedAt.getTime(),
      jobs: [...jobResults, poolCleanupStatus],
    });
  } finally {
    runtimeState.runInProgress = false;
  }
}

// ─────────────────────────────────────────────
// Deal Task Reminders
// ─────────────────────────────────────────────

/**
 * Resolves stale deal-task notifications and fires deal expiry / overdue
 * notifications. Task reminders are handled by processAllTaskReminders().
 */
async function processDealTasksReminders() {
  let masterConnection;
  try {
    masterConnection = await masterPool.getConnection();
    const [clients] = await masterConnection.execute(
      "SELECT * FROM clients WHERE status = 'active'",
    );
    masterConnection.release();
    masterConnection = null;

    const localNowStr = getLocalIsoString(new Date());
    const todayStr = localNowStr.slice(0, 10);

    const expiringSoonDate = new Date(
      new Date(todayStr).getTime() + 7 * 86400000,
    );
    const expiringSoonDateStr = expiringSoonDate.toISOString().slice(0, 10);

    await runInBatches(
      clients.filter(shouldProcessClient),
      runtimeState.maxParallelClients,
      async (client) => {
        let conn;
        try {
          const cfg = getClientDbConfig(client);
          const pool = getClientPool(cfg);
          conn = await pool.getConnection();

          await resolveDealsNotifications(conn, todayStr, expiringSoonDateStr);

          // Task reminders (email + in-app) handled by processAllTaskReminders()

          // Dedup gate — at most once per calendar day per deal (mirrors the
          // deal_tasks/lead_tasks last_notified_at pattern above). Without
          // this, the default every-minute cron schedule sent a real Expo
          // push and reset the in-app notification back to unread roughly
          // once a MINUTE, indefinitely, for every qualifying deal.
          const [deals] = await conn.execute(
            `SELECT d.id, d.deal_name, d.customer_id, c.name as customer_name, d.expected_close_date, d.status, d.created_by, d.assigned_to, d.assigned_users, d.priority
             FROM deals d
             LEFT JOIN customer c ON c.customer_id = d.customer_id
             WHERE LOWER(COALESCE(d.status, '')) = 'open'
               AND d.expected_close_date IS NOT NULL
               AND DATE(d.expected_close_date) <= ?
               AND (d.last_notified_at IS NULL OR DATE(d.last_notified_at) < ?)`,
            [expiringSoonDateStr, todayStr],
          );

          for (const deal of deals) {
            // Notify every collaborator (assigned_users), not just the
            // single resolved owner — see getDealRecipientUserIds.
            const recipientUserIds = getDealRecipientUserIds(deal);
            if (!recipientUserIds.length) continue;

            const closeDateStr = new Date(deal.expected_close_date)
              .toISOString()
              .slice(0, 10);
            const isOverdue = closeDateStr < todayStr;
            // "deal_overdue" is open-ended (stays true every day until the
            // deal is closed/rescheduled) — a groupKey embedding todayStr
            // there created a new unread row every day instead of updating
            // the same one in place. "deal_expiring_soon" is naturally
            // stable already since closeDateStr doesn't change day to day.
            const groupKey = isOverdue
              ? `deal-overdue:${deal.id}`
              : `deal-expiring-soon:${deal.id}:${closeDateStr}`;

            for (const recipientUserId of recipientUserIds) {
              const dealUp = await upsertNotification(conn, {
                type: isOverdue ? "deal_overdue" : "deal_expiring_soon",
                entityType: "deal",
                entityId: deal.id,
                userId: recipientUserId,
                title: deal.deal_name,
                message: isOverdue
                  ? `Deal overdue for closing`
                  : `Deal closing soon`,
                meta: {
                  kind: isOverdue ? "deal_overdue" : "deal_expiring_soon",
                  deal_id: deal.id,
                  deal_name: deal.deal_name,
                  customer_name: deal.customer_name || "",
                  expected_close_date: deal.expected_close_date,
                  priority: deal.priority || "Normal",
                  href: `/deals/${deal.id}`,
                },
                groupKey,
              });

              if (dealUp && dealUp.created) {
                await queueWhatsappReminder(conn, client, {
                  userId:  recipientUserId,
                  groupKey,
                  emoji:   isOverdue ? "⚠️" : "📅",
                  title:   deal.deal_name,
                  message: isOverdue ? "Deal overdue for closing" : "Deal closing soon",
                  context: deal.customer_name ? `Customer: ${deal.customer_name}` : "",
                  href:    `/deals/${deal.id}`,
                });
              }

              await sendExpoPushNotifications(
                conn,
                recipientUserId,
                deal.deal_name,
                isOverdue ? "Deal overdue for closing" : "Deal closing soon",
                {
                  type: isOverdue ? "deal_overdue" : "deal_expiring_soon",
                  entityType: "deal",
                  entityId: String(deal.id),
                  href: `/deals/${deal.id}`,
                },
              );
            }

            // Stamp last_notified_at so this deal won't fire again today
            await conn.execute(
              `UPDATE deals SET last_notified_at = NOW() WHERE id = ?`,
              [deal.id],
            );
          }
        } catch (err) {
          console.error(
            `[DealTasks] Error processing client ${client.subdomain}:`,
            err.message,
          );
        } finally {
          if (conn) conn.release();
        }
      },
    );
  } catch (err) {
    console.error("[DealTasks] processDealTasksReminders error:", err.message);
  } finally {
    if (masterConnection) masterConnection.release();
  }
}

// ─────────────────────────────────────────────
// Scheduler bootstrap / refresh
// ─────────────────────────────────────────────

async function getSchedulerSettings(connection) {
  const settings = await getMasterSettingsMap(connection, [
    MASTER_SETTING_CRON_SCHEDULE,
    MASTER_SETTING_CRON_TIMEZONE,
    MASTER_SETTING_MAX_PARALLEL_CLIENTS,
  ]);

  // FIX #6 – Validate the timezone with resolveTimezone so bad values fall
  // back to UTC rather than crashing node-cron.
  return {
    schedule:
      String(settings[MASTER_SETTING_CRON_SCHEDULE] || "").trim() ||
      DEFAULT_CRON_SCHEDULE,
    timezone: resolveTimezone(settings[MASTER_SETTING_CRON_TIMEZONE]),
    maxParallelClients: toPositiveInt(
      settings[MASTER_SETTING_MAX_PARALLEL_CLIENTS],
      DEFAULT_MAX_PARALLEL_CLIENTS,
    ),
  };
}

async function refreshSchedule() {
  if (runtimeState.scheduleRefreshInFlight) return;
  runtimeState.scheduleRefreshInFlight = true;

  let connection;
  try {
    connection = await masterPool.getConnection();
    await ensureMasterTables();
    const settings = await getSchedulerSettings(connection);

    if (!cron.validate(settings.schedule)) {
      throw new Error(
        `Invalid ${MASTER_SETTING_CRON_SCHEDULE} value in master_settings: ${settings.schedule}`,
      );
    }

    // Keep this fresh independently of the schedule/timezone early-return
    // below — an admin can change just the max-parallel-clients setting
    // without touching the cron expression, and that must still take effect
    // on the next refresh tick rather than being skipped.
    runtimeState.maxParallelClients = settings.maxParallelClients;

    if (
      runtimeState.activeSchedule === settings.schedule &&
      runtimeState.activeTimezone === settings.timezone
    ) {
      return; // nothing changed – no need to restart the cron task
    }

    if (runtimeState.activeCronTask) {
      runtimeState.activeCronTask.stop();
      runtimeState.activeCronTask = null;
    }

    runtimeState.activeCronTask = cron.schedule(settings.schedule, runJob, {
      timezone: settings.timezone,
    });
    runtimeState.activeSchedule = settings.schedule;
    runtimeState.activeTimezone = settings.timezone;

    console.log(
      `Cron Scheduler started on schedule "${settings.schedule}" ` +
        `(${settings.timezone})`,
    );
  } catch (error) {
    console.error("Failed to refresh cron schedule from master DB:", error);
  } finally {
    // scheduleRefreshInFlight is always reset, even on error – no risk of
    // permanently locking out future refreshes (original code was correct here).
    if (connection) connection.release();
    runtimeState.scheduleRefreshInFlight = false;
  }
}

async function startScheduler() {
  await refreshSchedule();
  setInterval(refreshSchedule, SCHEDULE_REFRESH_INTERVAL_MS);
}

startScheduler().catch((error) => {
  console.error("Failed to start cron scheduler:", error);
  process.exit(1);
});

// Independent of the master-DB-configurable schedule above — a fixed daily
// tick at 2am server time. runLogSync() itself is a no-op unless
// LOG_S3_SYNC=true, so scheduling it here unconditionally is safe: nothing
// starts moving to S3 just because this line exists.
cron.schedule("0 2 * * *", () => {
  runLogSync(masterPool).catch((err) => console.error("[LogSync] Run failed:", err.message));
});
