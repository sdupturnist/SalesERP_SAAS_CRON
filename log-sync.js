// log-sync.js — the only place logs actually move to S3 in this app.
//
// Writing a log line never talks to S3 (see appendRunLog() in index.js, and
// logAiSearchToFile() in the main app's src/app/lib/aiSearchFileLog.js) —
// this is a separate, independently-scheduled daily job that walks each
// known log folder, uploads any completed period-file (a day-file dated
// before today, or a month-file dated before this month) to S3, deletes it
// locally, and leaves the current, still-being-appended-to file alone.
//
// Fully inert unless LOG_S3_SYNC=true — on local dev, and on any server
// where that env var isn't set, this is a no-op every time it "runs".
// Turning it on requires S3_BUCKET/S3_REGION (same vars the main app's
// storage driver uses) to also be set.

const fs = require("fs");
const path = require("path");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");

let cachedS3Client = null;
function getS3Client() {
  if (cachedS3Client) return cachedS3Client;
  const region = process.env.S3_REGION;
  if (!region) return null;

  const accessKeyId = process.env.S3_ACCESS_KEY_ID;
  const secretAccessKey = process.env.S3_SECRET_ACCESS_KEY;

  cachedS3Client = new S3Client({
    region,
    ...(accessKeyId && secretAccessKey ? { credentials: { accessKeyId, secretAccessKey } } : {}),
    ...(process.env.S3_ENDPOINT
      ? { endpoint: process.env.S3_ENDPOINT, forcePathStyle: true }
      : {}),
  });
  return cachedS3Client;
}

function getPrefix() {
  return process.env.S3_PREFIX || "SalesERP";
}

// Same default the main app's storagePaths.js uses — both processes need to
// agree on this root so log-sync.js finds the exact files the app wrote.
function getBaseStorageDir() {
  return process.env.BASE_STORAGE_DIR || "/Users/saleem/Desktop/WEBAPPFILES/SALESERP";
}

const DAY_FILE_PATTERN = /^(\d{4}-\d{2}-\d{2})\.jsonl$/;
const MONTH_FILE_PATTERN = /^(\d{4}-\d{2})\.jsonl$/;

// Each folder descriptor: where the day/month-files live locally, how to
// build the S3 key for a given filename, which pattern to match filenames
// against, and the "current period" string files must NOT be older than.
async function getLogFolders(masterPool) {
  const folders = [
    {
      label: "recurring_jobs",
      localDir: process.env.LOG_FILE_DIR || path.join(__dirname, "data", "recurring_jobs"),
      s3KeyFor: (filename) => `${getPrefix()}/master/cron_logs/${filename}`,
      pattern: DAY_FILE_PATTERN,
      currentPeriod: new Date().toISOString().slice(0, 10), // YYYY-MM-DD
    },
  ];

  // AI search audit logs — one folder per tenant (+ "master"), monthly
  // files, same layout as src/app/lib/aiSearchFileLog.js writes.
  const base = getBaseStorageDir();
  const currentMonth = new Date().toISOString().slice(0, 7); // YYYY-MM (UTC-ish; matches the file's own UTC month naming closely enough for a "has this month rolled over" check)
  const scopes = ["master"];

  try {
    const [clients] = await masterPool.query(
      "SELECT subdomain FROM clients WHERE status = 'active'",
    );
    for (const client of clients) {
      if (client?.subdomain) scopes.push(client.subdomain);
    }
  } catch (err) {
    console.error("[LogSync] Failed to load active clients for AISearch log sync:", err.message);
  }

  for (const scope of scopes) {
    folders.push({
      label: `${scope}/AISearch`,
      localDir: path.join(base, scope, "AISearch"),
      s3KeyFor: (filename) => `${getPrefix()}/${scope}/AISearch/${filename}`,
      pattern: MONTH_FILE_PATTERN,
      currentPeriod: currentMonth,
    });
  }

  return folders;
}

async function syncFolder(client, bucket, folder) {
  if (!fs.existsSync(folder.localDir)) return;

  const entries = fs.readdirSync(folder.localDir);

  for (const filename of entries) {
    const match = filename.match(folder.pattern);
    if (!match) continue; // not a period-file we recognize — leave it alone

    const filePeriod = match[1];
    if (filePeriod >= folder.currentPeriod) continue; // current day/month — still being written, leave it alone

    const fullPath = path.join(folder.localDir, filename);
    try {
      const body = fs.readFileSync(fullPath);
      await client.send(
        new PutObjectCommand({ Bucket: bucket, Key: folder.s3KeyFor(filename), Body: body })
      );
      fs.unlinkSync(fullPath);
      console.log(`[LogSync] Archived ${folder.label}/${filename} to S3 and removed local copy.`);
    } catch (err) {
      console.error(`[LogSync] Failed to sync ${folder.label}/${filename}:`, err.message);
      // Leave the local file in place — it'll be retried on the next run.
    }
  }
}

async function runLogSync(masterPool) {
  if (process.env.LOG_S3_SYNC !== "true") return; // master switch — off by default everywhere

  const bucket = process.env.S3_BUCKET;
  if (!bucket) {
    console.warn("[LogSync] LOG_S3_SYNC=true but S3_BUCKET is not set — skipping.");
    return;
  }

  const client = getS3Client();
  if (!client) {
    console.warn("[LogSync] LOG_S3_SYNC=true but S3_REGION is not set — skipping.");
    return;
  }

  for (const folder of await getLogFolders(masterPool)) {
    await syncFolder(client, bucket, folder);
  }
}

module.exports = { runLogSync };
