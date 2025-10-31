/**
 * src/gitDataSync.js
 *
 * Safe data sync that:
 * - On initDataSync() tries a public raw download of the remote DB first (fast, no token).
 * - If the raw download succeeds, it writes local ./data/app.db and DOES NOT push.
 * - If raw download not available (private repo / network), falls back to GitHub API behavior.
 * - Will NOT push a blank local DB to remote.
 *
 * This file is a drop-in replacement for the previous gitDataSync implementation.
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const { octokit, GH_OWNER, GH_REPO, GH_BRANCH, configError } = require('./githubClient');

const LOCAL_DB_PATH = process.env.DATABASE_URL || './data/app.db';
const REMOTE_DB_PATH = 'data/app.db';
const REMOTE_RAW_URL = `https://raw.githubusercontent.com/${GH_OWNER}/${GH_REPO}/${GH_BRANCH}/${REMOTE_DB_PATH}`;

let lastKnownSha = null;
let dirty = false;
let timer = null;
const DEBOUNCE_MS = 3000;
const SIZE_WARN_LIMIT = 90 * 1024 * 1024; // 90MB

function downloadRawUrlToBuffer(url) {
  return new Promise((resolve, reject) => {
    const buffers = [];
    https.get(url, (res) => {
      if (res.statusCode >= 400) {
        // Not found or not accessible
        return reject(Object.assign(new Error(`Raw fetch failed: ${res.statusCode}`), { status: res.statusCode }));
      }
      res.on('data', (chunk) => buffers.push(chunk));
      res.on('end', () => resolve(Buffer.concat(buffers)));
    }).on('error', (err) => reject(err));
  });
}

async function getRemoteSha() {
  try {
    const { data } = await octokit.repos.getContent({
      owner: GH_OWNER,
      repo: GH_REPO,
      path: REMOTE_DB_PATH,
      ref: GH_BRANCH
    });
    if (Array.isArray(data)) throw new Error('Expected file, found directory: ' + REMOTE_DB_PATH);
    return data.sha || null;
  } catch (e) {
    if (e.status === 404) return null;
    throw e;
  }
}

async function fetchRemoteDbBufferViaApi() {
  const sha = await getRemoteSha();
  if (!sha) return null;
  const resp = await octokit.request('GET /repos/{owner}/{repo}/git/blobs/{file_sha}', {
    owner: GH_OWNER,
    repo: GH_REPO,
    file_sha: sha,
    headers: { accept: 'application/vnd.github.raw' }
  });
  const body = resp.data;
  if (Buffer.isBuffer(body)) return { buf: body, sha };
  if (typeof body === 'string') return { buf: Buffer.from(body, 'binary'), sha };
  if (body && body.content && body.encoding === 'base64') return { buf: Buffer.from(body.content, 'base64'), sha };
  throw new Error('Unable to fetch DB blob content via API');
}

async function putFileContents(contentBase64, message, sha = null) {
  const params = {
    owner: GH_OWNER,
    repo: GH_REPO,
    path: REMOTE_DB_PATH,
    message,
    content: contentBase64,
    branch: GH_BRANCH
  };
  if (sha) params.sha = sha;
  try {
    const { data } = await octokit.repos.createOrUpdateFileContents(params);
    return data;
  } catch (e) {
    if (e.status === 422 && e.message && e.message.includes(`"sha" wasn't supplied`)) {
      const remoteSha = await getRemoteSha();
      if (remoteSha) {
        params.sha = remoteSha;
        const { data } = await octokit.repos.createOrUpdateFileContents(params);
        return data;
      }
    }
    throw e;
  }
}

async function commitRawBlob(buffer, message) {
  const contentBase64 = buffer.toString('base64');
  const blob = await octokit.git.createBlob({
    owner: GH_OWNER,
    repo: GH_REPO,
    content: contentBase64,
    encoding: 'base64'
  });

  const ref = await octokit.git.getRef({ owner: GH_OWNER, repo: GH_REPO, ref: `heads/${GH_BRANCH}` });
  const baseCommitSha = ref.data.object.sha;
  const baseCommit = await octokit.git.getCommit({ owner: GH_OWNER, repo: GH_REPO, commit_sha: baseCommitSha });

  const tree = await octokit.git.createTree({
    owner: GH_OWNER,
    repo: GH_REPO,
    base_tree: baseCommit.data.tree.sha,
    tree: [
      {
        path: REMOTE_DB_PATH,
        mode: '100644',
        type: 'blob',
        sha: blob.data.sha
      }
    ]
  });

  const newCommit = await octokit.git.createCommit({
    owner: GH_OWNER,
    repo: GH_REPO,
    message,
    tree: tree.data.sha,
    parents: [baseCommitSha]
  });

  await octokit.git.updateRef({
    owner: GH_OWNER,
    repo: GH_REPO,
    ref: `heads/${GH_BRANCH}`,
    sha: newCommit.data.sha
  });

  return { commitSha: newCommit.data.sha, blobSha: blob.data.sha };
}

/**
 * initDataSync
 * - First: try to download the public raw URL (fast, no token). If it succeeds, use it and return.
 * - If public raw fails (404 or other), fall back to GitHub API approach (fetch remote via API).
 * - If remote missing:
 *     - If local exists and non-empty -> create remote from local (one-time).
 *     - If local missing or empty -> create empty local and DO NOT push.
 */
async function initDataSync() {
  const localDir = path.dirname(LOCAL_DB_PATH);
  if (!fs.existsSync(localDir)) fs.mkdirSync(localDir, { recursive: true });

  // 1) Try public raw download first (works for public repos)
  try {
    const buf = await downloadRawUrlToBuffer(REMOTE_RAW_URL);
    if (buf && buf.length > 0) {
      fs.writeFileSync(LOCAL_DB_PATH, buf);
      lastKnownSha = await getRemoteSha().catch(() => null);
      console.log('Pulled data/app.db via public raw URL.');
      // set up periodic flush and exit
      setupPeriodicAndShutdown();
      return;
    }
  } catch (e) {
    // raw fetch failed (private repo or network); fall through to API
    console.log('Public raw fetch failed, falling back to GitHub API: ', e.message || e);
  }

  // 2) Use GitHub API flow
  let remote = null;
  try {
    remote = await fetchRemoteDbBufferViaApi();
  } catch (e) {
    if (e.status && e.status !== 404) throw e;
  }

  if (remote && remote.buf) {
    fs.writeFileSync(LOCAL_DB_PATH, remote.buf);
    lastKnownSha = remote.sha;
    console.log('Pulled data/app.db via GitHub API (sha=%s).', lastKnownSha);
    setupPeriodicAndShutdown();
    return;
  }

  // remote missing
  if (fs.existsSync(LOCAL_DB_PATH) && fs.statSync(LOCAL_DB_PATH).size > 0) {
    // local non-empty -> create remote from local
    const buf = fs.readFileSync(LOCAL_DB_PATH);
    const size = buf.length;
    try {
      if (size <= SIZE_WARN_LIMIT) {
        const data = await putFileContents(buf.toString('base64'), 'Initialize data/app.db from local');
        lastKnownSha = data.content.sha || null;
        console.log('Created remote data/app.db from local (sha=%s).', lastKnownSha);
      } else {
        const { commitSha } = await commitRawBlob(buf, 'Initialize data/app.db from local (large file)');
        lastKnownSha = await getRemoteSha();
        console.log('Created remote data/app.db via raw commit (commit=%s).', commitSha);
      }
    } catch (e) {
      if (e.status === 404) {
        throw configError('Failed to create remote data/app.db: repository/branch not found or token lacking permission.');
      }
      throw e;
    }
  } else {
    // No remote and local empty -> create empty local and DO NOT push
    if (!fs.existsSync(LOCAL_DB_PATH)) {
      fs.writeFileSync(LOCAL_DB_PATH, Buffer.alloc(0));
      console.log('Created empty local data/app.db (remote not present). Not pushing blank DB.');
    } else {
      console.log('Remote missing and local DB empty; not creating remote to avoid overwriting repo.');
    }
    lastKnownSha = null;
  }

  setupPeriodicAndShutdown();
}

function setupPeriodicAndShutdown() {
  setInterval(() => {
    if (dirty) syncNow().catch(err => console.error('Periodic sync error:', err.message));
  }, 60 * 1000).unref();

  const shutdown = async () => {
    if (dirty) {
      try { await syncNow(true); } catch (e) { console.error('Final sync error:', e.message); }
    }
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

function markDataDirty() {
  dirty = true;
  if (timer) return;
  timer = setTimeout(() => {
    syncNow().catch(e => console.error('Sync error:', e.message));
    timer = null;
  }, DEBOUNCE_MS);
}

/**
 * syncNow(force)
 * Push local DB to remote safely. Uses contents API (with sha) and blob+commit fallback.
 */
async function syncNow(force = false) {
  if (!dirty && !force) return;
  if (!fs.existsSync(LOCAL_DB_PATH)) {
    console.warn('Local DB missing; skipping sync.');
    return;
  }

  const buf = fs.readFileSync(LOCAL_DB_PATH);
  const size = buf.length;
  const baseMessage = 'Sync data/app.db';

  const remoteSha = await getRemoteSha();

  if (remoteSha) {
    // update remote
    const contentBase64 = buf.toString('base64');
    try {
      const data = await putFileContents(contentBase64, baseMessage, remoteSha);
      lastKnownSha = data.content.sha || null;
      dirty = false;
      console.log('Updated remote data/app.db (sha=%s).', lastKnownSha);
      return;
    } catch (e) {
      console.warn('Contents API update failed, trying blob+commit fallback:', e.message || e);
    }

    // fallback
    try {
      const { commitSha } = await commitRawBlob(buf, baseMessage);
      lastKnownSha = await getRemoteSha();
      dirty = false;
      console.log('Updated remote data/app.db via blob commit (commit=%s).', commitSha);
      return;
    } catch (e2) {
      console.error('Blob commit fallback failed:', e2.message || e2);
      throw e2;
    }
  } else {
    // remote missing: only create if local non-empty
    if (size === 0) {
      console.warn('Local DB is empty and remote missing; refusing to push blank DB.');
      dirty = false;
      return;
    }
    const contentBase64 = buf.toString('base64');
    try {
      const data = await putFileContents(contentBase64, 'Create data/app.db from local');
      lastKnownSha = data.content.sha || null;
      dirty = false;
      console.log('Created remote data/app.db (sha=%s).', lastKnownSha);
      return;
    } catch (e) {
      console.warn('Contents API create failed, trying blob+commit:', e.message || e);
      try {
        const { commitSha } = await commitRawBlob(buf, 'Create data/app.db from local (blob commit)');
        lastKnownSha = await getRemoteSha();
        dirty = false;
        console.log('Created remote data/app.db via blob commit (commit=%s).', commitSha);
        return;
      } catch (e2) {
        console.error('Failed to create remote via blob commit:', e2.message || e2);
        throw e2;
      }
    }
  }
}

module.exports = {
  initDataSync,
  markDataDirty,
  syncNow
};

/*
Manual recovery (if you prefer a single command using the public raw URL):

mkdir -p data
curl -L -o ./data/app.db "https://raw.githubusercontent.com/Timmmy307/stu-private/main/data/app.db"
ls -lh data/app.db
*/
