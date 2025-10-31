/**
 * src/gitDataSync.js
 *
 * Safe data sync logic.
 *
 * Behavior:
 * - On initDataSync():
 *   1) ALWAYS check the remote repo for data/app.db first.
 *   2) If remote file exists -> PULL it and WRITE local ./data/app.db (do NOT overwrite remote).
 *   3) If remote file DOES NOT exist:
 *        - If local DB exists AND is non-empty -> CREATE remote from local (one-time).
 *        - If no local DB or local is empty -> create an empty local DB and DO NOT push (avoid overwriting remote later).
 *
 * - markDataDirty(): schedule a background sync (debounced).
 * - syncNow(force): push local DB to remote (safe: fetch sha when needed, retry with sha on 422, fallback to blob+commit for large files).
 *
 * Safety rules implemented so a fresh server WITHOUT ./data will NOT push a blank DB and overwrite a remote copy.
 *
 * IMPORTANT:
 * - Replace the existing src/gitDataSync.js with this file.
 * - To recover lost local DB from remote immediately, use the manual curl command below (I also repeat it in comments).
 */

const fs = require('fs');
const path = require('path');
const { octokit, GH_OWNER, GH_REPO, GH_BRANCH, configError } = require('./githubClient');

const LOCAL_DB_PATH = process.env.DATABASE_URL || './data/app.db';
const REMOTE_DB_PATH = 'data/app.db';

let lastKnownSha = null;
let dirty = false;
let timer = null;

const DEBOUNCE_MS = 3000;
const SIZE_WARN_LIMIT = 90 * 1024 * 1024; // 90 MB

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

async function fetchRemoteDbBuffer() {
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
  throw new Error('Unable to fetch DB blob content');
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
    // Retry if GitHub complains about missing sha
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
 * initDataSync()
 *
 * Startup logic:
 *  - If remote exists -> pull and write local (always).
 *  - Else:
 *     - If local exists and is non-empty -> create remote from local.
 *     - If local missing or empty -> create empty local file and DO NOT push.
 */
async function initDataSync() {
  const localDir = path.dirname(LOCAL_DB_PATH);
  if (!fs.existsSync(localDir)) fs.mkdirSync(localDir, { recursive: true });

  // Try to fetch remote
  let remote = null;
  try {
    remote = await fetchRemoteDbBuffer();
  } catch (e) {
    if (e.status && e.status !== 404) throw e;
  }

  if (remote && remote.buf) {
    // Remote exists — pull and overwrite local
    fs.writeFileSync(LOCAL_DB_PATH, remote.buf);
    lastKnownSha = remote.sha;
    console.log('Pulled data/app.db from remote (sha=%s).', lastKnownSha);
  } else {
    // Remote missing
    if (fs.existsSync(LOCAL_DB_PATH) && fs.statSync(LOCAL_DB_PATH).size > 0) {
      // Local non-empty -> create remote from local (one-time)
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
      // No remote and no useful local DB -> create empty local DB and do NOT push
      if (!fs.existsSync(LOCAL_DB_PATH)) {
        fs.writeFileSync(LOCAL_DB_PATH, Buffer.alloc(0));
        console.log('Created empty local data/app.db (remote not present). Not pushing blank DB.');
      } else {
        console.log('Remote missing and local DB empty; not creating remote to avoid overwriting repo.');
      }
      lastKnownSha = null;
    }
  }

  // Periodic flush (every minute) if dirty
  setInterval(() => {
    if (dirty) syncNow().catch(err => console.error('Periodic sync error:', err.message));
  }, 60 * 1000).unref();

  // Final flush on shutdown
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
 *
 * Push local DB to remote safely. If remote exists, update with sha; if not, create it (but only if local non-empty).
 * Includes blob+commit fallback for large files.
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
    // Remote exists: update it
    const contentBase64 = buf.toString('base64');
    try {
      const data = await putFileContents(contentBase64, baseMessage, remoteSha);
      lastKnownSha = data.content.sha || null;
      dirty = false;
      console.log('Updated remote data/app.db (sha=%s).', lastKnownSha);
      return;
    } catch (e) {
      // If contents API failed, attempt blob+commit fallback
      console.warn('Contents API update failed, attempting blob+commit fallback:', e.message || e);
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
    }
  } else {
    // Remote missing: create it only if local non-empty
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
Manual recovery command (run on your server in the project root) to restore local DB from GitHub immediately:

export GITHUB_TOKEN="YOUR_TOKEN"
curl -H "Authorization: token $GITHUB_TOKEN" -H "Accept: application/vnd.github.v3.raw" \
  "https://api.github.com/repos/<OWNER>/<REPO>/contents/data/app.db?ref=<BRANCH>" -o ./data/app.db

Replace <OWNER>, <REPO>, <BRANCH>, and YOUR_TOKEN.
After that restart the app (npm start) so it will use the restored local DB.

Notes:
- With this file in place, future restarts will pull the remote copy when present and will not overwrite it with a blank local DB.
- After restoring the local DB, the app will operate normally and subsequent DB writes will be pushed to remote safely.
*/
