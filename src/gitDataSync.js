/**
 * src/gitDataSync.js
 *
 * Safe data sync between local ./data/app.db and remote repo/data/app.db.
 *
 * Behavior summary:
 * - On initDataSync():
 *   - If remote data/app.db exists: ALWAYS pull it and write local copy; DO NOT push/overwrite remote.
 *   - Else (remote missing):
 *       - If a non-empty local data/app.db exists: create remote file from local copy.
 *       - If no local DB or local DB is empty: do NOT push a blank DB (avoid overwriting remote later).
 * - Exposes markDataDirty() to schedule a push, and syncNow(force) to push immediately.
 * - When pushing, uses contents API, retries with remote sha if GitHub returns "sha wasn't supplied".
 * - For large files attempts blob+tree+commit, with fallback to contents API.
 *
 * Important safety rule: initDataSync will NOT create/overwrite a remote file if the remote already exists.
 * This prevents accidental overwrites of a repo copy by an empty local file on a fresh server.
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
const SIZE_WARN_LIMIT = 90 * 1024 * 1024; // 90MB

async function getRemoteSha() {
  try {
    const { data } = await octokit.repos.getContent({
      owner: GH_OWNER,
      repo: GH_REPO,
      path: REMOTE_DB_PATH,
      ref: GH_BRANCH
    });
    if (Array.isArray(data)) throw new Error('Expected file, found directory at ' + REMOTE_DB_PATH);
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
  // Create blob
  const contentBase64 = buffer.toString('base64');
  const blob = await octokit.git.createBlob({
    owner: GH_OWNER,
    repo: GH_REPO,
    content: contentBase64,
    encoding: 'base64'
  });

  // Get current commit of the branch
  const ref = await octokit.git.getRef({ owner: GH_OWNER, repo: GH_REPO, ref: `heads/${GH_BRANCH}` });
  const baseCommitSha = ref.data.object.sha;
  const baseCommit = await octokit.git.getCommit({ owner: GH_OWNER, repo: GH_REPO, commit_sha: baseCommitSha });

  // Create tree and commit, update ref
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
 * Safe startup sync:
 * - If remote exists: pull remote and write local. DO NOT push.
 * - If remote missing:
 *     - If local exists and non-empty: create remote from local.
 *     - If local missing or empty: create local empty file and DO NOT push (avoid overwriting remote later).
 */
async function initDataSync() {
  const localDir = path.dirname(LOCAL_DB_PATH);
  if (!fs.existsSync(localDir)) fs.mkdirSync(localDir, { recursive: true });

  // Check remote
  let remote = null;
  try {
    remote = await fetchRemoteDbBuffer();
  } catch (e) {
    // If error is not 404-like, surface it
    if (e.status && e.status !== 404) throw e;
  }

  if (remote && remote.buf) {
    // Remote exists -> pull and overwrite local
    fs.writeFileSync(LOCAL_DB_PATH, remote.buf);
    lastKnownSha = remote.sha;
    console.log('Pulled data/app.db from remote (sha=%s).', lastKnownSha);
  } else {
    // Remote missing
    // If local exists and has non-zero size, create remote from local
    if (fs.existsSync(LOCAL_DB_PATH) && fs.statSync(LOCAL_DB_PATH).size > 0) {
      // Push local up as initial remote file
      const buf = fs.readFileSync(LOCAL_DB_PATH);
      const size = buf.length;
      const contentBase64 = buf.toString('base64');

      try {
        if (size <= SIZE_WARN_LIMIT) {
          const data = await putFileContents(contentBase64, 'Initialize data/app.db from local');
          lastKnownSha = data.content.sha || null;
          console.log('Created remote data/app.db from local (sha=%s).', lastKnownSha);
        } else {
          const { commitSha } = await commitRawBlob(buf, 'Initialize data/app.db from local (large file)');
          lastKnownSha = await getRemoteSha();
          console.log('Created remote data/app.db via raw commit (commit=%s).', commitSha);
        }
      } catch (e) {
        // If we cannot create remote (permission/branch issues), raise configError for clarity
        if (e.status === 404) {
          throw configError('Failed to create remote data/app.db: repository/branch not found or token lacking permission.');
        }
        throw e;
      }
    } else {
      // No remote and no local (or empty local): create an empty local DB but DO NOT push.
      if (!fs.existsSync(LOCAL_DB_PATH)) {
        fs.writeFileSync(LOCAL_DB_PATH, Buffer.alloc(0));
        console.log('Created empty local data/app.db (remote not present). Not pushing blank DB to remote.');
      } else {
        console.log('Remote missing and local DB empty; not creating remote to avoid overwriting remote copy later.');
      }
      // leave lastKnownSha null
    }
  }

  // periodic flush if dirty
  setInterval(() => {
    if (dirty) syncNow().catch(err => console.error('Periodic sync error:', err.message));
  }, 60 * 1000).unref();

  // Final sync on shutdown
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
 * Push local DB to remote. If remote exists, update by including remote sha. If remote missing, create it.
 * For safety: if remote exists we fetch sha first to avoid "sha wasn't supplied".
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

  // Check remote existence
  const remoteSha = await getRemoteSha();

  // If remote exists, prefer contents API with sha
  if (remoteSha) {
    const contentBase64 = buf.toString('base64');
    try {
      const data = await putFileContents(contentBase64, baseMessage, remoteSha);
      lastKnownSha = data.content.sha || null;
      dirty = false;
      console.log('Updated remote data/app.db (sha=%s).', lastKnownSha);
      return;
    } catch (e) {
      // If contents API fails (e.g., 422), try to re-fetch sha and retry once
      if (e.status === 422 && e.message && e.message.includes(`"sha" wasn't supplied`)) {
        const refreshedSha = await getRemoteSha();
        if (refreshedSha) {
          const data = await putFileContents(buf.toString('base64'), baseMessage, refreshedSha);
          lastKnownSha = data.content.sha || null;
          dirty = false;
          console.log('Updated remote data/app.db after retry (sha=%s).', lastKnownSha);
          return;
        }
      }
      // If contents API fails in other ways, try blob+commit fallback
      console.warn('Contents API update failed, attempting blob+commit fallback:', e.message || e);
    }

    // fallback to blob+commit for robustness
    try {
      const { commitSha } = await commitRawBlob(buf, baseMessage);
      lastKnownSha = await getRemoteSha();
      dirty = false;
      console.log('Updated remote data/app.db via blob commit (commit=%s).', commitSha);
      return;
    } catch (e) {
      console.error('Blob commit fallback failed:', e.message || e);
      throw e;
    }
  } else {
    // Remote missing: attempt to create remote file from local
    if (size === 0) {
      // Don't push empty DB - safety
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
      // If creation fails, attempt blob commit as fallback
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
