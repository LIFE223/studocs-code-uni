/**
 * src/api.js
 *
 * Full file — JSON API for the static frontend.
 * This version includes a more robust binary streaming for /docs/:id/view and /docs/:id/download:
 * - Ensures Content-Type and Content-Length are set.
 * - Sends Buffer so browsers can consume the PDF reliably.
 *
 * It also includes the search fix (shows approved docs, user's own docs, admin sees all).
 */

const express = require('express');
const multer = require('multer');
const mime = require('mime-types');
const { v4: uuidv4 } = require('uuid');

const { db } = require('./db');
const { ensureAuthed, ensureAdmin, registerUser, loginUser, findUserById, findUserByUsername } = require('./auth');
const { uploadPdf, fetchBlobBySha, moveToBannedFolder } = require('./githubStorage');
const { markDataDirty } = require('./gitDataSync');

const router = express.Router();
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 150 * 1024 * 1024 } }); // 150MB

// Helpers
function me(req) {
  if (!req.session || !req.session.userId) return null;
  const u = findUserById.get(req.session.userId);
  if (!u) return null;
  return {
    id: u.id,
    email: u.email,
    username: u.username,
    points: u.points,
    is_admin: !!u.is_admin,
    status: u.status,
    ban_reason: u.ban_reason || null,
    banned_at: u.banned_at || null
  };
}

function isValidGradeLevel(val) {
  if (!val) return false;
  const trimmed = String(val).trim();
  if (trimmed.toLowerCase() === 'college') return true;
  const num = Number(trimmed);
  return Number.isInteger(num) && num >= 1 && num <= 14;
}

function isValidUsername(name) {
  const s = String(name || '').trim();
  if (!s) return false;
  if (s.length < 3 || s.length > 30) return false;
  return /^[a-zA-Z0-9_.-]+$/.test(s);
}

// Auth/session
router.get('/me', (req, res) => {
  const user = me(req);
  const serverBanned = user ? user.status === 'banned' : false;
  res.json({
    ok: true,
    user,
    serverBanned,
    serverBanReason: serverBanned ? (user.ban_reason || 'Violation of rules') : '',
    branding: process.env.BRANDING_TEXT || 'Made by Firewall Freedom'
  });
});

router.post('/auth/register', async (req, res) => {
  try {
    const { email, password, confirm, client_banned, client_ban_reason, username } = req.body;
    if (!email || !password) throw new Error('Email and password required');
    if (password !== confirm) throw new Error('Passwords do not match');
    let finalUsername = String(username || '').trim();
    if (finalUsername && !isValidUsername(finalUsername)) {
      throw new Error('Invalid username. Use 3–30 chars: letters, numbers, _, ., -');
    }
    // If blank, default will be email inside registerUser
    const u = await registerUser(
      String(email).trim().toLowerCase(),
      password,
      client_banned === true || client_banned === 'true',
      client_ban_reason || '',
      finalUsername || null
    );
    req.session.userId = u.id;
    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message || 'Registration failed' });
  }
});

router.post('/auth/login', async (req, res) => {
  try {
    const { email, password, client_banned, client_ban_reason } = req.body;
    const identity = String(email).trim(); // can be email or username
    const u = await loginUser(identity, password, client_banned === true || client_banned === 'true', client_ban_reason || '');
    req.session.userId = u.id;
    res.json({ ok: true, is_admin: !!u.is_admin });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message || 'Login failed' });
  }
});

router.post('/auth/logout', (req, res) => {
  req.session.destroy(() => {
    res.json({ ok: true });
  });
});

// Account settings
const updateUsernameStmt = db.prepare('UPDATE users SET username = ? WHERE id = ?');
router.patch('/account/username', ensureAuthed, (req, res) => {
  try {
    const user = me(req);
    const newUsername = String(req.body.username || '').trim();
    if (!isValidUsername(newUsername)) {
      return res.status(400).json({ ok: false, error: 'Invalid username. Use 3–30 chars: letters, numbers, _, ., -' });
    }
    const existing = findUserByUsername.get(newUsername);
    if (existing && existing.id !== user.id) {
      return res.status(400).json({ ok: false, error: 'Username already in use' });
    }
    updateUsernameStmt.run(newUsername, user.id);
    markDataDirty();
    res.json({ ok: true, username: newUsername });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message || 'Unable to update username' });
  }
});

const userDocsStmt = db.prepare('SELECT * FROM documents WHERE uploaded_by = ?');
const deleteDocStmt = db.prepare('DELETE FROM documents WHERE id = ?');
const deleteUserStmt = db.prepare('DELETE FROM users WHERE id = ?');

router.delete('/account', ensureAuthed, async (req, res) => {
  try {
    const user = me(req);
    const docs = userDocsStmt.all(user.id);
    for (const doc of docs) {
      try {
        await moveToBannedFolder(doc.github_path, doc.github_sha, doc.id);
      } catch (e) {
        console.error('Move to banned-pdfs failed for doc', doc.id, e.message);
      }
      deleteDocStmt.run(doc.id);
      markDataDirty();
    }
    deleteUserStmt.run(user.id);
    markDataDirty();
    req.session.destroy(() => {});
    res.json({ ok: true, moved_docs: docs.length });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message || 'Account deletion failed' });
  }
});

// Leaderboard
const leaderStmt = db.prepare(`
SELECT email, username, points FROM users
WHERE status='active' AND is_admin=0
ORDER BY points DESC, datetime(created_at) ASC
LIMIT 10
`);
router.get('/leaderboard', (req, res) => {
  res.json({ ok: true, leaders: leaderStmt.all() });
});

// SEARCH / LIST docs
// Returns documents that are either approved OR belong to the current user OR (if admin) all docs.
// Supports q (title LIKE) and school filters; those filters apply on top of visibility rules.
router.get('/docs/search', async (req, res) => {
  try {
    const qRaw = (req.query.q || '').toString().trim();
    const schoolRaw = (req.query.school || '').toString().trim();

    // Prepare wildcard patterns
    const qLike = qRaw ? `%${qRaw}%` : null;
    const schoolLike = schoolRaw ? `%${schoolRaw}%` : null;

    const currentUser = me(req);
    const userId = currentUser ? currentUser.id : null;
    const isAdmin = currentUser && currentUser.is_admin;

    // Build SQL dynamically to incorporate visibility rules safely
    let sql = `
SELECT d.id, d.title, d.course, d.school, d.grade_level, d.tags, d.created_at, d.status,
       u.status as uploader_status
FROM documents d
LEFT JOIN users u ON u.id = d.uploaded_by
WHERE 1=1
`;

    const params = {};

    // Visibility clause
    if (!isAdmin) {
      sql += ` AND (d.status = 'approved'`;
      if (userId) {
        sql += ` OR d.uploaded_by = @userId`;
        params.userId = userId;
      }
      sql += `)`;
    }

    // Title filter
    if (qLike) {
      sql += ` AND d.title LIKE @q`;
      params.q = qLike;
    }

    // School filter
    if (schoolLike) {
      sql += ` AND d.school LIKE @school`;
      params.school = schoolLike;
    }

    sql += ` ORDER BY datetime(d.created_at) DESC LIMIT 200`;

    const stmt = db.prepare(sql);
    const docs = stmt.all(params);

    res.json({ ok: true, docs });
  } catch (e) {
    console.error('Search error', e);
    res.status(500).json({ ok: false, error: e.message || 'Search failed' });
  }
});

// Upload single doc (pending by default)
const insertDoc = db.prepare(`
INSERT INTO documents
(id, title, description, course, tags, content_type, size, github_path, github_sha, uploaded_by, created_at, status, school, grade_level)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
`);
router.post('/docs/upload', ensureAuthed, upload.single('pdfs'), async (req, res) => {
  try {
    const currentUser = me(req);
    if (!currentUser || currentUser.status === 'banned') {
      throw new Error('You are banned and cannot upload.');
    }
    if (!req.file) throw new Error('No file uploaded');
    const { title, description, course, tags, school, grade_level } = req.body;
    if (!school || !String(school).trim()) throw new Error('School is required');
    if (!isValidGradeLevel(grade_level)) throw new Error('Grade level must be 1–14 or College');

    const contentType = req.file.mimetype || mime.lookup(req.file.originalname) || 'application/pdf';
    if (contentType !== 'application/pdf') throw new Error('Only PDF files are allowed');

    const id = uuidv4();
    const gh = await uploadPdf(req.file.buffer, req.file.originalname, id);

    insertDoc.run(
      id,
      String(title || req.file.originalname).trim(),
      description ? String(description).trim() : '',
      course ? String(course).trim() : '',
      tags ? String(tags).trim() : '',
      contentType,
      req.file.size,
      gh.path,
      gh.sha,
      currentUser.id,
      new Date().toISOString(),
      String(school).trim(),
      String(grade_level).trim()
    );
    markDataDirty();

    res.json({ ok: true, id });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message || 'Upload failed' });
  }
});

// Upload multiple docs (batch)
router.post('/docs/uploads', ensureAuthed, upload.array('pdfs', 50), async (req, res) => {
  try {
    const currentUser = me(req);
    if (!currentUser || currentUser.status === 'banned') {
      throw new Error('You are banned and cannot upload.');
    }
    if (!req.files || req.files.length === 0) throw new Error('No files uploaded');
    const { title, description, course, tags, school, grade_level } = req.body;
    if (!school || !String(school).trim()) throw new Error('School is required');
    if (!isValidGradeLevel(grade_level)) throw new Error('Grade level must be 1–14 or College');

    const ids = [];
    for (const file of req.files) {
      const contentType = file.mimetype || mime.lookup(file.originalname) || 'application/pdf';
      if (contentType !== 'application/pdf') throw new Error('Only PDF files are allowed');

      const id = uuidv4();
      const gh = await uploadPdf(file.buffer, file.originalname, id);

      insertDoc.run(
        id,
        String(title || file.originalname).trim(),
        description ? String(description).trim() : '',
        course ? String(course).trim() : '',
        tags ? String(tags).trim() : '',
        contentType,
        file.size,
        gh.path,
        gh.sha,
        currentUser.id,
        new Date().toISOString(),
        String(school).trim(),
        String(grade_level).trim()
      );
      ids.push(id);
    }
    markDataDirty();

    res.json({ ok: true, ids });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message || 'Batch upload failed' });
  }
});

// Get doc metadata
const getDoc = db.prepare(`
SELECT d.*, u.email as uploader_email, u.username as uploader_username, u.status as uploader_status, u.banned_at as uploader_banned_at, u.ban_reason as uploader_ban_reason
FROM documents d
LEFT JOIN users u ON u.id = d.uploaded_by
WHERE d.id = ?
`);
router.get('/docs/:id', (req, res) => {
  const doc = getDoc.get(req.params.id);
  if (!doc) return res.status(404).json({ ok: false, error: 'Not found' });

  const user = me(req);
  const isAdmin = user && user.is_admin;
  const isOwner = user && user.id === doc.uploaded_by;

  if (doc.status !== 'approved' && !isAdmin && !isOwner) {
    return res.status(403).json({ ok: false, error: 'Pending review' });
  }
  res.json({ ok: true, doc });
});

// View/download binary
// IMPORTANT: send Buffer with Content-Length header for reliable browser rendering
router.get('/docs/:id/view', ensureAuthed, async (req, res) => {
  const doc = getDoc.get(req.params.id);
  if (!doc) return res.status(404).send('Not found');

  const user = me(req);
  const isAdmin = user && user.is_admin;
  const isOwner = user && user.id === doc.uploaded_by;
  if (doc.status !== 'approved' && !isAdmin && !isOwner) {
    return res.status(403).send('Pending review');
  }

  try {
    const buf = await fetchBlobBySha(doc.github_sha);
    if (!Buffer.isBuffer(buf)) {
      // Ensure we have a Buffer
      const b = Buffer.from(buf || '');
      res.setHeader('Content-Type', 'application/pdf');
      res.setHeader('Content-Length', String(b.length));
      return res.send(b);
    }
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Length', String(buf.length));
    // Prevent caching of PDFs (optional)
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    return res.send(buf);
  } catch (e) {
    console.error('Error fetching PDF blob', e);
    return res.status(500).send('Error fetching PDF');
  }
});

router.get('/docs/:id/download', ensureAuthed, async (req, res) => {
  const doc = getDoc.get(req.params.id);
  if (!doc) return res.status(404).send('Not found');

  const user = me(req);
  const isAdmin = user && user.is_admin;
  const isOwner = user && user.id === doc.uploaded_by;
  if (doc.status !== 'approved' && !isAdmin && !isOwner) {
    return res.status(403).send('Pending review');
  }

  const filename = doc.title.replace(/[^a-zA-Z0-9._-]+/g, '-') + '.pdf';
  try {
    const buf = await fetchBlobBySha(doc.github_sha);
    const b = Buffer.isBuffer(buf) ? buf : Buffer.from(buf || '');
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Length', String(b.length));
    return res.send(b);
  } catch (e) {
    console.error('Error fetching PDF for download', e);
    return res.status(500).send('Error fetching PDF');
  }
});

// Admin moderation
const latestPendingStmt = db.prepare(`
SELECT d.*, u.email as uploader_email, u.username as uploader_username, u.status as uploader_status
FROM documents d
LEFT JOIN users u ON u.id = d.uploaded_by
WHERE d.status='pending'
ORDER BY datetime(d.created_at) DESC
LIMIT 1
`);
const approveStmt = db.prepare(`UPDATE documents SET status='approved' WHERE id=?`);
const denyStmt = db.prepare(`UPDATE documents SET status='denied' WHERE id=?`);
const addPointsStmt = db.prepare(`UPDATE users SET points = points + 5 WHERE id=?`);
const banUserStmt = db.prepare(`UPDATE users SET status='banned', banned_at=?, ban_reason=? WHERE id=?`);

router.get('/admin/pending', ensureAdmin, (req, res) => {
  const doc = latestPendingStmt.get();
  res.json({ ok: true, doc: doc || null });
});

router.post('/admin/approve', ensureAdmin, (req, res) => {
  const { doc_id, uploader_id } = req.body;
  if (!doc_id) return res.status(400).json({ ok: false, error: 'doc_id required' });
  approveStmt.run(doc_id);
  if (uploader_id) addPointsStmt.run(uploader_id);
  markDataDirty();
  res.json({ ok: true });
});

router.post('/admin/deny', ensureAdmin, (req, res) => {
  const { doc_id } = req.body;
  if (!doc_id) return res.status(400).json({ ok: false, error: 'doc_id required' });
  denyStmt.run(doc_id);
  markDataDirty();
  res.json({ ok: true });
});

router.post('/admin/block-user', ensureAdmin, (req, res) => {
  const { uploader_id, reason } = req.body;
  if (!uploader_id) return res.status(400).json({ ok: false, error: 'uploader_id required' });
  const banReason = reason && String(reason).trim() ? String(reason).trim() : 'Violation of rules';
  banUserStmt.run(new Date().toISOString(), banReason, uploader_id);
  markDataDirty();
  res.json({ ok: true });
});

module.exports = router;
