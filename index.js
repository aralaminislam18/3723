/**
 * Meetlity free push server (OneSignal) — fixed version.
 * Changes vs previous:
 *  1. Cursor is loaded BEFORE any listener is attached (old code had a race:
 *     listeners fired with lastProcessedAt=0 and could re-push old history).
 *  2. Notifications (likes/comments/...) now use the same cursor, so ones
 *     missed while the server slept are recovered too.
 *  3. Explicit TTL: messages/notifications 3 days (offline phone gets them
 *     when net returns); call ring 45s (no stale "incoming call" later);
 *     call-ended 120s.
 *  4. Cursor is flushed immediately on SIGTERM (Render spin-down/redeploy).
 *  5. Cursor only advances when OneSignal really accepted the push.
 */
const admin = require("firebase-admin");
const fetch = require("node-fetch");
const http = require("http");

const ONESIGNAL_APP_ID = process.env.ONESIGNAL_APP_ID;
const ONESIGNAL_REST_API_KEY = process.env.ONESIGNAL_REST_API_KEY;
const FIREBASE_DATABASE_URL = process.env.FIREBASE_DATABASE_URL;
if (!ONESIGNAL_APP_ID || !ONESIGNAL_REST_API_KEY || !FIREBASE_DATABASE_URL) {
  console.error("Missing env vars: ONESIGNAL_APP_ID, ONESIGNAL_REST_API_KEY, FIREBASE_DATABASE_URL");
  process.exit(1);
}

const serviceAccount = require("./service-account.json");
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: FIREBASE_DATABASE_URL,
});
const db = admin.database();

const TTL_DEFAULT = 3 * 24 * 60 * 60; // 3 days
const TTL_CALL = 45;
const TTL_CALL_ENDED = 120;

// ---- Cursor ---------------------------------------------------------------
const cursorRef = db.ref("/_pushServerState/lastProcessedAt");
let lastProcessedAt = 0;
let saveTimer = null;

async function loadCursor() {
  try {
    const saved = (await cursorRef.get()).val();
    lastProcessedAt = typeof saved === "number" ? saved : Date.now();
  } catch (err) {
    console.error("Failed to load cursor, defaulting to now:", err);
    lastProcessedAt = Date.now();
  }
  console.log(`Resuming from cursor: ${new Date(lastProcessedAt).toISOString()}`);
}

function flushCursor() {
  if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
  return cursorRef.set(lastProcessedAt).catch((e) => console.error("Cursor save failed:", e));
}

function advanceCursor(ts) {
  if (!ts || ts <= lastProcessedAt) return;
  lastProcessedAt = ts;
  if (saveTimer) return;
  saveTimer = setTimeout(() => { saveTimer = null; flushCursor(); }, 2000);
}

for (const sig of ["SIGTERM", "SIGINT"]) {
  process.on(sig, async () => {
    await flushCursor();
    process.exit(0);
  });
}

const startedAt = Date.now(); // used for calls only (never replay calls)

// ---- Listeners ------------------------------------------------------------
function watchUser(userUid) {
  db.ref(`/users/${userUid}/conversations`).on("child_added", (peerSnap) => {
    const peerUid = peerSnap.key;
    db.ref(`/users/${userUid}/conversations/${peerUid}/messages`).on("child_added", async (msgSnap) => {
      const messageId = msgSnap.key;
      const message = msgSnap.val();
      if (!message || !message.senderUid) return;
      const ts = message.timestamp || startedAt;
      if (ts <= lastProcessedAt) return;
      if (userUid !== message.senderUid) return; // only sender's copy triggers push
      if (!peerUid || peerUid === userUid) return;

      try {
        const sender = (await db.ref(`users/${userUid}`).get()).val() || {};
        const senderName = sender.name || sender.displayName || sender.fullName || "Meetlity user";

        let preview = "New message";
        if (message.type === "text" && message.text) preview = message.text;
        else if (message.type === "voice") preview = "Voice message";
        else if (message.type === "image") preview = "Photo";
        else if (message.type === "video") preview = "Video";
        else if (message.type) preview = "Attachment";

        const ok = await sendOneSignalPush({
          externalId: peerUid,
          headings: senderName,
          contents: preview,
          data: {
            conversationId: [userUid, peerUid].sort().join("_"),
            senderUid: userUid,
            senderName,
            preview,
            messageId,
          },
        });
        if (ok) advanceCursor(ts);
      } catch (err) {
        console.error("Push send failed", err);
      }
    });
  });

  db.ref(`/users/${userUid}/notifications`).on("child_added", async (notifSnap) => {
    const notif = notifSnap.val();
    if (!notif || !notif.fromUid) return;
    if (notif.timestamp && notif.timestamp <= lastProcessedAt) return;
    try {
      const fromName = notif.fromName || "Someone";
      const { title, body } = describeNotification(notif, fromName);
      const ok = await sendOneSignalPush({
        externalId: userUid,
        headings: title,
        contents: body,
        data: {
          type: notif.type,
          fromUid: notif.fromUid,
          fromName,
          targetPostId: notif.targetPostId || null,
        },
      });
      if (ok && notif.timestamp) advanceCursor(notif.timestamp);
    } catch (err) {
      console.error("Notification push send failed", err);
    }
  });

  db.ref(`/users/${userUid}/callInbox`).on("child_added", async (callSnap) => {
    const callId = callSnap.key;
    const inboxTimestamp = callSnap.val();
    if (typeof inboxTimestamp === "number" && inboxTimestamp < startedAt) return;
    try {
      const call = (await db.ref(`/calls/${callId}`).get()).val();
      if (!call || call.status !== "ringing") return;
      const callerUid = call.callerId;
      if (!callerUid || callerUid === userUid) return;

      await sendOneSignalPush({
        externalId: userUid,
        headings: call.callerName || "Meetlity",
        contents: call.type === "video" ? "Incoming video call" : "Incoming audio call",
        data: {
          callId,
          callerUid,
          callerName: call.callerName || null,
          callerAvatarUrl: call.callerAvatarUrl || null,
          callType: call.type || "audio",
        },
        priority: 10,
        ttl: TTL_CALL,
      });
    } catch (err) {
      console.error("Call push send failed", err);
    }
  });
}

const notifiedCallIds = new Set();
const TERMINAL = new Set(["rejected", "cancelled", "timeout", "failed", "ended", "busy"]);

function watchCalls() {
  db.ref("/calls").on("child_changed", async (callSnap) => {
    try {
      const callId = callSnap.key;
      const call = callSnap.val();
      if (!call || !TERMINAL.has(call.status)) return;
      if (notifiedCallIds.has(callId)) return;
      notifiedCallIds.add(callId);
      if (notifiedCallIds.size > 5000) notifiedCallIds.clear();

      await Promise.all(
        [call.callerId, call.calleeId].filter(Boolean).map((uid) =>
          sendOneSignalPush({
            externalId: uid,
            headings: "Meetlity",
            contents: "Call ended",
            data: { callEndedId: callId },
            priority: 10,
            ttl: TTL_CALL_ENDED,
          }).catch((e) => console.error(`Call-ended push to ${uid} failed`, e))
        )
      );
    } catch (err) {
      console.error("Call-ended watcher failed", err);
    }
  });
}

function describeNotification(notif, fromName) {
  const title = "Meetlity";
  switch (notif.type) {
    case "like": return { title, body: `${fromName} liked your post` };
    case "comment": return { title, body: `${fromName} commented: ${notif.previewText || ""}`.trim() };
    case "reaction": return { title, body: `${fromName} reacted to your post` };
    case "comment_reaction": return { title, body: `${fromName} reacted to your comment` };
    case "friend_request": return { title, body: `${fromName} sent you a friend request` };
    case "friend_accept": return { title, body: `${fromName} accepted your friend request` };
    case "message_request": return { title, body: `${fromName} sent you a message request` };
    case "reel_reaction": return { title, body: `${fromName} reacted to your reel` };
    case "reel_comment": return { title, body: `${fromName} commented on your reel: ${notif.previewText || ""}`.trim() };
    case "reel_share": return { title, body: `${fromName} shared your reel` };
    default: return { title, body: `${fromName} sent you a notification` };
  }
}

// Returns true only if OneSignal accepted the request.
async function sendOneSignalPush({ externalId, headings, contents, data, priority, ttl }) {
  const res = await fetch("https://onesignal.com/api/v1/notifications", {
    method: "POST",
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      Authorization: `Basic ${ONESIGNAL_REST_API_KEY}`,
    },
    body: JSON.stringify({
      app_id: ONESIGNAL_APP_ID,
      include_aliases: { external_id: [externalId] },
      target_channel: "push",
      headings: { en: headings },
      contents: { en: contents },
      data,
      ttl: ttl || TTL_DEFAULT,
      ...(priority ? { priority } : {}),
    }),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    console.error("OneSignal API error:", body);
    return false;
  }
  console.log("Push sent:", body.id);
  return true;
}

// ---- Health server + self-ping -------------------------------------------
const PORT = process.env.PORT || 3000;
http
  .createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Meetlity push server is running.\n");
  })
  .listen(PORT, () => console.log(`Health check server on port ${PORT}`));

const SELF_PING_URL = process.env.SELF_PING_URL;
if (SELF_PING_URL) {
  setInterval(() => {
    fetch(SELF_PING_URL)
      .then(() => console.log(`Self-ping OK (${new Date().toISOString()})`))
      .catch((err) => console.error("Self-ping failed:", err.message));
  }, 2 * 60 * 1000);
} else {
  console.warn("SELF_PING_URL not set — service may spin down after 15 min idle.");
}

// ---- Start: cursor FIRST, then listeners ---------------------------------
(async () => {
  await loadCursor();
  watchCalls();
  db.ref("/users").on("child_added", (userSnap) => watchUser(userSnap.key));
  console.log("Meetlity OneSignal push server running.");
})();
