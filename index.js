/**
 * Free push-notification sender for Meetlity — replaces the Firebase Cloud
 * Function (functions/index.js), which needs a paid Blaze plan to deploy.
 *
 * This is a small always-on Node process (NOT a Cloud Function) meant to
 * run on a free host like Render.com or Railway.app. It:
 *   1. Connects to the same Realtime Database the Android app already uses,
 *      via the Firebase Admin SDK (using the service-account JSON you
 *      generated for OneSignal — reuse that same file here).
 *   2. Listens for new messages the same way the Cloud Function did:
 *      /users/{userUid}/conversations/{peerUid}/messages/{messageId}
 *   3. Also listens for new in-app notifications (likes, comments,
 *      reactions, friend requests, etc.):
 *      /users/{userUid}/notifications/{notificationId}
 *   4. Also listens for new incoming calls:
 *      /users/{userUid}/callInbox/{callId}
 *   5. Sends a push through OneSignal's REST API, targeting the recipient
 *      by external_id == their Firebase uid (set on the Android side via
 *      OneSignal.login(uid) — already wired into MainActivity).
 *
 * Nothing here writes or modifies message/call data — it only reads what
 * the app already wrote and asks OneSignal to deliver a push about it.
 */

const admin = require("firebase-admin");
const fetch = require("node-fetch");

// ---- Config (set these as environment variables on your host) ----------
const ONESIGNAL_APP_ID = process.env.ONESIGNAL_APP_ID; // OneSignal Dashboard > Settings > Keys & IDs
const ONESIGNAL_REST_API_KEY = process.env.ONESIGNAL_REST_API_KEY; // same page, "REST API Key"
const FIREBASE_DATABASE_URL = process.env.FIREBASE_DATABASE_URL; // e.g. https://meetlity-3be01-default-rtdb.firebaseio.com

if (!ONESIGNAL_APP_ID || !ONESIGNAL_REST_API_KEY || !FIREBASE_DATABASE_URL) {
  console.error(
    "Missing required env vars: ONESIGNAL_APP_ID, ONESIGNAL_REST_API_KEY, FIREBASE_DATABASE_URL"
  );
  process.exit(1);
}

// service-account.json must be uploaded alongside this file (same one you
// generated in Firebase Console > Project settings > Service accounts).
const serviceAccount = require("./service-account.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: FIREBASE_DATABASE_URL,
});

const db = admin.database();

// ---- Recovery of missed events while the server was asleep/offline -------
// Instead of only remembering "when this process started" in memory (which
// is lost on every restart), we persist the last-processed timestamp to
// Firebase itself, under a small bookkeeping node. On every restart we load
// it back, so messages/notifications that arrived while the server was down
// (e.g. Render free-tier spin-down) still get a push once it wakes up.
//
// NOTE: calls are intentionally NOT covered by this cursor (see the
// callInbox listener below) — a call push that arrives late, after the
// caller has already given up and hung up, is worse than no push at all,
// so calls only ever use the in-memory startedAt cutoff, never replayed
// after a restart.
const CURSOR_PATH = "/_pushServerState/lastProcessedAt";
const cursorRef = db.ref(CURSOR_PATH);

let lastProcessedAt = 0; // will be loaded from Firebase below
let cursorLoaded = false;
const pendingBeforeLoad = []; // events that arrive before the cursor finishes loading

async function loadCursor() {
  try {
    const snap = await cursorRef.get();
    const saved = snap.val();
    // First run ever (no cursor saved yet): start from "now" so we don't
    // replay the app's entire historical message/notification backlog.
    lastProcessedAt = typeof saved === "number" ? saved : Date.now();
  } catch (err) {
    console.error("Failed to load push-server cursor, defaulting to now:", err);
    lastProcessedAt = Date.now();
  }
  cursorLoaded = true;
  console.log(
    `Resuming from cursor: ${new Date(lastProcessedAt).toISOString()} (recovering anything missed since then)`
  );
}

// Advance and persist the cursor. Called after successfully handling each
// event so a crash/restart resumes from roughly where it left off.
let saveQueued = false;
function advanceCursor(timestamp) {
  if (!timestamp || timestamp <= lastProcessedAt) return;
  lastProcessedAt = timestamp;
  if (saveQueued) return; // coalesce rapid-fire saves into one write
  saveQueued = true;
  setTimeout(() => {
    saveQueued = false;
    cursorRef.set(lastProcessedAt).catch((err) => {
      console.error("Failed to save push-server cursor:", err);
    });
  }, 2000);
}

const startedAt = Date.now(); // still used as a fallback for events with no timestamp field

const messagesRef = db.ref("/users");

messagesRef.on("child_added", (userSnap) => {
  const userUid = userSnap.key;
  const conversationsRef = db.ref(`/users/${userUid}/conversations`);

  conversationsRef.on("child_added", (peerSnap) => {
    const peerUid = peerSnap.key;
    const msgsRef = db.ref(`/users/${userUid}/conversations/${peerUid}/messages`);

    msgsRef.on("child_added", async (msgSnap) => {
      const messageId = msgSnap.key;
      const message = msgSnap.val();
      if (!message || !message.senderUid) return;
      const ts = message.timestamp || startedAt;
      if (ts < lastProcessedAt) return; // already handled before restart, or genuinely old history

      // Same de-dup rule as the original Cloud Function: only the
      // sender's own copy of the message triggers a push.
      if (userUid !== message.senderUid) return;
      if (!peerUid || peerUid === userUid) return;

      try {
        const senderSnap = await db.ref(`users/${userUid}`).get();
        const sender = senderSnap.val() || {};
        const senderName = sender.name || sender.displayName || sender.fullName || "Meetlity user";

        let preview = "New message";
        if (message.type === "text" && message.text) preview = message.text;
        else if (message.type === "voice") preview = "Voice message";
        else if (message.type === "image") preview = "Photo";
        else if (message.type === "video") preview = "Video";
        else if (message.type) preview = "Attachment";

        await sendOneSignalPush({
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
        advanceCursor(ts);
      } catch (err) {
        console.error("Push send failed", err);
      }
    });
  });

  // --- In-app notifications (likes, comments, reactions, friend
  // requests, etc.) -> also send a push, same de-dup/skip-old-history
  // rules as messages above.
  const notificationsRef = db.ref(`/users/${userUid}/notifications`);
  notificationsRef.on("child_added", async (notifSnap) => {
    const notif = notifSnap.val();
    if (!notif || !notif.fromUid) return;
    if (notif.timestamp && notif.timestamp < startedAt) return; // skip old history

    try {
      const fromName = notif.fromName || "Someone";
      const { title, body } = describeNotification(notif, fromName);

      await sendOneSignalPush({
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
    } catch (err) {
      console.error("Notification push send failed", err);
    }
  });

  // --- Incoming calls -> push so IncomingCallActivity can pop over the
  // lock screen even with the app fully killed.
  //
  // The call system was redesigned to use a single top-level master node
  // (calls/{callId}) instead of mirroring the same call under both
  // participants' own users/{uid}/calls/{callId} — see
  // CallSignalingRepository's class doc on the Android side. callInbox is
  // unaffected: users/{calleeUid}/callInbox/{callId} still just points at
  // that one call doc, exactly as before.
  //
  // callInbox's value is just a timestamp (see startCall in
  // CallSignalingRepository), so the caller's name/avatar/type are read
  // from the single calls/{callId} doc, same as the Android app does.
  //
  // The WebRTC offer SDP itself is deliberately left OUT of the push data
  // — SDPs can run several KB and risk truncation/rejection at OneSignal's
  // payload limit. IncomingCallActivity already listens live to the call
  // doc in Firebase, so it reads the offer from there once it's on
  // screen rather than needing it in the push.
  const callInboxRef = db.ref(`/users/${userUid}/callInbox`);
  callInboxRef.on("child_added", async (callSnap) => {
    const callId = callSnap.key;
    const inboxTimestamp = callSnap.val();
    // Only push for calls that started after this process came up — an
    // old/stale call ringing on a dead server is worse than not ringing.
    if (typeof inboxTimestamp === "number" && inboxTimestamp < startedAt) return;

    try {
      const callSnapFull = await db.ref(`/calls/${callId}`).get();
      const call = callSnapFull.val();
      if (!call || call.status !== "ringing") return; // already answered/rejected/ended

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
        priority: 10, // ring pushes should preempt normal delivery queueing
      });
    } catch (err) {
      console.error("Call push send failed", err);
    }
  });
});

// --- Call-ended real-time signal ------------------------------------------
// Registered ONCE at the top level (not per-user, unlike callInbox above)
// since calls/{callId} is now a single top-level node shared by both
// participants — one listener here covers every call in the system instead
// of needing one per user.
//
// This is what lets a call actually stop ringing/close on a device whose
// app was fully killed (App Kill Handling: "যদি Caller App Kill হয় ->
// Database Cleanup হবে", and the Real-Time Requirement that a reject/end
// takes effect on both devices immediately, no manual refresh). Without
// this, a killed app only ever learns the call ended by re-opening the app
// and picking up whatever Firebase state happens to still be there — the
// OneSignalNotificationServiceExtension on the Android side already has a
// dormant handler waiting for exactly this "callEndedId" push field; it
// just never had a sender.
//
// notifiedCallIds is a small in-memory guard so a call that gets multiple
// status writes in quick succession (e.g. "rejected" written, then the
// node deleted moments later during cleanup) only ever triggers one push
// per call, per participant.
const notifiedCallIds = new Set();
const TERMINAL_CALL_STATUSES = new Set(["rejected", "cancelled", "timeout", "failed", "ended", "busy"]);

const callsRootRef = db.ref("/calls");
callsRootRef.on("child_changed", async (callSnap) => {
  try {
    const callId = callSnap.key;
    const call = callSnap.val();
    if (!call || !TERMINAL_CALL_STATUSES.has(call.status)) return;
    if (notifiedCallIds.has(callId)) return;
    notifiedCallIds.add(callId);
    // Keep the guard set from growing forever across a long-running process.
    if (notifiedCallIds.size > 5000) notifiedCallIds.clear();

    const targets = [call.callerId, call.calleeId].filter(Boolean);
    await Promise.all(
      targets.map((uid) =>
        sendOneSignalPush({
          externalId: uid,
          // headings/contents are never actually shown for this push —
          // OneSignalNotificationServiceExtension calls preventDefault()
          // unconditionally whenever callEndedId is present (see its
          // class doc on the Android side) — only the data payload below
          // matters here.
          headings: "Meetlity",
          contents: "Call ended",
          data: { callEndedId: callId },
          priority: 10,
        }).catch((err) => console.error(`Call-ended push to ${uid} failed`, err))
      )
    );
  } catch (err) {
    console.error("Call-ended watcher failed", err);
  }
});

function describeNotification(notif, fromName) {
  const title = "Meetlity";
  switch (notif.type) {
    case "like":
      return { title, body: `${fromName} liked your post` };
    case "comment":
      return { title, body: `${fromName} commented: ${notif.previewText || ""}`.trim() };
    case "reaction":
      return { title, body: `${fromName} reacted to your post` };
    case "comment_reaction":
      return { title, body: `${fromName} reacted to your comment` };
    case "friend_request":
      return { title, body: `${fromName} sent you a friend request` };
    case "friend_accept":
      return { title, body: `${fromName} accepted your friend request` };
    case "message_request":
      return { title, body: `${fromName} sent you a message request` };
    case "reel_reaction":
      return { title, body: `${fromName} reacted to your reel` };
    case "reel_comment":
      return { title, body: `${fromName} commented on your reel: ${notif.previewText || ""}`.trim() };
    case "reel_share":
      return { title, body: `${fromName} shared your reel` };
    default:
      return { title, body: `${fromName} sent you a notification` };
  }
}

async function sendOneSignalPush({ externalId, headings, contents, data, priority }) {
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
      ...(priority ? { priority } : {}),
    }),
  });
  const body = await res.json();
  if (!res.ok) {
    console.error("OneSignal API error:", body);
  } else {
    console.log("Push sent:", body.id);
  }
}

console.log("Meetlity OneSignal push server running — watching for new messages, notifications, and calls...");

// ---- Minimal HTTP server so Render (or any host expecting a Web Service)
// detects an open port and doesn't spin the deploy down as unhealthy. This
// process is really a background worker, not a web app — this endpoint is
// just a health check.
const http = require("http");
const PORT = process.env.PORT || 3000;
http
  .createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Meetlity push server is running.\n");
  })
  .listen(PORT, () => {
    console.log(`Health check server listening on port ${PORT}`);
  });

// ---- Self-ping to prevent Render's free tier from spinning this service
// down after 15 minutes of inactivity. Every 2 minutes, the server makes
// an HTTP request to its own public URL. Render sees this as inbound
// traffic and keeps the instance awake, so we don't depend on an external
// uptime-monitoring service.
//
// Set SELF_PING_URL as an environment variable to your Render URL, e.g.
// https://three723.onrender.com  (no trailing slash).
const SELF_PING_URL = process.env.SELF_PING_URL;
if (SELF_PING_URL) {
  const PING_INTERVAL_MS = 2 * 60 * 1000; // 2 minutes — safely under Render's 15-min spin-down
  setInterval(() => {
    fetch(SELF_PING_URL)
      .then(() => console.log(`Self-ping OK (${new Date().toISOString()})`))
      .catch((err) => console.error("Self-ping failed:", err.message));
  }, PING_INTERVAL_MS);
  console.log(`Self-ping enabled — pinging ${SELF_PING_URL} every 2 minutes.`);
} else {
  console.warn(
    "SELF_PING_URL not set — this service will spin down after 15 min of inactivity unless something else pings it (e.g. UptimeRobot)."
  );
}

loadCursor();
