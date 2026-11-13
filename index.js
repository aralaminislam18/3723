/**
 * ফাইল   : index.js (onesignal-server)
 * কাজ    : ডাটাবেসে নতুন মেসেজ / নোটিফিকেশন / কল এলে OneSignal দিয়ে ফোনে পুশ পাঠায়
 * ব্যবহার : Render ইত্যাদিতে আলাদা Node সার্ভার হিসেবে চলে (node index.js)
 * নির্ভর : firebase-admin, node-fetch, service-account.json, env: ONESIGNAL_APP_ID /
 *          ONESIGNAL_REST_API_KEY / FIREBASE_DATABASE_URL
 *
 * নতুন ডাটাবেস স্ট্রাকচার অনুযায়ী পরিবর্তন (পুরনো users/{uid}/... আর নেই):
 *   users                        -> userProfiles
 *   users/{uid}/conversations    -> userChats/{uid}
 *   users/{uid}/notifications    -> userNotifications/{uid}
 *   users/{uid}/callInbox        -> userCallInbox/{uid}
 *   calls/{callId}               -> আগের মতোই (রুটে মাস্টার নোড)
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

// ---- নতুন ডাটাবেস নোডের নাম (এক জায়গায়, যাতে পরে বদলানো সহজ হয়) ----
const NODE_PROFILES = "userProfiles";
const NODE_CHATS = "userChats";
const NODE_NOTIFICATIONS = "userNotifications";
const NODE_CALL_INBOX = "userCallInbox";
const NODE_CALLS = "calls";

const TTL_DEFAULT = 3 * 24 * 60 * 60; // ৩ দিন: ফোন অফলাইন থাকলে নেট এলে পাবে
const TTL_CALL = 45;                  // কলের রিং ৪৫ সেকেন্ড, পুরনো কল আর বাজবে না
const TTL_CALL_ENDED = 120;

// ---- কার্সর (শেষ কোন সময় পর্যন্ত পুশ গেছে) --------------------------------
const cursorRef = db.ref("/_pushServerState/lastProcessedAt");
let lastProcessedAt = 0;
let saveTimer = null;

/** ডাটাবেস থেকে কার্সর পড়ে; না পেলে এখনকার সময় ধরে */
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

/** কার্সর সঙ্গে সঙ্গে ডাটাবেসে সেভ করে */
function flushCursor() {
  if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
  return cursorRef.set(lastProcessedAt).catch((e) => console.error("Cursor save failed:", e));
}

/** পুশ সফল হলে কার্সর সামনে বাড়ায় (২ সেকেন্ড পরে সেভ হয়) */
function advanceCursor(ts) {
  if (!ts || ts <= lastProcessedAt) return;
  lastProcessedAt = ts;
  if (saveTimer) return;
  saveTimer = setTimeout(() => { saveTimer = null; flushCursor(); }, 2000);
}

for (const sig of ["SIGTERM", "SIGINT"]) {
  process.on(sig, async () => {
    await flushCursor(); // রিডিপ্লয়/স্পিন-ডাউনের আগে কার্সর সেভ
    process.exit(0);
  });
}

const startedAt = Date.now(); // শুধু কলের জন্য (পুরনো কল কখনো রিপ্লে হবে না)

// ---- প্রতিটি ইউজারের জন্য লিসেনার ----------------------------------------
/** একজন ইউজারের চ্যাট, নোটিফিকেশন ও কল-ইনবক্স শোনে */
function watchUser(userUid) {
  // চ্যাট: userChats/{uid}/{peerUid}/messages/{msgId}
  db.ref(`/${NODE_CHATS}/${userUid}`).on("child_added", (peerSnap) => {
    const peerUid = peerSnap.key;
    db.ref(`/${NODE_CHATS}/${userUid}/${peerUid}/messages`).on("child_added", async (msgSnap) => {
      const messageId = msgSnap.key;
      const message = msgSnap.val();
      if (!message || !message.senderUid) return;
      const ts = message.timestamp || startedAt;
      if (ts <= lastProcessedAt) return;
      if (userUid !== message.senderUid) return; // শুধু প্রেরকের কপি পুশ ট্রিগার করে
      if (!peerUid || peerUid === userUid) return;

      try {
        const sender = (await db.ref(`/${NODE_PROFILES}/${userUid}`).get()).val() || {};
        const senderName = sender.fullName || sender.name || sender.displayName || "Meetlity user";

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

  // নোটিফিকেশন (রিঅ্যাকশন/কমেন্ট/ফ্রেন্ড রিকোয়েস্ট ...): userNotifications/{uid}/{id}
  db.ref(`/${NODE_NOTIFICATIONS}/${userUid}`).on("child_added", async (notifSnap) => {
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

  // ইনকামিং কল: userCallInbox/{uid}/{callId} = সময়, তারপর calls/{callId} থেকে বিস্তারিত
  db.ref(`/${NODE_CALL_INBOX}/${userUid}`).on("child_added", async (callSnap) => {
    const callId = callSnap.key;
    const inboxTimestamp = callSnap.val();
    if (typeof inboxTimestamp === "number" && inboxTimestamp < startedAt) return;
    try {
      const call = (await db.ref(`/${NODE_CALLS}/${callId}`).get()).val();
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

/** কল শেষ হলে দুই পক্ষকেই "Call ended" পুশ দেয় (রিং বন্ধ করার জন্য) */
function watchCalls() {
  db.ref(`/${NODE_CALLS}`).on("child_changed", async (callSnap) => {
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

/** নোটিফিকেশনের ধরন অনুযায়ী শিরোনাম ও লেখা বানায় */
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

/** OneSignal API তে পুশ পাঠায়; OneSignal গ্রহণ করলেই true ফেরত দেয় */
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

// ---- হেলথ সার্ভার ও সেলফ-পিং ---------------------------------------------
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

// ---- শুরু: আগে কার্সর, তারপর লিসেনার ---------------------------------------
(async () => {
  await loadCursor();
  watchCalls();
  // সব ইউজার userProfiles থেকে আসে (আগে ছিল /users)
  db.ref(`/${NODE_PROFILES}`).on("child_added", (userSnap) => watchUser(userSnap.key));
  console.log("Meetlity OneSignal push server running.");
})();
