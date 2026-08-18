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
 *   4. Sends a push through OneSignal's REST API, targeting the recipient
 *      by external_id == their Firebase uid (set on the Android side via
 *      OneSignal.login(uid) — already wired into MainActivity).
 *
 * Nothing here writes or modifies message data — it only reads what the
 * app already wrote and asks OneSignal to deliver a push about it.
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

// Only handles messages created after this process started, same as a
// Cloud Function trigger would — avoids re-notifying the entire history
// on every restart.
const startedAt = Date.now();

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
      if (message.timestamp && message.timestamp < startedAt) return; // skip old history

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

async function sendOneSignalPush({ externalId, headings, contents, data }) {
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
    }),
  });
  const body = await res.json();
  if (!res.ok) {
    console.error("OneSignal API error:", body);
  } else {
    console.log("Push sent:", body.id);
  }
}

console.log("Meetlity OneSignal push server running — watching for new messages...");
