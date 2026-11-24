/**
 * ফাইল   : otp-login.js  (onesignal-server এর পাশে রাখুন)
 * কাজ    : Chativya এর ওটিপি লগইন
 *            POST /auth/otp/request  { identifier }        -> ৮ সংখ্যার কোড বানিয়ে OneSignal দিয়ে Feelivya অ্যাপে পাঠায়
 *            POST /auth/otp/verify   { identifier, code }  -> কোড মিললে Firebase custom token ফেরত দেয়
 * ডাটাবেস: /_otpLogin/{uid}  = { hash, createdAt, expiresAt, attempts, hourStart, hourCount }
 *            (কোড কখনো সরাসরি রাখা হয় না, শুধু HMAC হ্যাশ; অ্যাপ এই নোড পড়তে/লিখতে পারবে না — Rules এ বন্ধ রাখুন)
 * env    : OTP_SECRET (লম্বা র‍্যান্ডম গোপন লেখা, ৩২+ অক্ষর) — নতুন যোগ করতে হবে
 *
 * দুই-প্রজেক্ট মোড (ঐচ্ছিক): নিচের দুটো env দিলে —
 *   CHATIVYA_SERVICE_ACCOUNT = Chativya-র নতুন Firebase প্রজেক্টের service account JSON (পুরো লেখাটা এক লাইনে)
 *   CHATIVYA_DATABASE_URL    = নতুন প্রজেক্টের Realtime Database URL
 * তখন কোড যাচাই হয় Feelivya (মেইন) প্রজেক্টের ডেটা দিয়ে, কিন্তু লগইন টোকেন বানানো হয় Chativya-র প্রজেক্ট থেকে
 * (একই uid)। প্রথমবার লগইনে নাম/ইউজারনেম/ফোন ইত্যাদি ছোট প্রোফাইল তথ্য নতুন প্রজেক্টে কপি হয়।
 * env দুটো না দিলে আগের মতোই একই প্রজেক্টে কাজ করে।
 */
const crypto = require("crypto");

const OTP_NODE = "_otpLogin";
const CODE_DIGITS = 8;
const CODE_TTL_MS = 5 * 60 * 1000;      // ৫ মিনিট
const RESEND_COOLDOWN_MS = 30 * 1000;   // একই অ্যাকাউন্টে ৩০ সেকেন্ডের আগে নতুন কোড নয়
const MAX_PER_HOUR = 5;                 // একই অ্যাকাউন্টে ঘণ্টায় সর্বোচ্চ ৫টি কোড
const MAX_ATTEMPTS = 5;                 // ভুল কোড ৫ বার হলে কোড বাতিল
const MAX_BODY_BYTES = 2048;

const USERNAME_KEY = /^[a-z0-9_]{1,60}$/;
const PHONE_KEY = /^[0-9]{7,15}$/;

function createOtpHandler({ admin, db, sendOneSignalPush }) {
  const secret = process.env.OTP_SECRET;
  if (!secret || secret.length < 32) {
    console.error("OTP_SECRET missing or shorter than 32 chars — OTP login disabled");
    return () => false;
  }

  // ---- Chativya-র আলাদা প্রজেক্ট (ঐচ্ছিক) ----
  let chatAuth = null; // null = মেইন প্রজেক্টেই টোকেন
  let chatDb = null;
  const saRaw = process.env.CHATIVYA_SERVICE_ACCOUNT;
  const chatDbUrl = process.env.CHATIVYA_DATABASE_URL;
  if (saRaw || chatDbUrl) {
    try {
      if (!saRaw || !chatDbUrl) throw new Error("CHATIVYA_SERVICE_ACCOUNT ও CHATIVYA_DATABASE_URL দুটোই লাগবে");
      const existing = admin.apps.find((a) => a && a.name === "chativya");
      const chatApp = existing || admin.initializeApp(
        { credential: admin.credential.cert(JSON.parse(saRaw)), databaseURL: chatDbUrl },
        "chativya"
      );
      chatAuth = chatApp.auth();
      chatDb = chatApp.database();
      console.log("OTP: Chativya আলাদা প্রজেক্ট মোড চালু");
    } catch (e) {
      console.error("Chativya project config invalid — OTP login disabled:", e && e.message ? e.message : e);
      return () => false;
    }
  }

  // ইউজার খোঁজা ও ওটিপি রেকর্ড: আলাদা প্রজেক্ট মোডে শুধু Chativya-র ডাটাবেস; না হলে মেইন
  const ddb = chatDb || db;

  // ---- IP ভিত্তিক সহজ রেট লিমিট (মেমোরিতে) ----
  const ipHits = new Map(); // key -> [timestamps]
  function ipLimited(ip, bucket, max, windowMs) {
    const key = `${bucket}|${ip}`;
    const now = Date.now();
    const arr = (ipHits.get(key) || []).filter((t) => now - t < windowMs);
    arr.push(now);
    ipHits.set(key, arr);
    if (ipHits.size > 20000) ipHits.clear();
    return arr.length > max;
  }

  function clientIp(req) {
    const xf = req.headers["x-forwarded-for"];
    if (xf) return String(xf).split(",")[0].trim();
    return req.socket.remoteAddress || "unknown";
  }

  function json(res, status, body) {
    res.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
    res.end(JSON.stringify(body));
  }

  function readJson(req) {
    return new Promise((resolve, reject) => {
      let size = 0;
      const chunks = [];
      req.on("data", (c) => {
        size += c.length;
        if (size > MAX_BODY_BYTES) { reject(new Error("too_large")); req.destroy(); return; }
        chunks.push(c);
      });
      req.on("end", () => {
        try { resolve(JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}")); }
        catch (e) { reject(new Error("bad_json")); }
      });
      req.on("error", reject);
    });
  }

  function hashCode(uid, code) {
    return crypto.createHmac("sha256", secret).update(`${uid}:${code}`).digest("hex");
  }

  function safeEqualHex(a, b) {
    const ba = Buffer.from(String(a), "hex");
    const bb = Buffer.from(String(b), "hex");
    return ba.length === bb.length && crypto.timingSafeEqual(ba, bb);
  }

  function newCode() {
    return String(crypto.randomInt(0, 10 ** CODE_DIGITS)).padStart(CODE_DIGITS, "0");
  }

  /** ইউজারনেম / ফোন / ইমেইল থেকে uid বের করে */
  async function resolveUid(identifierRaw) {
    const identifier = String(identifierRaw || "").trim();
    if (!identifier || identifier.length > 120) return null;

    // ---- আলাদা প্রজেক্ট মোড: শুধু Chativya-র ডাটাবেস (মেইন প্রজেক্টে যায় না) ----
    if (chatDb) {
      if (identifier.includes("@")) {
        // দরকার: Rules এ userProfiles এর নিচে ".indexOn": ["email"]
        const snap = await chatDb.ref("userProfiles").orderByChild("email")
          .equalTo(identifier.toLowerCase()).limitToFirst(1).get();
        let found = null;
        snap.forEach((c) => { found = c.key; });
        if (found) return found;
        const snap2 = await chatDb.ref("userProfiles").orderByChild("email")
          .equalTo(identifier).limitToFirst(1).get();
        snap2.forEach((c) => { found = c.key; });
        return found;
      }
      const uk2 = identifier.toLowerCase();
      const pk2 = identifier.replace(/[^0-9]/g, "");
      const paths = [];
      if (USERNAME_KEY.test(uk2)) paths.push(`usernameIndex/${uk2}`);
      if (PHONE_KEY.test(pk2)) paths.push(`phoneIndex/${pk2}`);
      for (const path of paths) {
        const uid = (await chatDb.ref(path).child("uid").get()).val();
        if (typeof uid === "string" && uid) return uid;
      }
      return null;
    }

    // ---- সাধারণ মোড: মেইন প্রজেক্ট (আগের মতো) ----
    if (identifier.includes("@")) {
      try {
        const u = await admin.auth().getUserByEmail(identifier.toLowerCase());
        return u.disabled ? null : u.uid;
      } catch (e) { return null; }
    }

    const uk = identifier.toLowerCase();
    const pk = identifier.replace(/[^0-9]/g, "");
    const lookups = [];
    if (USERNAME_KEY.test(uk)) lookups.push(`usernameIndex/${uk}`);
    if (PHONE_KEY.test(pk)) lookups.push(`phoneIndex/${pk}`);

    for (const path of lookups) {
      const uid = (await db.ref(path).child("uid").get()).val();
      if (typeof uid === "string" && uid) {
        try {
          const u = await admin.auth().getUser(uid);
          if (!u.disabled) return uid;
        } catch (e) { /* পরেরটা চেষ্টা */ }
      }
    }
    return null;
  }

  async function handleRequest(req, res) {
    const ip = clientIp(req);
    if (ipLimited(ip, "req", 10, 60 * 1000)) return json(res, 429, { ok: false, error: "too_many_requests" });

    const body = await readJson(req);
    const uid = await resolveUid(body.identifier);

    // অ্যাকাউন্ট আছে কিনা বোঝা না যায় — তাই না থাকলেও একই উত্তর
    const reply = { ok: true, expiresInSec: CODE_TTL_MS / 1000, resendAfterSec: RESEND_COOLDOWN_MS / 1000 };
    if (!uid) return json(res, 200, reply);

    const code = newCode();
    const codeHash = hashCode(uid, code);
    const now = Date.now();
    let throttled = false;

    await ddb.ref(`${OTP_NODE}/${uid}`).transaction((rec) => {
      throttled = false;
      if (rec && now - (rec.createdAt || 0) < RESEND_COOLDOWN_MS) { throttled = true; return; }
      let hourStart = rec && rec.hourStart && now - rec.hourStart < 3600 * 1000 ? rec.hourStart : now;
      let hourCount = rec && hourStart === rec.hourStart ? rec.hourCount || 0 : 0;
      if (hourCount >= MAX_PER_HOUR) { throttled = true; return; }
      return { hash: codeHash, createdAt: now, expiresAt: now + CODE_TTL_MS, attempts: 0, hourStart, hourCount: hourCount + 1 };
    });
    if (throttled) return json(res, 200, reply);

    const ok = await sendOneSignalPush({
      externalId: uid,
      headings: "Chativya login code",
      contents: `Your code is ${code}. It works for 5 minutes. Never share it with anyone.`,
      priority: 10,
      ttl: CODE_TTL_MS / 1000,
    });
    if (!ok) {
      await ddb.ref(`${OTP_NODE}/${uid}`).remove().catch(() => {});
      return json(res, 502, { ok: false, error: "send_failed" });
    }
    console.log(`OTP sent for uid ${uid.slice(0, 6)}…`);
    return json(res, 200, reply);
  }

  async function handleVerify(req, res) {
    const ip = clientIp(req);
    if (ipLimited(ip, "verify", 20, 60 * 1000)) return json(res, 429, { ok: false, error: "too_many_requests" });

    const body = await readJson(req);
    const code = String(body.code || "").trim();
    const invalid = { ok: false, error: "invalid_or_expired" };
    if (!new RegExp(`^[0-9]{${CODE_DIGITS}}$`).test(code)) return json(res, 200, invalid);

    const uid = await resolveUid(body.identifier);
    if (!uid) return json(res, 200, invalid);

    const wanted = hashCode(uid, code);
    let outcome = "invalid";
    // একটাই ট্রানজ্যাকশনে: মেয়াদ/চেষ্টা যাচাই, মিললে কোড মুছে দেওয়া (একবারই ব্যবহার), না মিললে চেষ্টা +১
    await ddb.ref(`${OTP_NODE}/${uid}`).transaction((rec) => {
      outcome = "invalid";
      if (rec === null) return rec;
      if (Date.now() > (rec.expiresAt || 0)) return null;
      if ((rec.attempts || 0) >= MAX_ATTEMPTS) return null;
      if (safeEqualHex(rec.hash, wanted)) { outcome = "ok"; return null; }
      rec.attempts = (rec.attempts || 0) + 1;
      return rec;
    });
    if (outcome !== "ok") return json(res, 200, invalid);

    let token;
    if (chatAuth) {
      token = await chatAuth.createCustomToken(uid);
    } else {
      token = await admin.auth().createCustomToken(uid);
    }
    console.log(`OTP login OK for uid ${uid.slice(0, 6)}…`);
    return json(res, 200, { ok: true, token });
  }

  /** index.js এর http সার্ভারে প্রথমে ডাকুন; রুট মিললে true ফেরত দেয় (রেসপন্স হয়ে গেছে) */
  return function otpHandler(req, res) {
    const path = (req.url || "").split("?")[0];
    if (path !== "/auth/otp/request" && path !== "/auth/otp/verify") return false;
    if (req.method !== "POST") { json(res, 405, { ok: false, error: "method_not_allowed" }); return true; }

    const run = path === "/auth/otp/request" ? handleRequest : handleVerify;
    run(req, res).catch((err) => {
      console.error("OTP route error:", err && err.message ? err.message : err);
      if (!res.headersSent) json(res, 400, { ok: false, error: "bad_request" });
    });
    return true;
  };
}

module.exports = { createOtpHandler };
