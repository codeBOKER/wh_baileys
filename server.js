require("dotenv").config();
const {
    default: makeWASocket,
    DisconnectReason,
    BufferJSON,
    fetchLatestBaileysVersion,
    Browsers // 👈 Added browser fingerprint support to avoid bot detection
} = require("@whiskeysockets/baileys");

const express = require("express");
const qrcode = require("qrcode-terminal");
const pino = require("pino");
const axios = require("axios");
const crypto = require("crypto");
const { createClient } = require("@supabase/supabase-js");
const Redis = require("ioredis");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 7860;
const SEND_WEBHOOK_URL = process.env.LLM_WEBHOOK_URL + "/whatsapp";
const ACCESS_TOKEN = process.env.ACCESS_TOKEN;
const DAILY_MSG_LIMIT_PER_USER = parseInt(process.env.DAILY_MSG_LIMIT_PER_USER) || 500;
const DAILY_MSG_LIMIT_GLOBAL = parseInt(process.env.DAILY_MSG_LIMIT_GLOBAL) || 500;
const MODE = process.env.MODE || "prod";
const IS_PROD = MODE === "prod";
const AUTH_TABLE = `whatsapp_auth_${MODE}`;
const SEND_MODE = process.env.SEND_MODE || "limited";

// Log only in non-production environments
const log = (...args) => { if (!IS_PROD) console.log(...args); };
const logWarn = (...args) => { if (!IS_PROD) console.warn(...args); };

// Initialize Supabase and Redis connections
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const redis = new Redis(process.env.REDIS_URL);

let sock = null;
let isReconnecting = false;
let retry428Count = 0; // 👈 Counter to track repeated 428 errors and prevent infinite loops

// ─── Buffer repair function (Fix Buffers) ───
// Converts damaged Buffer objects coming from Supabase into real Buffers that the Signal crypto library understands
function fixBuffers(obj) {
    if (!obj || typeof obj !== "object") return obj;
    if (obj.type === "Buffer" && Array.isArray(obj.data)) {
        return Buffer.from(obj.data);
    }
    for (const key of Object.keys(obj)) {
        if (typeof obj[key] === "object" && obj[key] !== null) {
            obj[key] = fixBuffers(obj[key]);
        }
    }
    return obj;
}

// ─── Daily message limits ───
const dailyUserCounts = new Map();
let dailyGlobalCount = 0;
let dailyResetDate = new Date().toDateString();

function resetDailyCountsIfNeeded() {
    const today = new Date().toDateString();
    if (today !== dailyResetDate) {
        dailyUserCounts.clear();
        dailyGlobalCount = 0;
        dailyResetDate = today;
        log("[Limits] Daily message counters reset");
    }
}

function checkDailyLimits(jid) {
    resetDailyCountsIfNeeded();

    const userCount = dailyUserCounts.get(jid) || 0;
    if (userCount >= DAILY_MSG_LIMIT_PER_USER) {
        logWarn(`[Limits] BLOCKED: ${jid} hit daily per-user limit (${userCount}/${DAILY_MSG_LIMIT_PER_USER})`);
        return false;
    }

    if (dailyGlobalCount >= DAILY_MSG_LIMIT_GLOBAL) {
        logWarn(`[Limits] BLOCKED: Global daily limit reached (${dailyGlobalCount}/${DAILY_MSG_LIMIT_GLOBAL})`);
        return false;
    }

    return true;
}

function recordSentMessage(jid) {
    resetDailyCountsIfNeeded();
    dailyUserCounts.set(jid, (dailyUserCounts.get(jid) || 0) + 1);
    dailyGlobalCount++;
}

// ─── Group message deduplication ───
const dedupCache = new Map();
const DEDUP_TTL = 30 * 60 * 1000; // 30 minutes

function isDuplicateGroupMessage(participant, messageText) {
    const key = `${participant}:${messageText}`;
    if (dedupCache.has(key)) {
        return true;
    }
    dedupCache.set(key, Date.now());
    return false;
}

setInterval(() => {
    const now = Date.now();
    for (const [key, timestamp] of dedupCache) {
        if (now - timestamp > DEDUP_TTL) {
            dedupCache.delete(key);
        }
    }
}, 5 * 60 * 1000);

// ─── Local LRU key cache ───
const keyCache = new Map();
const KEY_CACHE_TTL = 3600000; // 1 hour
const KEY_CACHE_MAX = 10000;

function getCachedKey(fullId) {
    const entry = keyCache.get(fullId);
    if (!entry) return undefined;
    if (Date.now() - entry.ts > KEY_CACHE_TTL) {
        keyCache.delete(fullId);
        return undefined;
    }
    return entry.value;
}

function setCachedKey(fullId, value) {
    if (keyCache.size >= KEY_CACHE_MAX) {
        const firstKey = keyCache.keys().next().value;
        keyCache.delete(firstKey);
    }
    keyCache.set(fullId, { value, ts: Date.now() });
}

// ─── User preload cache ───
const localChatCache = new Set();
let usersPreloaded = false;

async function preloadUsers() {
    try {
        const { data, error } = await supabase
            .from("users")
            .select("remote_jid");

        if (error) {
            console.error("[Preload] Failed to load users:", error.message);
            return;
        }

        if (data) {
            for (const user of data) {
                localChatCache.add(user.remote_jid);
                await redis.set(`${MODE}:user:${user.remote_jid}`, "true", "EX", 86400);
            }
        }
        usersPreloaded = true;
        log(`[Preload] Loaded ${localChatCache.size} users into memory cache`);
    } catch (err) {
        console.error("[Preload] Error:", err.message);
    }
}

// ─── Non-blocking write queue ───
let dbWriteQueue = Promise.resolve();

// ─── Supabase session save and management engine (fully revised) ───
async function useSupabaseAuthState() {
    const writeData = async (data, id) => {
        try {
            const jsonStr = JSON.stringify(data, BufferJSON.replacer);
            const { error } = await supabase
                .from(AUTH_TABLE)
                .upsert({ id, data: JSON.parse(jsonStr), updated_at: new Date() });
            if (error) console.error(`[DB Write Error - ${id}]:`, error.message);
        } catch (error) {
            console.error("Write mapping failed", error);
        }
    };

    const readData = async (id) => {
        try {
            const { data, error } = await supabase
                .from(AUTH_TABLE)
                .select("data")
                .eq("id", id)
                .maybeSingle();

            if (error || !data || !data.data) return null;

            // Fix Buffer issues after retrieval and deserialization
            let parsed = JSON.parse(JSON.stringify(data.data), BufferJSON.reviver);
            return fixBuffers(parsed);
        } catch (error) {
            return null;
        }
    };

    let creds = await readData("creds");
    if (!creds) {
        const { initAuthCreds } = require("@whiskeysockets/baileys");
        creds = initAuthCreds();
        await writeData(creds, "creds");
    }

    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    if (!ids || ids.length === 0) return data;

                    const uncachedIds = [];
                    for (const id of ids) {
                        const fullId = `${type}-${id}`;
                        const cached = getCachedKey(fullId);
                        if (cached !== undefined) {
                            data[id] = cached;
                        } else {
                            uncachedIds.push(id);
                        }
                    }

                    if (uncachedIds.length > 0) {
                        const fullIds = uncachedIds.map(id => `${type}-${id}`);
                        try {
                            const { data: dbRows, error } = await supabase
                                .from(AUTH_TABLE)
                                .select("id, data")
                                .in("id", fullIds);

                            if (error) {
                                console.error(`[DB Bulk Read Error - ${type}]:`, error.message);
                                return data;
                            }

                            const rowMap = new Map(dbRows?.map(row => [row.id, row.data]) || []);

                            for (const id of uncachedIds) {
                                const key = `${type}-${id}`;
                                let value = rowMap.get(key);

                                if (value) {
                                    value = JSON.parse(JSON.stringify(value), BufferJSON.reviver);
                                    value = fixBuffers(value); // Fix Buffers retrieved programmatically

                                    if (type === "app-state-sync-key") {
                                        const { proto } = require("@whiskeysockets/baileys");
                                        value = proto.Message.AppStateSyncKeyData.fromObject(value);
                                    }
                                    setCachedKey(key, value);
                                }
                                data[id] = value || undefined;
                            }
                        } catch (err) {
                            console.error(`Bulk read mapping crash for ${type}:`, err);
                        }
                    }

                    return data;
                },
                set: async (data) => {
                    const upserts = [];
                    const deletes = [];

                    for (const category in data) {
                        for (const id in data[category]) {
                            const value = data[category][id];
                            const key = `${category}-${id}`;

                            if (value) {
                                const jsonStr = JSON.stringify(value, BufferJSON.replacer);
                                upserts.push({
                                    id: key,
                                    data: JSON.parse(jsonStr),
                                    updated_at: new Date()
                                });
                                setCachedKey(key, value);
                            } else {
                                deletes.push(key);
                                keyCache.delete(key);
                            }
                        }
                    }

                    dbWriteQueue = dbWriteQueue.then(async () => {
                        try {
                            if (upserts.length > 0) {
                                const { error } = await supabase.from(AUTH_TABLE).upsert(upserts);
                                if (error) console.error("[DB Bulk Write Error]:", error.message);
                            }
                            if (deletes.length > 0) {
                                const { error } = await supabase.from(AUTH_TABLE).delete().in("id", deletes);
                                if (error) console.error("[DB Bulk Delete Error]:", error.message);
                            }
                        } catch (err) {
                            console.error("Bulk write execution network failure:", err?.message || err);
                        }
                    });
                }
            }
        },
        saveCreds: () => writeData(creds, "creds")
    };
}

function generateSignature(body, appSecret) {
    const digest = crypto
        .createHmac("sha256", appSecret)
        .update(body)
        .digest("hex");

    return `sha256=${digest}`;
}

async function verifyAndRegisterUser(remoteJid, remoteJidAlt, msg) {
    try {
        if (localChatCache.has(remoteJid)) return;

        const cacheKey = `${MODE}:user:${remoteJid}`;
        const cachedUser = await redis.get(cacheKey);
        if (cachedUser) {
            localChatCache.add(remoteJid);
            return;
        }

        const { data: dbUser, error } = await supabase
            .from("users")
            .select("remote_jid")
            .eq("remote_jid", remoteJid)
            .single();

        if (dbUser) {
            await redis.set(`${MODE}:user:${remoteJid}`, "true", "EX", 86400);
            localChatCache.add(remoteJid);
            return;
        }

        await supabase.from("users").insert({
            remote_jid: remoteJid,
            remote_jid_alt: remoteJidAlt || null
        });

        await redis.set(`${MODE}:user:${remoteJid}`, "true", "EX", 86400);
        localChatCache.add(remoteJid);
        log(`Registered new user: ${remoteJid}`);
    } catch (err) {
        console.error("User registration error:", err.message);
    }
}

async function rateLimitOutgoingMessage() {
    const now = Date.now();
    const minIntervalMs = 500;

    const rateLimitKey = `${MODE}:whatsapp_last_sent_timestamp`;
    const delay = await redis.eval(
        `local now = tonumber(ARGV[1])
         local interval = tonumber(ARGV[2])
         local key = ARGV[3]
         local last = tonumber(redis.call('get', key) or '0')
         local target = last + interval
         if target < now then
             target = now
         end
         redis.call('set', key, target)
         return target - now`,
        0,
        now,
        minIntervalMs,
        rateLimitKey
    );

    if (delay > 0) {
        await new Promise((resolve) => setTimeout(resolve, delay));
    }
}

// ─── Main WhatsApp startup function ───
async function startWhatsApp() {
    try {
        const { state, saveCreds } = await useSupabaseAuthState();
        const { version, isLatest } = await fetchLatestBaileysVersion();
        log(`[WhatsApp] Using WA version ${version.join(".")} (latest: ${isLatest})`);

        sock = makeWASocket({
            auth: state,
            version,
            browser: Browsers.ubuntu("Desktop"), // 👈 Provide a reliable browser fingerprint to prevent 428/405 blocks
            logger: pino({ level: "silent" }),
            markOnlineOnConnect: false,
            syncFullHistory: false
        });

        sock.ev.on("creds.update", saveCreds);

        sock.ev.on("connection.update", async (update) => {
            const { connection, lastDisconnect, qr } = update;
            if (connection === "close" || !sock?.user) {
                log(`[WhatsApp] Connection Status: ${connection || 'Initializing...'}`);
            }

            if (qr) {
                console.log("==================================================");
                console.log("📱 NEW QR CODE GENERATED - SCAN VIA LOGS:");
                console.log(`   [MODE: ${MODE}]`);
                console.log("==================================================");
                qrcode.generate(qr, { small: true });
                console.log("==================================================");
            }

            if (connection === "open") {
                console.log("✅ WhatsApp Connected Successfully!");
                isReconnecting = false;
                retry428Count = 0; // Reset counter on success
                if (!usersPreloaded) {
                    await preloadUsers();
                }
            }

            if (connection === "close") {
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                console.log(`❌ Connection closed. Status: ${statusCode}`);

                // If the error is 428, increment the counter to track persistence
                if (statusCode === 428 || statusCode === DisconnectReason.connectionClosed) {
                    retry428Count++;
                    console.warn(`⚠️ Status 428 encountered (${retry428Count}/3)`);
                }

                // Session cleanup condition: true block (401/403/logout) or repeated 428 more than 3 times
                const shouldLogout =
                    statusCode === 401 ||
                    statusCode === 403 ||
                    statusCode === DisconnectReason.loggedOut ||
                    retry428Count >= 3;

                if (shouldLogout) {
                    console.log("🧹 Session corrupt or repeated 428 loop. Nuking Supabase Session Data...");
                    retry428Count = 0;
                    keyCache.clear(); // Clear cached data

                    const { error } = await supabase.from(AUTH_TABLE).delete().neq("id", "keep_alive_placeholder");
                    if (error) {
                        console.error("🚨 CRITICAL: Failed to wipe DB! Check Supabase RLS:", error.message);
                    } else {
                        console.log("✅ Database wiped successfully. Reconnecting to issue a new QR...");
                    }

                    sock?.end(undefined);
                    sock = null;

                    setTimeout(() => startWhatsApp(), 5000);
                    return;
                }

                if (statusCode === 405) {
                    console.log("⚠️ Status 405: Client rejected. Reconnecting without wiping session...");
                }

                if (!isReconnecting) {
                    isReconnecting = true;
                    console.log("🔄 Reconnecting in 5 seconds...");
                    setTimeout(() => {
                        isReconnecting = false;
                        startWhatsApp();
                    }, 5000);
                }
            }
        });

        sock.ev.on("messages.upsert", async ({ messages, type }) => {
            try {
                log(`[MSG] Received ${messages?.length || 0} messages, type: ${type}`);

                if (type !== "notify" && type !== "append") {
                    log(`[MSG] Skipped: unrecognized type "${type}"`);
                    return;
                }

                const msg = messages?.[0];
                if (!msg || msg.key.fromMe) {
                    log("[MSG] Skipped: no message object");
                    return;
                }

                if (msg.key.remoteJid === "status@broadcast") {
                    log("[MSG] Skipped: status update");
                    return;
                }

                const remoteJid = msg.key.remoteJid;
                const remoteJidAlt = msg.key.remoteJidAlt;

                const extendedText = msg.message?.extendedTextMessage;
                const messageText =
                    msg.message?.conversation ||
                    extendedText?.text ||
                    msg.message?.extendedTextMessage?.text;

                if (!messageText) {
                    log("[MSG] Skipped: no text content. Message keys:", Object.keys(msg.message || {}));
                    return;
                }

                const contextInfo = extendedText?.contextInfo;
                const contextMessageId = contextInfo?.stanzaId || null;

                if (remoteJid.endsWith("@g.us")) {
                    const participant = msg.key.participant || remoteJid;
                    if (isDuplicateGroupMessage(participant, messageText)) {
                        log(`[MSG] Skipped: duplicate group message from ${participant} in ${remoteJid}`);
                        return;
                    }
                }

                const payload = {
                    object: "whatsapp_business_account",
                    entry: [
                        {
                            id: "BAILEYS_ACCOUNT",
                            changes: [
                                {
                                    field: "messages",
                                    value: {
                                        messaging_product: "whatsapp",
                                        metadata: {
                                            display_phone_number: "BAILEYS_GATEWAY",
                                            phone_number_id: "BAILEYS_GATEWAY_ID"
                                        },
                                        contacts: [
                                            {
                                                wa_id: remoteJid,
                                                profile: {
                                                    name: msg.pushName || "User"
                                                }
                                            }
                                        ],
                                        messages: [
                                            (() => {
                                                const msgEntry = {
                                                    from: remoteJid,
                                                    id: msg.key.id,
                                                    timestamp: String(msg.messageTimestamp),
                                                    type: "text",
                                                    text: {
                                                        body: messageText
                                                    }
                                                };
                                                if (remoteJidAlt) {
                                                    msgEntry.remote_jid_alt = remoteJidAlt;
                                                }
                                                if (contextMessageId) {
                                                    msgEntry.context = { id: contextMessageId };
                                                }
                                                return msgEntry;
                                            })()
                                        ]
                                    }
                                }
                            ]
                        }
                    ]
                };

                const body = JSON.stringify(payload);
                const signature = generateSignature(body, process.env.WHATSAPP_APP_SECRET);

                log(`📤 Forwarding [${remoteJid}]: ${messageText}`);
                log(`📤 Webhook URL: ${SEND_WEBHOOK_URL}`);

                if (!remoteJid.endsWith("@g.us")) {
                    await sock.readMessages([{ remoteJid, id: msg.key.id }]);
                    await sock.sendPresenceUpdate("composing", remoteJid);
                }

                try {
                    const webhookRes = await axios.post(SEND_WEBHOOK_URL, body, {
                        headers: {
                            "Content-Type": "application/json",
                            "x-hub-signature-256": signature
                        },
                        timeout: 10000
                    });
                    log(`✅ Webhook sent OK: ${webhookRes.status}`);
                } catch (webhookErr) {
                    console.error(`❌ Webhook FAILED: ${webhookErr?.response?.status || 'no response'} - ${webhookErr?.response?.data ? JSON.stringify(webhookErr.response.data) : webhookErr?.message}`);
                }

                if (!remoteJid.endsWith("@g.us")) {
                    verifyAndRegisterUser(remoteJid, remoteJidAlt, msg).catch(() => {});
                }

            } catch (err) {
                console.error("Message handler error:", err?.response?.data || err?.message || err);
            }
        });
    } catch (err) {
        console.error("Startup Error:", err);
    }
}

// ─── Outbound message sending route ───
app.post("/v20.0/:phone_number_id/messages", async (req, res) => {
    try {
        const authHeader = req.headers.authorization;
        if (!authHeader || authHeader !== `Bearer ${ACCESS_TOKEN}`) {
            return res.status(401).json({
                error: {
                    message: "Unauthorized system authorization credentials token verification failed."
                }
            });
        }

        const {
            messaging_product,
            to,
            type,
            text,
            interactive
        } = req.body;

        if (messaging_product !== "whatsapp") {
            return res.status(400).json({
                error: {
                    message: "Invalid messaging product"
                }
            });
        }

        if (!to) {
            return res.status(400).json({
                error: {
                    message: "Recipient remoteJid or phone number is required"
                }
            });
        }

        if (!sock) {
            return res.status(500).json({
                error: {
                    message: "WhatsApp backend connection engine down"
                }
            });
        }

        const jid = to.includes("@")
            ? to
            : `${to}@s.whatsapp.net`;

        if (SEND_MODE !== "unlimited") {
            if (!checkDailyLimits(jid)) {
                return res.status(429).json({
                    error: {
                        message: "Daily message limit reached. Try again tomorrow."
                    }
                });
            }

            await rateLimitOutgoingMessage();
        }

        let result;

        switch (type) {
            case "text": {
                if (!text?.body) {
                    return res.status(400).json({
                        error: {
                            message: "Text body is required"
                        }
                    });
                }

                const charCount = text.body.length;
                const typingDuration = Math.min(4000, Math.max(1000, charCount * 40));

                if (SEND_MODE !== "unlimited" && !jid.endsWith("@g.us")) {
                    await sock.sendPresenceUpdate("available", jid);
                    await sock.sendPresenceUpdate("composing", jid);
                    await new Promise(resolve => setTimeout(resolve, typingDuration));
                }

                result = await sock.sendMessage(jid, {
                    text: text.body
                });

                if (SEND_MODE !== "unlimited") {
                    recordSentMessage(jid);
                }

                break;
            }

            default: {
                return res.status(400).json({
                    error: {
                        message: `Unsupported message type: ${type}`
                    }
                });
            }
        }

        return res.status(200).json({
            messaging_product: "whatsapp",
            contacts: [
                {
                    input: to,
                    wa_id: to
                }
            ],
            messages: [
                {
                    id: result.key.id
                }
            ]
        });
    } catch (err) {
        console.error("Outbound Sender Error:", err);

        return res.status(500).json({
            error: {
                message:
                    err?.message ||
                    "Internal Engine Error Processing Messaging Event"
            }
        });
    }
});

app.get("/", (req, res) => {
    res.json({ status: "running", mode: MODE, sendMode: SEND_MODE, environment: "huggingface-spaces" });
});

app.listen(PORT, () => {
    console.log(`🚀 Server safely deployed and processing requests on port ${PORT} [MODE: ${MODE}]`);
    startWhatsApp();
});