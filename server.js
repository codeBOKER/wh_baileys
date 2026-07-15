require("dotenv").config();
const {
    default: makeWASocket,
    DisconnectReason,
    BufferJSON,
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

// Initialize Database and Cache Connections
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const redis = new Redis(process.env.REDIS_URL);

let sock = null;
let isReconnecting = false;

// ─── ENHANCEMENT 5: Daily Message Limits ───
// Prevents bans by capping per-user and global outbound messages per day
const dailyUserCounts = new Map();   // { jid: count }
let dailyGlobalCount = 0;
let dailyResetDate = new Date().toDateString();

function resetDailyCountsIfNeeded() {
    const today = new Date().toDateString();
    if (today !== dailyResetDate) {
        dailyUserCounts.clear();
        dailyGlobalCount = 0;
        dailyResetDate = today;
        console.log("[Limits] Daily message counters reset");
    }
}

function checkDailyLimits(jid) {
    resetDailyCountsIfNeeded();

    const userCount = dailyUserCounts.get(jid) || 0;
    if (userCount >= DAILY_MSG_LIMIT_PER_USER) {
        console.log(`[Limits] BLOCKED: ${jid} hit daily per-user limit (${userCount}/${DAILY_MSG_LIMIT_PER_USER})`);
        return false;
    }

    if (dailyGlobalCount >= DAILY_MSG_LIMIT_GLOBAL) {
        console.log(`[Limits] BLOCKED: Global daily limit reached (${dailyGlobalCount}/${DAILY_MSG_LIMIT_GLOBAL})`);
        return false;
    }

    return true;
}

function recordSentMessage(jid) {
    resetDailyCountsIfNeeded();
    dailyUserCounts.set(jid, (dailyUserCounts.get(jid) || 0) + 1);
    dailyGlobalCount++;
}

// ─── ENHANCEMENT: Group Message Deduplication ───
// Discards duplicate messages sent by the same user to multiple groups within 30 minutes
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

// Cleanup expired entries every 5 minutes
setInterval(() => {
    const now = Date.now();
    for (const [key, timestamp] of dedupCache) {
        if (now - timestamp > DEDUP_TTL) {
            dedupCache.delete(key);
        }
    }
}, 5 * 60 * 1000);

// ─── ENHANCEMENT 1: In-Memory LRU Cache for Auth Keys ───
// Reduces Supabase queries by caching keys locally with TTL
const keyCache = new Map();
const KEY_CACHE_TTL = 3600000; // 1 hour TTL
const KEY_CACHE_MAX = 10000;   // Max entries before eviction

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
        // Evict oldest entry
        const firstKey = keyCache.keys().next().value;
        keyCache.delete(firstKey);
    }
    keyCache.set(fullId, { value, ts: Date.now() });
}

// ─── ENHANCEMENT 2: Pre-loaded User Cache ───
// Bulk-loads all known users on startup to avoid per-message DB lookups
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
                await redis.set(`user:${user.remote_jid}`, "true", "EX", 86400);
            }
        }
        usersPreloaded = true;
        console.log(`[Preload] Loaded ${localChatCache.size} users into memory cache`);
    } catch (err) {
        console.error("[Preload] Error:", err.message);
    }
}

// ─── ENHANCEMENT 3: Non-Blocking Write Queue ───
// Fire-and-forget for key writes, only block when critical
let dbWriteQueue = Promise.resolve();

// Custom Supabase Authentication State Engine
async function useSupabaseAuthState() {
    const writeData = async (data, id) => {
        try {
            const jsonStr = JSON.stringify(data, BufferJSON.replacer);
            const { error } = await supabase
                .from("whatsapp_auth")
                .upsert({ id, data: JSON.parse(jsonStr), updated_at: new Date() });
            if (error) console.error(`[DB Write Error - ${id}]:`, error.message);
        } catch (error) {
            console.error("Write mapping failed", error);
        }
    };

    const readData = async (id) => {
        try {
            const { data, error } = await supabase
                .from("whatsapp_auth")
                .select("data")
                .eq("id", id)
                .maybeSingle();

            if (error) return null;
            if (!data || !data.data) return null;
            
            return JSON.parse(JSON.stringify(data.data), BufferJSON.reviver);
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

                    // ─── ENHANCEMENT 1b: Check cache first ───
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

                    // Only query Supabase for uncached keys
                    if (uncachedIds.length > 0) {
                        const fullIds = uncachedIds.map(id => `${type}-${id}`);
                        try {
                            const { data: dbRows, error } = await supabase
                                .from("whatsapp_auth")
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
                                    if (type === "app-state-sync-key") {
                                        const { proto } = require("@whiskeysockets/baileys");
                                        value = proto.Message.AppStateSyncKeyData.fromObject(value);
                                    }
                                    // ─── Cache the fetched key ───
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
                                // ─── Update local cache immediately ───
                                setCachedKey(key, value);
                            } else {
                                deletes.push(key);
                                keyCache.delete(key);
                            }
                        }
                    }

                    // ─── ENHANCEMENT 3b: Fire-and-forget for non-critical writes ───
                    // Don't block the caller - write in background
                    dbWriteQueue = dbWriteQueue.then(async () => {
                        try {
                            if (upserts.length > 0) {
                                const { error } = await supabase.from("whatsapp_auth").upsert(upserts);
                                if (error) console.error("[DB Bulk Write Error]:", error.message);
                            }
                            if (deletes.length > 0) {
                                const { error } = await supabase.from("whatsapp_auth").delete().in("id", deletes);
                                if (error) console.error("[DB Bulk Delete Error]:", error.message);
                            }
                        } catch (err) {
                            console.error("Bulk write execution network failure:", err?.message || err);
                        }
                    });

                    // ─── Don't await - fire and forget ───
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

// ─── ENHANCEMENT 4: Async User Registration (Non-Blocking) ───
// Runs in background, doesn't block message forwarding
async function verifyAndRegisterUser(remoteJid, remoteJidAlt, msg) {
    try {
        // Step 1: Check local cache (instant)
        if (localChatCache.has(remoteJid)) return;

        // Step 2: Check Redis cache
        const cacheKey = `user:${remoteJid}`;
        const cachedUser = await redis.get(cacheKey);
        if (cachedUser) {
            localChatCache.add(remoteJid);
            return;
        }

        // Step 3: Check Supabase
        const { data: dbUser, error } = await supabase
            .from("users")
            .select("remote_jid")
            .eq("remote_jid", remoteJid)
            .single();

        if (dbUser) {
            await redis.set(cacheKey, "true", "EX", 86400);
            localChatCache.add(remoteJid);
            return;
        }

        // Step 4: Register new user
        await supabase.from("users").insert({
            remote_jid: remoteJid,
            remote_jid_alt: remoteJidAlt || null
        });
        
        await redis.set(cacheKey, "true", "EX", 86400);
        localChatCache.add(remoteJid);
        console.log(`Registered new user: ${remoteJid}`);
    } catch (err) {
        // ─── Don't let user registration errors crash message handling ───
        console.error("User registration error:", err.message);
    }
}

// Redis Lua-script Atomic Rate Limiting (2 messages per second)
async function rateLimitOutgoingMessage() {
    const now = Date.now();
    const minIntervalMs = 500;

    const delay = await redis.eval(
        `local now = tonumber(ARGV[1])
         local interval = tonumber(ARGV[2])
         local last = tonumber(redis.call('get', 'whatsapp_last_sent_timestamp') or '0')
         local target = last + interval
         if target < now then
             target = now
         end
         redis.call('set', 'whatsapp_last_sent_timestamp', target)
         return target - now`,
        0,
        now,
        minIntervalMs
    );

    if (delay > 0) {
        await new Promise((resolve) => setTimeout(resolve, delay));
    }
}

async function startWhatsApp() {
    try {
        const { state, saveCreds } = await useSupabaseAuthState();

        sock = makeWASocket({
            auth: state,
            logger: pino({ level: "silent" }),
            markOnlineOnConnect: false,
            syncFullHistory: false
        });

        sock.ev.on("creds.update", saveCreds);

        sock.ev.on("connection.update", async (update) => {
            const { connection, lastDisconnect, qr } = update;
            if (connection === "close" || !sock?.user) {
                console.log(`[WhatsApp] Connection Status: ${connection || 'Initializing...'}`);
            }

            if (qr) {
                console.log("==================================================");
                console.log("📱 NEW QR CODE GENERATED - SCAN VIA HUGGING FACE LOGS:");
                console.log("==================================================");
                qrcode.generate(qr, { small: true });
                console.log("==================================================");
            }

            if (connection === "open") {
                console.log("✅ WhatsApp Connected");
                isReconnecting = false;
                // ─── Pre-load users on successful connection ───
                if (!usersPreloaded) {
                    await preloadUsers();
                }
            }

            if (connection === "close") {
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                console.log(`❌ Connection closed. Status: ${statusCode}`);

                const shouldLogout =
                    statusCode === 401 ||
                    statusCode === 405 ||
                    statusCode === 403 ||
                    statusCode === DisconnectReason.loggedOut;

                if (shouldLogout) {
                    console.log("🧹 Session corrupted. Nuking Database Session Data...");
                    
                    const { error } = await supabase.from("whatsapp_auth").delete().neq("id", "keep_alive_placeholder");
                    if (error) {
                        console.error("🚨 CRITICAL: Failed to wipe DB! Check Supabase RLS:", error.message);
                    } else {
                        console.log("✅ Database wiped successfully.");
                    }

                    sock.end(undefined);
                    sock = null;
                    
                    setTimeout(() => startWhatsApp(), 5000);
                    return;
                }

                if (!isReconnecting) {
                    isReconnecting = true;
                    console.log("🔄 Reconnecting in 5 seconds...");
                    setTimeout(() => {
                        isReconnecting = false;
                        startWhatsApp();
                    }, 3000);
                }
            }
        });

        sock.ev.on("messages.upsert", async ({ messages, type }) => {
            try {
                console.log(`[MSG] Received ${messages?.length || 0} messages, type: ${type}`);

                if (type !== "notify" && type !== "append") {
                    console.log(`[MSG] Skipped: unrecognized type "${type}"`);
                    return;
                }

                const msg = messages?.[0];
                if (!msg || msg.key.fromMe) {
                    console.log("[MSG] Skipped: no message object");
                    return;
                }


                if (msg.key.remoteJid === "status@broadcast") {
                    console.log("[MSG] Skipped: status update");
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
                    console.log("[MSG] Skipped: no text content. Message keys:", Object.keys(msg.message || {}));
                    return;
                }

                const contextInfo = extendedText?.contextInfo;
                const contextMessageId = contextInfo?.stanzaId || null;

                // ─── ENHANCEMENT: Group Message Deduplication ───
                if (remoteJid.endsWith("@g.us")) {
                    const participant = msg.key.participant || remoteJid;
                    if (isDuplicateGroupMessage(participant, messageText)) {
                        console.log(`[MSG] Skipped: duplicate group message from ${participant} in ${remoteJid}`);
                        return;
                    }
                }

                // ─── ENHANCEMENT 4b: Fire presence update + webhook in parallel ───
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
                
                console.log(`📤 Forwarding [${remoteJid}]: ${messageText}`);
                console.log(`📤 Webhook URL: ${SEND_WEBHOOK_URL}`);

                await sock.sendPresenceUpdate("composing", remoteJid);

                try {
                    const webhookRes = await axios.post(SEND_WEBHOOK_URL, body, {
                        headers: {
                            "Content-Type": "application/json",
                            "x-hub-signature-256": signature
                        },
                        timeout: 10000
                    });
                    console.log(`✅ Webhook sent OK: ${webhookRes.status}`);
                } catch (webhookErr) {
                    console.error(`❌ Webhook FAILED: ${webhookErr?.response?.status || 'no response'} - ${webhookErr?.response?.data ? JSON.stringify(webhookErr.response.data) : webhookErr?.message}`);
                }
                
                await sock.sendPresenceUpdate("paused", remoteJid);
                
                // ─── User registration runs in background (non-blocking) ───
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

// Inbound Messages Route via Outbound Platform Router
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

        if (!checkDailyLimits(jid)) {
            return res.status(429).json({
                error: {
                    message: "Daily message limit reached. Try again tomorrow."
                }
            });
        }

        await rateLimitOutgoingMessage();

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

                if (!jid.endsWith("@g.us")) {
                    await sock.sendPresenceUpdate("available", jid);
                    await sock.sendPresenceUpdate("composing", jid);
                    await new Promise(resolve => setTimeout(resolve, typingDuration));
                }

                result = await sock.sendMessage(jid, {
                    text: text.body
                });

                recordSentMessage(jid);

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
    res.json({ status: "running", environment: "huggingface-spaces" });
});

app.listen(PORT, () => {
    console.log(`🚀 Server safely deployed and processing requests on port ${PORT}`);
    startWhatsApp();
});
