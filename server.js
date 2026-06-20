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
const ACCESS_TOKEN = process.env.ACCESS_TOKEN; // Token used to verify inbound API requests

// Initialize Database and Cache Connections
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const redis = new Redis(process.env.REDIS_URL);

let sock = null;
let isReconnecting = false;

// 1. Initialize Baileys In-Memory Store
//const store = makeInMemoryStore({});
// 1. Initialize Native L1 In-Memory Cache (Sub-millisecond speed)
const localChatCache = new Set();
//
let dbWriteQueue = Promise.resolve();
// 2. Custom Supabase Authentication State Engine (Replaces useMultiFileAuthState)
// Fully Optimized, Bulk-Operational Supabase Auth State Engine
async function useSupabaseAuthState() {
    // Kept for initial creds reading/writing
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
                // OPTIMIZED: Uses 1 bulk query instead of N concurrent HTTP requests
                get: async (type, ids) => {
                    const data = {};
                    if (!ids || ids.length === 0) return data;

                    const fullIds = ids.map(id => `${type}-${id}`);
                    try {
                        const { data: dbRows, error } = await supabase
                            .from("whatsapp_auth")
                            .select("id, data")
                            .in("id", fullIds);

                        if (error) {
                            console.error(`[DB Bulk Read Error - ${type}]:`, error.message);
                            return data;
                        }

                        // Map database results into a quick lookup dictionary
                        const rowMap = new Map(dbRows?.map(row => [row.id, row.data]) || []);

                        for (const id of ids) {
                            const key = `${type}-${id}`;
                            let value = rowMap.get(key);

                            if (value) {
                                // Revive Buffer structures safely
                                value = JSON.parse(JSON.stringify(value), BufferJSON.reviver);
                                if (type === "app-state-sync-key") {
                                    const { proto } = require("@whiskeysockets/baileys");
                                    value = proto.Message.AppStateSyncKeyData.fromObject(value);
                                }
                            }
                            data[id] = value || undefined;
                        }
                    } catch (err) {
                        console.error(`Bulk read mapping crash for ${type}:`, err);
                    }
                    return data;
                },
                // OPTIMIZED: Combines multiple individual inserts/deletes into single bulk operations
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
                            } else {
                                deletes.push(key);
                            }
                        }
                    }

                    // Chain this operation to the end of the global write queue
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

                    // Wait for this specific task in the queue to finish before resolving keys.set
                    await dbWriteQueue;
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

// 3. User Resolution Layer (Cascading Store -> Redis Cache -> Supabase)
async function verifyAndRegisterUser(remoteJid, remoteJidAlt, msg) {
    // Step 1: Check Native L1 Local Memory Store
    if (localChatCache.has(remoteJid)) return;

    // Step 2: Check Fast Redis Cache Layer
    const cacheKey = `user:${remoteJid}`;
    const cachedUser = await redis.get(cacheKey);
    if (cachedUser) {
        localChatCache.add(remoteJid); // Hydrate L1 cache
        return;
    }

    // Step 3: Fall back to Supabase Core Storage
    const { data: dbUser, error } = await supabase
        .from("users")
        .select("remote_jid")
        .eq("remote_jid", remoteJid)
        .single();

    if (dbUser) {
        // Hydrate both cache layers
        await redis.set(cacheKey, "true", "EX", 86400); // 24-Hour Expiration Window
        localChatCache.add(remoteJid);
        return;
    }

    // Completely New User Detected -> Save to Supabase and Cache Instantly
    await supabase.from("users").insert({
        remote_jid: remoteJid,
        remote_jid_alt: remoteJidAlt || null
    });
    
    await redis.set(cacheKey, "true", "EX", 86400);
    localChatCache.add(remoteJid);
    console.log(`✨ Registered absolute new user: ${remoteJid}`);
}

// 4. Redis Lua-script Atomic Rate Limiting (Ensures max 2 messages per second)
async function rateLimitOutgoingMessage() {
    const now = Date.now();
    const minIntervalMs = 500; // 500ms spacing = 2 messages per second max

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
            markOnlineOnConnect: true,
            syncFullHistory: false
        });


        sock.ev.on("creds.update", saveCreds);

        sock.ev.on("connection.update", async (update) => {
            const { connection, lastDisconnect} = update;
            
            if (connection === "open") {
                console.log("✅ WhatsApp Connected");
                isReconnecting = false;
            }

            if (connection === "close") {
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                console.log(`❌ Connection closed. Status: ${statusCode}`);

                // 401: Unauthorized, 405: Not Allowed/Bad Session, 403: Forbidden
                const shouldLogout =
                    statusCode === 401 ||
                    statusCode === 405 ||
                    statusCode === 403 ||
                    statusCode === DisconnectReason.loggedOut;

                if (shouldLogout) {
                    console.log("🧹 Session corrupted. Nuking Database Session Data...");
                    
                    // Explicitly clear the table and catch errors
                    const { error } = await supabase.from("whatsapp_auth").delete().neq("id", "keep_alive_placeholder");
                    if (error) {
                        console.error("🚨 CRITICAL: Failed to wipe DB! Check Supabase RLS:", error.message);
                    } else {
                        console.log("✅ Database wiped successfully.");
                    }

                    // Destroy the current socket completely
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
                if (type !== "notify") return;

                const msg = messages?.[0];
                // if (!msg || msg.key.fromMe) return;
                if (!msg.key.fromMe) return;
                if (msg.key.remoteJid.endsWith("@g.us")) return; //check what this mean

                const remoteJid = msg.key.remoteJid;
                const remoteJidAlt = msg.key.remoteJidAlt;
                const senderNumber = remoteJid.split("@")[0];

                const messageText =
                    msg.message?.conversation ||
                    msg.message?.extendedTextMessage?.text;

                if (!messageText) return;

                await sock.sendPresenceUpdate("composing", remoteJid);

                // Step 2: Build the Meta-Compatible Payload using remoteJid instead of remoteJidAlt
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
                                            {
                                                from: remoteJid,
                                                id: msg.key.id,
                                                timestamp: String(msg.messageTimestamp),
                                                type: "text",
                                                text: {
                                                    body: messageText
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    ]
                };

                console.log(`📩 Forwarding [${remoteJid}]: ${messageText}`);
                const body = JSON.stringify(payload);

                const signature = generateSignature(body, process.env.WHATSAPP_APP_SECRET);
                
                // Dispatch payload upstream to LLM handler
                await axios.post(SEND_WEBHOOK_URL, body, {
                    headers: {
                        "Content-Type": "application/json",
                        "x-hub-signature-256": signature
                    }
                });
                await sock.sendPresenceUpdate("paused", remoteJid);
                // Step 3: Trigger typing indicator back down to the specific user chat
                
                
                // Step 1: Process User Verification Flows
                await verifyAndRegisterUser(remoteJid, remoteJidAlt, msg);

            } catch (err) {
                console.error("Webhook Forwarding Error:", err?.response?.data || err);
            }
        });
    } catch (err) {
        console.error("Startup Error:", err);
    }
}

// 5. Inbound Messages Route via Outbound Platform Router
app.post("/v20.0/:phone_number_id/messages", async (req, res) => {
    try {
        // Secure Endpoint via App System Bearer Access Token validation
        const authHeader = req.headers.authorization;
        if (!authHeader || authHeader !== `Bearer ${ACCESS_TOKEN}`) {
            return res.status(401).json({
                error: { message: "Unauthorized system authorization credentials token verification failed." }
            });
        }

        const { messaging_product, to, type, text } = req.body;

        if (messaging_product !== "whatsapp" || type !== "text" || !text?.body) {
            return res.status(400).json({
                error: { message: "Invalid Meta payload format structure" }
            });
        }

        if (!sock) {
            return res.status(500).json({
                error: { message: "WhatsApp backend connection engine down" }
            });
        }

        // Apply distributed execution rate-limiting delay prior to dispatching down into WhatsApp API pipeline
        await rateLimitOutgoingMessage();

        const jid = to.includes("@") ? to : `${to}@s.whatsapp.net`;
        const result = await sock.sendMessage(jid, { text: text.body });

        return res.status(200).json({
            messaging_product: "whatsapp",
            contacts: [{ input: to, wa_id: to }],
            messages: [{ id: result.key.id }]
        });
    } catch (err) {
        console.error("Outbound Sender Error:", err);
        return res.status(500).json({
            error: { message: err?.message || "Internal Engine Error Processing Messaging Event" }
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