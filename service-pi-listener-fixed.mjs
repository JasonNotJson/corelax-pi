// /opt/corelax-service-pi/service-pi-listener.mjs
// Corelax Service Pi Listener (realtime-first + adaptive polling + alias compatibility)

import "dotenv/config";
import WebSocket from "ws";
import mqtt from "mqtt";
import { createClient } from "@supabase/supabase-js";
globalThis.WebSocket = WebSocket;

// ---------- ENV ----------
const ENV = {
  SUPABASE_URL: process.env.SUPABASE_URL,
  SUPABASE_ANON_KEY: process.env.SUPABASE_ANON_KEY,
  PI_EMAIL: process.env.PI_EMAIL,
  PI_PASSWORD: process.env.PI_PASSWORD,
  DEVICE_ID: process.env.DEVICE_ID || "svc-mukougaoka-01",
  SITE_ID: process.env.SITE_ID || "b11e8011-77e4-4e39-a320-d9685cdb27e0",
  MQTT_URL: process.env.MQTT_URL || "mqtt://127.0.0.1:1883",
  MQTT_USER: process.env.MQTT_USER || "servicepi",
  MQTT_PASS: process.env.MQTT_PASS || "kk1kxL",
  POLL_MS_BASE: parseInt(process.env.POLL_MS || "30000", 10),
  DEBUG_MQTT_ECHO: process.env.DEBUG_MQTT_ECHO === "1",
};

for (const k of ["SUPABASE_URL", "SUPABASE_ANON_KEY", "PI_EMAIL", "PI_PASSWORD"]) {
  if (!ENV[k]) {
    console.error("[FATAL] Missing env:", k);
    process.exit(1);
  }
}

// ---------- SUPABASE ----------
const supabase = createClient(ENV.SUPABASE_URL, ENV.SUPABASE_ANON_KEY, {
  auth: { persistSession: true, autoRefreshToken: true },
  realtime: { params: { eventsPerSecond: 10 }, timeout: 15000 },
});

// ---------- MQTT ----------
const mqttClient = mqtt.connect(ENV.MQTT_URL, {
  username: ENV.MQTT_USER,
  password: ENV.MQTT_PASS,
  keepalive: 60,
  reconnectPeriod: 2000,
});

let mqttReady = false;
mqttClient.on("connect", () => {
  mqttReady = true;
  console.log("[OK] MQTT connected");
});
mqttClient.on("reconnect", () => console.log("[...] MQTT reconnecting"));
mqttClient.on("close", () => {
  mqttReady = false;
  console.log("[X] MQTT closed");
});
mqttClient.on("error", (e) => console.error("MQTT error", e?.message || e));

if (ENV.DEBUG_MQTT_ECHO) {
  const canonDebug = `site/${ENV.SITE_ID}/#`;
  mqttClient.subscribe(canonDebug, { qos: 0 }, (err) => {
    if (err) console.error("MQTT debug subscribe error:", err?.message || err);
    else console.log("[DEBUG] Debug subscribed:", canonDebug);
  });
  mqttClient.on("message", (topic, msg) => {
    try {
      console.log("[DEBUG] IN", topic, msg.toString());
    } catch {}
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const publishJson = (topic, obj) =>
  new Promise((res, rej) =>
    mqttClient.publish(topic, JSON.stringify(obj), { qos: 1, retain: false }, (err) => (err ? rej(err) : res()))
  );

const publishString = (topic, str) =>
  new Promise((res, rej) =>
    mqttClient.publish(topic, String(str), { qos: 1, retain: false }, (err) => (err ? rej(err) : res()))
  );

// ---------- Topic helpers ----------
const canonicalTopic = (target) => `site/${ENV.SITE_ID}/esp/${target}/cmd`;
const doorAlias = () => "door/control";
const waterAlias = () => "water/control";
const fanAlias = () => "fan/control";
const isFanTarget = (t) => String(t || "").toLowerCase().includes("fan");

function chairAlias(target) {
  const m = String(target || "").toLowerCase().match(/(?:esp-)?chair-?(\d+)/);
  if (!m) return null;
  return `chair${parseInt(m[1], 10)}/control`;
}

function normalizeType(raw) {
  const t = String(raw || "").trim().toUpperCase();
  switch (t) {
    case "OPEN_DOOR":
    case "DOOR_OPEN":
    case "OPEN_DOOR_COMMAND":
      return "open_door";
    case "DISPENSE_WATER":
    case "WATER_ACTIVATE":
    case "WATER_PULSE":
      return "water_pulse";
    case "CHAIR_START":
    case "CHAIR_ACTIVATE":
    case "CHAIR_START_MINUTES":
    case "START":
      return "chair_start";
    case "STOP":
    case "CHAIR_STOP":
    case "CHAIR_DEACTIVATE":
      return "stop";
    case "FAN_START":
    case "START_FAN":
      return "fan_start";
    case "FAN_STOP":
    case "STOP_FAN":
      return "fan_stop";
    default:
      return "unknown";
  }
}

// ---------- De-dupe ----------
const processed = new Set();
function markProcessed(id) {
  processed.add(id);
  if (processed.size > 2000) {
    const it = processed.values();
    for (let i = 0; i < 500; i++) processed.delete(it.next().value);
  }
}

// ---------- Command processing ----------
async function completeCommand(id, success, errMsg = null) {
  const { error } = await supabase.rpc("complete_command", {
    p_command_id: id,
    p_success: !!success,
    p_err: errMsg,
  });
  if (error) console.error("complete_command RPC error:", error.message);
}

async function processCommand(cmd) {
  const id = cmd?.id;
  if (!id || processed.has(id)) return;

  const type = normalizeType(cmd?.command_type);
  const p = cmd?.payload || {};
  console.log("[CMD]", id, type, JSON.stringify(p));

  const { data: claimed, error: claimErr } = await supabase.rpc("ack_command", { p_command_id: id });
  if (claimErr) {
    console.error("ack_command error:", claimErr.message);
    return;
  }
  if (!claimed) {
    console.warn("[SKIP] skip (already claimed/invalid status)");
    return;
  }

  try {
    const target = p.target || p.machine_id || "";
    let publishedOk = false;

    if (type === "open_door") {
      const pulse_ms = p.pulse_ms ?? p.args?.pulse_ms ?? 2000;
      const tgt = target || "esp-door-01";
      const canon = canonicalTopic(tgt);
      const msg = { id, action: "OPEN_DOOR", args: { pulse_ms } };
      await Promise.allSettled([
        publishJson(canon, msg).then(() => { publishedOk = true; }),
        publishJson(doorAlias(), msg).then(() => { publishedOk = true; }),
      ]);
      console.log(`MQTT -> ${canon} & ${doorAlias()} ${JSON.stringify(msg)}`);
    } else if (type === "water_pulse") {
      const pulse_ms = p.pulse_ms ?? p.args?.pulse_ms ?? 2000;
      const tgt = target || "esp-cooler-01";
      const canon = canonicalTopic(tgt);
      const msg = { id, action: "DISPENSE_WATER", args: { pulse_ms } };
      await Promise.allSettled([
        publishJson(canon, msg).then(() => { publishedOk = true; }),
        publishJson(waterAlias(), msg).then(() => { publishedOk = true; }),
        publishString(waterAlias(), "START").then(() => { publishedOk = true; }).catch(() => {}),
      ]);
      console.log(`MQTT -> ${canon} & ${waterAlias()} ${JSON.stringify(msg)} (+ 'START')`);
    } else if (type === "fan_start" || (type === "chair_start" && isFanTarget(target))) {
      const tgt = target || "esp-fan-01";
      const canon = canonicalTopic(tgt);
      await Promise.allSettled([
        publishString(fanAlias(), "START").then(() => { publishedOk = true; }),
        publishJson(canon, { id, action: "START", args: {} }).then(() => { publishedOk = true; }).catch(() => {}),
      ]);
      console.log(`MQTT -> ${fanAlias()} 'START' (+ ${canon} JSON)`);
    } else if (type === "fan_stop" || (type === "stop" && isFanTarget(target))) {
      const tgt = target || "esp-fan-01";
      const canon = canonicalTopic(tgt);
      await Promise.allSettled([
        publishString(fanAlias(), "STOP").then(() => { publishedOk = true; }),
        publishJson(canon, { id, action: "STOP", args: {} }).then(() => { publishedOk = true; }).catch(() => {}),
      ]);
      console.log(`MQTT -> ${fanAlias()} 'STOP' (+ ${canon} JSON)`);
    } else if (type === "chair_start") {
      const minutes = Math.max(1, p.minutes ?? p.chairMinutes ?? p.args?.minutes ?? 15);
      const tgt = target || "esp-chair-03";
      const canon = canonicalTopic(tgt);
      const alias = chairAlias(tgt);
      const msg = { id, action: "START", args: { minutes } };
      await Promise.allSettled([
        publishJson(canon, msg).then(() => { publishedOk = true; }),
        alias ? publishString(alias, "START").then(() => { publishedOk = true; }) : Promise.resolve(),
      ]);
      console.log(`MQTT -> ${canon} ${JSON.stringify(msg)}${alias ? ` (+ ${alias}: 'START')` : ""}`);
    } else if (type === "stop") {
      const tgt = target || "esp-chair-03";
      const canon = canonicalTopic(tgt);
      const alias = chairAlias(tgt);
      const off_ms = p.off_ms ?? p.args?.off_ms ?? 2000;
      const on_ms = p.on_ms ?? p.args?.on_ms ?? 20000;
      const msg = { id, action: "STOP", args: { off_ms, on_ms } };
      await Promise.allSettled([
        publishJson(canon, msg).then(() => { publishedOk = true; }),
        alias ? publishString(alias, "STOP").then(() => { publishedOk = true; }) : Promise.resolve(),
      ]);
      console.log(`MQTT -> ${canon} ${JSON.stringify(msg)}${alias ? ` (+ ${alias}: 'STOP')` : ""}`);
    } else {
      throw new Error("unknown/unsupported command_type: " + (cmd?.command_type || ""));
    }

    if (!publishedOk) throw new Error("MQTT publish failed (no path succeeded)");
    await completeCommand(id, true, null);
    console.log("[OK] completed", String(id).slice(0, 8));
  } catch (e) {
    console.error("[ERROR] command error", e?.message || e);
    await completeCommand(id, false, String(e?.message || e));
  } finally {
    markProcessed(id);
  }
}

// ---------- Polling (safety net) ----------
let pollTimer = null;
let pollingMs = ENV.POLL_MS_BASE;

function startPolling(ms = ENV.POLL_MS_BASE) {
  if (pollTimer) clearInterval(pollTimer);
  pollingMs = ms;
  pollTimer = setInterval(pollPending, pollingMs);
  console.log(`[POLL] Polling every ${pollingMs}ms (safety-net)`);
}

async function pollPending() {
  try {
    const { data: rows, error } = await supabase.rpc("pending_for_device", { p_device_id: ENV.DEVICE_ID });
    if (error) {
      console.error("pending_for_device:", error.message);
      return;
    }
    if (rows?.length) {
      console.log(`[POLL] Poll picked up ${rows.length} command(s)`);
      for (const r of rows) {
        await processCommand(r);
        await sleep(120);
      }
    }
  } catch (e) {
    console.error("poll error:", e?.message || e);
  }
}

// ---------- Realtime ----------
async function subscribeRealtime() {
  return supabase
    .channel(`svc:${ENV.DEVICE_ID}`)
    .on("postgres_changes", {
      event: "INSERT",
      schema: "public",
      table: "commands",
      filter: `device_id=eq.${ENV.DEVICE_ID}`,
    }, (payload) => {
      if (payload?.new) processCommand(payload.new);
    })
    .subscribe((status) => console.log("[RT] Realtime status:", status));
}

// ---------- Boot ----------
async function boot() {
  const { data: auth, error } = await supabase.auth.signInWithPassword({
    email: ENV.PI_EMAIL,
    password: ENV.PI_PASSWORD,
  });
  if (error) {
    console.error("[FATAL] Auth failed:", error.message);
    process.exit(1);
  }

  const tok = auth?.session?.access_token;
  if (tok) supabase.realtime.setAuth(tok);
  supabase.auth.onAuthStateChange((_evt, session) => {
    const t = session?.access_token;
    if (t) supabase.realtime.setAuth(t);
  });

  await pollPending();
  startPolling(ENV.POLL_MS_BASE);
  await subscribeRealtime();

  if (!mqttReady) {
    console.log("[WAIT] Waiting for MQTT connect...");
    for (let i = 0; i < 10 && !mqttReady; i++) await sleep(300);
  }
  console.log("[READY] Service Pi listener ready:", {
    device: ENV.DEVICE_ID,
    site: ENV.SITE_ID,
    mqttReady,
    pollMs: pollingMs,
  });
}

process.on("SIGINT", () => {
  try {
    mqttClient.end(true);
  } catch {}
  process.exit(0);
});

process.on("SIGTERM", () => {
  try {
    mqttClient.end(true);
  } catch {}
  process.exit(0);
});

boot().catch((e) => {
  console.error("Fatal:", e);
  process.exit(1);
});