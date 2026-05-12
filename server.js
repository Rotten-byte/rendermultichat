const crypto = require("crypto");
const http = require("http");
const path = require("path");
const tls = require("tls");

const cors = require("cors");
const express = require("express");
const { Server } = require("socket.io");
const { WebcastPushConnection } = require("tiktok-live-connector");

let LiveChat = null;
let YOUTUBE_CHAT_IMPORT_ERROR = null;
let WebSocketClient = null;
let WS_IMPORT_ERROR = null;

try {
  ({ LiveChat } = require("youtube-chat"));
} catch (error) {
  YOUTUBE_CHAT_IMPORT_ERROR = error;
}

try {
  WebSocketClient = require("ws");
} catch (error) {
  WS_IMPORT_ERROR = error;
}

const PORT = toInt(process.env.PORT, 3000, 1, 65535);
const ALLOW_ORIGIN = cleanText(process.env.ALLOW_ORIGIN, 500) || "*";
const EVENT_BUFFER_SIZE = toInt(process.env.EVENT_BUFFER_SIZE, 500, 50, 5000);
const LIKE_MILESTONE = toInt(process.env.TIKTOK_LIKE_MILESTONE, 3000, 100, 1000000);
const INGEST_TOKEN = cleanText(process.env.INGEST_TOKEN, 200);
const AUTO_TIKTOK_USERNAME = normalizeHandle(process.env.TIKTOK_USERNAME || "");
const AUTO_YOUTUBE_LIVE_ID = normalizeYouTubeLiveId(process.env.YOUTUBE_LIVE_ID || "");
const AUTO_TWITCH_CHANNEL = normalizeTwitchChannel(process.env.TWITCH_CHANNEL || "");
const AUTO_KICK_CHANNEL = normalizeKickChannel(process.env.KICK_CHANNEL || "");
const SOCKET_JOIN_PUBLISH = asBool(process.env.SOCKET_JOIN_PUBLISH, false);
const KICK_PUSHER_URL = "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679?protocol=7&client=js&version=8.4.0&flash=false";

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: ALLOW_ORIGIN,
    methods: ["GET", "POST"]
  }
});

app.use(cors({ origin: ALLOW_ORIGIN }));
app.use(express.json({ limit: "512kb" }));
app.use("/ticker-1080x42", express.static(path.join(__dirname, "public", "ticker-1080x42")));

const store = createEventStore(EVENT_BUFFER_SIZE);
const sourceStatus = {
  bridge: {
    state: "online",
    detail: "Bridge activo",
    updatedAt: Date.now()
  },
  tiktok: {
    state: AUTO_TIKTOK_USERNAME ? "starting" : "idle",
    detail: AUTO_TIKTOK_USERNAME ? `Preparando @${AUTO_TIKTOK_USERNAME}` : "Sin cuenta TikTok configurada",
    username: AUTO_TIKTOK_USERNAME,
    updatedAt: Date.now()
  },
  youtube: {
    state: AUTO_YOUTUBE_LIVE_ID ? "starting" : "idle",
    detail: AUTO_YOUTUBE_LIVE_ID ? `Preparando live ${AUTO_YOUTUBE_LIVE_ID}` : "Sin live de YouTube configurado",
    username: AUTO_YOUTUBE_LIVE_ID,
    updatedAt: Date.now()
  },
  twitch: {
    state: AUTO_TWITCH_CHANNEL ? "starting" : "idle",
    detail: AUTO_TWITCH_CHANNEL ? `Preparando canal #${AUTO_TWITCH_CHANNEL}` : "Sin canal de Twitch configurado",
    username: AUTO_TWITCH_CHANNEL,
    updatedAt: Date.now()
  },
  kick: {
    state: AUTO_KICK_CHANNEL ? "starting" : "idle",
    detail: AUTO_KICK_CHANNEL ? `Preparando canal ${AUTO_KICK_CHANNEL}` : "Sin canal de Kick configurado",
    username: AUTO_KICK_CHANNEL,
    updatedAt: Date.now()
  }
};

let bridgeTikTokSession = null;
let bridgeYouTubeSession = null;
let bridgeTwitchSession = null;
let bridgeKickSession = null;

boot();

function boot() {
  console.log("========================================");
  console.log("ROTTEN MULTISTREAM BRIDGE ACTIVO");
  console.log(`HTTP: http://0.0.0.0:${PORT}`);
  console.log(`TikTok auto source: ${AUTO_TIKTOK_USERNAME || "OFF"}`);
  console.log(`YouTube auto source: ${AUTO_YOUTUBE_LIVE_ID || "OFF"}`);
  console.log(`Twitch auto source: ${AUTO_TWITCH_CHANNEL || "OFF"}`);
  console.log(`Kick auto source: ${AUTO_KICK_CHANNEL || "OFF"}`);
  console.log("========================================");

  registerRoutes();
  registerSocketCompatibilityLayer();

  server.listen(PORT, () => {
    console.log(`Bridge escuchando en puerto ${PORT}`);
  });

  if (AUTO_TIKTOK_USERNAME) {
    bridgeTikTokSession = startBridgeTikTok(AUTO_TIKTOK_USERNAME);
  }
  if (AUTO_YOUTUBE_LIVE_ID) {
    bridgeYouTubeSession = startBridgeYouTube(AUTO_YOUTUBE_LIVE_ID);
  }
  if (AUTO_TWITCH_CHANNEL) {
    bridgeTwitchSession = startBridgeTwitch(AUTO_TWITCH_CHANNEL);
  }
  if (AUTO_KICK_CHANNEL) {
    bridgeKickSession = startBridgeKick(AUTO_KICK_CHANNEL);
  }
}

function registerRoutes() {
  app.get("/", (req, res) => {
    res.json({
      ok: true,
      name: "Rotten Multistream Bridge",
      version: "1.1.0",
      endpoints: [
        "/health",
        "/events",
        "/ingest",
        "/sources/configure",
        "/sources/disconnect-all",
        "/sources/tiktok/connect",
        "/sources/tiktok/disconnect",
        "/sources/youtube/connect",
        "/sources/youtube/disconnect",
        "/sources/twitch/connect",
        "/sources/twitch/disconnect",
        "/sources/kick/connect",
        "/sources/kick/disconnect"
      ]
    });
  });

  app.get("/health", (req, res) => {
    const snapshot = store.snapshot();
    res.json({
      ok: true,
      version: "1.1.0",
      bridge: {
        port: PORT,
        allowOrigin: ALLOW_ORIGIN,
        bufferSize: snapshot.maxSize,
        eventCount: snapshot.count,
        lastSeq: snapshot.lastSeq
      },
      sources: sourceStatus
    });
  });

  app.get("/events", (req, res) => {
    const since = toInt(req.query.since, 0, 0);
    const limit = toInt(req.query.limit, 30, 1, 100);
    const kinds = splitCsv(req.query.kinds);
    const sources = splitCsv(req.query.sources);
    const types = splitCsv(req.query.types);

    res.json(
      store.getSince({
        since,
        limit,
        kinds,
        sources,
        types
      })
    );
  });

  app.post("/ingest", requireAuth, (req, res) => {
    const body = req.body;
    const rawEvents = Array.isArray(body?.events) ? body.events : [body];
    const accepted = [];

    rawEvents.forEach((rawEvent) => {
      const event = publishNormalizedEvent(rawEvent);
      if (event) {
        accepted.push(event);
      }
    });

    res.json({
      ok: true,
      accepted: accepted.length,
      last_seq: store.lastSeq(),
      events: accepted
    });
  });

  app.post("/sources/configure", requireAuth, (req, res) => {
    const payload = req.body || {};
    const configured = configureSources({
      tiktokUsername: payload.tiktokUsername ?? payload.tiktok ?? payload.username ?? payload.tiktok_username,
      youtubeLiveId: payload.youtubeLiveId ?? payload.youtube ?? payload.liveId ?? payload.youtube_live_id,
      twitchChannel: payload.twitchChannel ?? payload.twitch ?? payload.channel ?? payload.twitch_channel,
      kickChannel: payload.kickChannel ?? payload.kick ?? payload.kick_channel
    });

    res.json({
      ok: true,
      detail: "Fuentes actualizadas",
      configured,
      sources: sourceStatus
    });
  });

  app.post("/sources/disconnect-all", requireAuth, (req, res) => {
    disconnectAllSources("Fuentes desconectadas manualmente");
    res.json({
      ok: true,
      detail: "Todas las fuentes fueron desconectadas",
      sources: sourceStatus
    });
  });

  app.post("/sources/tiktok/connect", requireAuth, (req, res) => {
    const username = normalizeHandle(req.body?.username || req.query.username || "");
    if (!username) {
      res.status(400).json({ ok: false, error: "username requerido" });
      return;
    }

    if (bridgeTikTokSession) {
      bridgeTikTokSession.stop();
      bridgeTikTokSession = null;
    }

    bridgeTikTokSession = startBridgeTikTok(username);
    res.json({
      ok: true,
      source: "tiktok",
      username,
      detail: `Conectando a @${username}`
    });
  });

  app.post("/sources/tiktok/disconnect", requireAuth, (req, res) => {
    if (bridgeTikTokSession) {
      bridgeTikTokSession.stop();
      bridgeTikTokSession = null;
    }

    setSourceStatus("tiktok", "idle", "TikTok desconectado manualmente", "");
    res.json({
      ok: true,
      source: "tiktok",
      detail: "TikTok desconectado"
    });
  });

  app.post("/sources/youtube/connect", requireAuth, (req, res) => {
    const liveId = normalizeYouTubeLiveId(req.body?.liveId || req.body?.youtubeLiveId || req.query.liveId || req.query.youtubeLiveId || "");
    if (!liveId) {
      res.status(400).json({ ok: false, error: "liveId requerido" });
      return;
    }

    bridgeYouTubeSession = restartSourceSession(
      bridgeYouTubeSession,
      "youtube",
      liveId,
      startBridgeYouTube,
      "Sin live de YouTube configurado",
      "YouTube desconectado manualmente"
    );
    res.json({
      ok: true,
      source: "youtube",
      liveId,
      detail: `Conectando al live ${liveId}`
    });
  });

  app.post("/sources/youtube/disconnect", requireAuth, (req, res) => {
    bridgeYouTubeSession = restartSourceSession(
      bridgeYouTubeSession,
      "youtube",
      "",
      startBridgeYouTube,
      "Sin live de YouTube configurado",
      "YouTube desconectado manualmente"
    );
    res.json({
      ok: true,
      source: "youtube",
      detail: "YouTube desconectado"
    });
  });

  app.post("/sources/twitch/connect", requireAuth, (req, res) => {
    const channel = normalizeTwitchChannel(req.body?.channel || req.body?.twitchChannel || req.query.channel || req.query.twitchChannel || "");
    if (!channel) {
      res.status(400).json({ ok: false, error: "channel requerido" });
      return;
    }

    bridgeTwitchSession = restartSourceSession(
      bridgeTwitchSession,
      "twitch",
      channel,
      startBridgeTwitch,
      "Sin canal de Twitch configurado",
      "Twitch desconectado manualmente"
    );
    res.json({
      ok: true,
      source: "twitch",
      channel,
      detail: `Conectando a #${channel}`
    });
  });

  app.post("/sources/twitch/disconnect", requireAuth, (req, res) => {
    bridgeTwitchSession = restartSourceSession(
      bridgeTwitchSession,
      "twitch",
      "",
      startBridgeTwitch,
      "Sin canal de Twitch configurado",
      "Twitch desconectado manualmente"
    );
    res.json({
      ok: true,
      source: "twitch",
      detail: "Twitch desconectado"
    });
  });

  app.post("/sources/kick/connect", requireAuth, (req, res) => {
    const channel = normalizeKickChannel(req.body?.channel || req.body?.kickChannel || req.query.channel || req.query.kickChannel || "");
    if (!channel) {
      res.status(400).json({ ok: false, error: "channel requerido" });
      return;
    }

    bridgeKickSession = restartSourceSession(
      bridgeKickSession,
      "kick",
      channel,
      startBridgeKick,
      "Sin canal de Kick configurado",
      "Kick desconectado manualmente"
    );
    res.json({
      ok: true,
      source: "kick",
      channel,
      detail: `Conectando a ${channel}`
    });
  });

  app.post("/sources/kick/disconnect", requireAuth, (req, res) => {
    bridgeKickSession = restartSourceSession(
      bridgeKickSession,
      "kick",
      "",
      startBridgeKick,
      "Sin canal de Kick configurado",
      "Kick desconectado manualmente"
    );
    res.json({
      ok: true,
      source: "kick",
      detail: "Kick desconectado"
    });
  });
}

function registerSocketCompatibilityLayer() {
  io.on("connection", (socket) => {
    let legacySession = null;

    socket.emit("status", "Proxy listo");

    socket.on("join", (username) => {
      const handle = normalizeHandle(username || "");

      if (!handle) {
        socket.emit("status", "Usuario no valido");
        return;
      }

      if (legacySession) {
        legacySession.stop();
        legacySession = null;
      }

      legacySession = createTikTokSession({
        username: handle,
        onStatus(text) {
          socket.emit("status", text);
        },
        onChat(event) {
          socket.emit("comment", {
            uniqueId: event.user,
            nickname: event.displayName,
            text: event.message
          });
          if (SOCKET_JOIN_PUBLISH) {
            publishNormalizedEvent(event);
          }
        },
        onGift(event) {
          socket.emit("gift", {
            uniqueId: event.user,
            nickname: event.displayName,
            giftName: event.title || event.message || "regalo",
            repeatCount: event.count || 1
          });
          if (SOCKET_JOIN_PUBLISH) {
            publishNormalizedEvent(event);
          }
        },
        onFollow(event) {
          socket.emit("follow", {
            uniqueId: event.user,
            nickname: event.displayName
          });
          if (SOCKET_JOIN_PUBLISH) {
            publishNormalizedEvent(event);
          }
        },
        onShare(event) {
          socket.emit("share", {
            uniqueId: event.user,
            nickname: event.displayName
          });
          if (SOCKET_JOIN_PUBLISH) {
            publishNormalizedEvent(event);
          }
        },
        onLike(event) {
          socket.emit("like", {
            uniqueId: event.user,
            nickname: event.displayName,
            totalLikeCount: event.count || 0
          });
          if (SOCKET_JOIN_PUBLISH) {
            publishNormalizedEvent(event);
          }
        },
        onRepost(event) {
          socket.emit("repost", {
            uniqueId: event.user,
            nickname: event.displayName
          });
          if (SOCKET_JOIN_PUBLISH) {
            publishNormalizedEvent(event);
          }
        }
      });

      legacySession.start();
    });

    socket.on("disconnect", () => {
      if (legacySession) {
        legacySession.stop();
        legacySession = null;
      }
    });
  });
}

function startBridgeTikTok(username) {
  setSourceStatus("tiktok", "starting", `Conectando a @${username}`, username);

  const session = createTikTokSession({
    username,
    onStatus(text, stateOverride) {
      setSourceStatus("tiktok", stateOverride || inferStatusState(text), text, username);
    },
    onChat(event) {
      publishNormalizedEvent(event);
    },
    onGift(event) {
      publishNormalizedEvent(event);
    },
    onFollow(event) {
      publishNormalizedEvent(event);
    },
    onShare(event) {
      publishNormalizedEvent(event);
    },
    onLike(event) {
      publishNormalizedEvent(event);
    },
    onRepost(event) {
      publishNormalizedEvent(event);
    }
  });

  session.start();
  return session;
}

function configureSources(input) {
  const configured = {
    tiktok: normalizeHandle(input?.tiktokUsername || ""),
    youtube: normalizeYouTubeLiveId(input?.youtubeLiveId || ""),
    twitch: normalizeTwitchChannel(input?.twitchChannel || ""),
    kick: normalizeKickChannel(input?.kickChannel || "")
  };

  bridgeTikTokSession = restartSourceSession(
    bridgeTikTokSession,
    "tiktok",
    configured.tiktok,
    startBridgeTikTok,
    "Sin cuenta TikTok configurada"
  );
  bridgeYouTubeSession = restartSourceSession(
    bridgeYouTubeSession,
    "youtube",
    configured.youtube,
    startBridgeYouTube,
    "Sin live de YouTube configurado"
  );
  bridgeTwitchSession = restartSourceSession(
    bridgeTwitchSession,
    "twitch",
    configured.twitch,
    startBridgeTwitch,
    "Sin canal de Twitch configurado"
  );
  bridgeKickSession = restartSourceSession(
    bridgeKickSession,
    "kick",
    configured.kick,
    startBridgeKick,
    "Sin canal de Kick configurado"
  );

  return configured;
}

function disconnectAllSources(detail) {
  bridgeTikTokSession = restartSourceSession(
    bridgeTikTokSession,
    "tiktok",
    "",
    startBridgeTikTok,
    "Sin cuenta TikTok configurada",
    detail || "TikTok desconectado manualmente"
  );
  bridgeYouTubeSession = restartSourceSession(
    bridgeYouTubeSession,
    "youtube",
    "",
    startBridgeYouTube,
    "Sin live de YouTube configurado",
    detail || "YouTube desconectado manualmente"
  );
  bridgeTwitchSession = restartSourceSession(
    bridgeTwitchSession,
    "twitch",
    "",
    startBridgeTwitch,
    "Sin canal de Twitch configurado",
    detail || "Twitch desconectado manualmente"
  );
  bridgeKickSession = restartSourceSession(
    bridgeKickSession,
    "kick",
    "",
    startBridgeKick,
    "Sin canal de Kick configurado",
    detail || "Kick desconectado manualmente"
  );
}

function restartSourceSession(currentSession, source, target, startFn, idleDetail, stopDetail) {
  if (currentSession) {
    try {
      currentSession.stop();
    } catch (error) {
      // ignore
    }
    currentSession = null;
  }

  if (!target) {
    setSourceStatus(source, "idle", stopDetail || idleDetail, "");
    return null;
  }

  return startFn(target);
}

function startBridgeYouTube(liveId) {
  if (!LiveChat) {
    setSourceStatus("youtube", "error", `youtube-chat no disponible: ${friendlyError(YOUTUBE_CHAT_IMPORT_ERROR)}`, liveId);
    return createNoopSession();
  }

  const session = {
    liveId,
    reconnectTimer: null,
    reconnectAttempt: 0,
    stopped: false,
    client: null
  };

  async function connect() {
    if (session.stopped) {
      return;
    }

    cleanupClient();
    setSourceStatus("youtube", "starting", `Conectando al live ${session.liveId}`, session.liveId);

    const client = new LiveChat({ liveId: session.liveId });
    session.client = client;

    client.on("start", (resolvedLiveId) => {
      session.reconnectAttempt = 0;
      if (resolvedLiveId) {
        session.liveId = normalizeYouTubeLiveId(resolvedLiveId) || session.liveId;
      }
      setSourceStatus("youtube", "connected", `Conectado al live ${session.liveId}`, session.liveId);
    });

    client.on("chat", (chatItem) => {
      const normalizedEvents = normalizeYouTubeChatItem(chatItem, session.liveId);
      normalizedEvents.forEach((event) => publishNormalizedEvent(event));
    });

    client.on("end", (reason) => {
      if (session.stopped) {
        return;
      }
      setSourceStatus("youtube", "warning", reason ? `Live finalizado: ${cleanText(reason, 120)}` : "Live finalizado", session.liveId);
      scheduleReconnect(`Reconectando YouTube en ${Math.round(nextReconnectDelay(session) / 1000)}s`);
    });

    client.on("error", (error) => {
      if (session.stopped) {
        return;
      }
      setSourceStatus("youtube", "error", `Error YouTube: ${friendlyError(error)}`, session.liveId);
      scheduleReconnect(`Reconectando YouTube en ${Math.round(nextReconnectDelay(session) / 1000)}s`);
    });

    try {
      const ok = await client.start();
      if (!ok) {
        throw new Error("No se pudo iniciar la lectura del chat de YouTube");
      }
    } catch (error) {
      if (session.stopped) {
        return;
      }
      setSourceStatus("youtube", "error", `Error YouTube: ${friendlyError(error)}`, session.liveId);
      scheduleReconnect(`Reconectando YouTube en ${Math.round(nextReconnectDelay(session) / 1000)}s`);
    }
  }

  function nextReconnectDelay(currentSession) {
    currentSession.reconnectAttempt += 1;
    return Math.min(30000, 3000 * currentSession.reconnectAttempt);
  }

  function scheduleReconnect(detail) {
    if (session.stopped) {
      return;
    }
    cleanupClient();
    clearTimeout(session.reconnectTimer);
    const delayMs = Math.min(30000, 3000 * Math.max(1, session.reconnectAttempt));
    setSourceStatus("youtube", "warning", detail, session.liveId);
    session.reconnectTimer = setTimeout(connect, delayMs);
  }

  function cleanupClient() {
    clearTimeout(session.reconnectTimer);
    session.reconnectTimer = null;

    if (session.client) {
      try {
        session.client.removeAllListeners();
      } catch (error) {
        // ignore
      }
      try {
        session.client.stop();
      } catch (error) {
        // ignore
      }
      session.client = null;
    }
  }

  connect();
  return {
    stop() {
      session.stopped = true;
      cleanupClient();
    }
  };
}

function startBridgeTwitch(channel) {
  const normalizedChannel = normalizeTwitchChannel(channel);
  const session = {
    channel: normalizedChannel,
    socket: null,
    reconnectTimer: null,
    reconnectAttempt: 0,
    stopped: false,
    buffer: ""
  };

  function connect() {
    if (session.stopped) {
      return;
    }

    cleanupSocket();
    setSourceStatus("twitch", "starting", `Conectando a #${session.channel}`, session.channel);

    const nickname = `justinfan${Math.floor(10000 + Math.random() * 89999)}`;
    const socket = tls.connect(
      {
        host: "irc.chat.twitch.tv",
        port: 6697,
        servername: "irc.chat.twitch.tv",
        rejectUnauthorized: true
      },
      () => {
        socket.write("PASS SCHMOOPIIE\r\n");
        socket.write(`NICK ${nickname}\r\n`);
        socket.write("CAP REQ :twitch.tv/tags twitch.tv/commands\r\n");
        socket.write(`JOIN #${session.channel}\r\n`);
        setSourceStatus("twitch", "connected", `Conectado a #${session.channel}`, session.channel);
        session.reconnectAttempt = 0;
      }
    );

    socket.setEncoding("utf8");
    socket.setTimeout(1000);
    session.socket = socket;

    socket.on("data", (chunk) => {
      session.buffer += String(chunk || "");
      while (session.buffer.includes("\r\n")) {
        const splitIndex = session.buffer.indexOf("\r\n");
        const line = session.buffer.slice(0, splitIndex);
        session.buffer = session.buffer.slice(splitIndex + 2);
        handleTwitchLine(session, line);
      }
    });

    socket.on("timeout", () => {
      // keep loop active with short socket timeout
    });

    socket.on("error", (error) => {
      if (session.stopped) {
        return;
      }
      setSourceStatus("twitch", "error", `Error Twitch: ${friendlyError(error)}`, session.channel);
      scheduleReconnect();
    });

    socket.on("close", () => {
      if (session.stopped) {
        return;
      }
      setSourceStatus("twitch", "warning", "Conexion Twitch cerrada", session.channel);
      scheduleReconnect();
    });
  }

  function scheduleReconnect() {
    if (session.stopped) {
      return;
    }
    cleanupSocket();
    clearTimeout(session.reconnectTimer);
    session.reconnectAttempt += 1;
    const delayMs = Math.min(30000, 3000 * session.reconnectAttempt);
    setSourceStatus("twitch", "warning", `Reconectando Twitch en ${Math.round(delayMs / 1000)}s`, session.channel);
    session.reconnectTimer = setTimeout(connect, delayMs);
  }

  function cleanupSocket() {
    clearTimeout(session.reconnectTimer);
    session.reconnectTimer = null;
    session.buffer = "";
    if (session.socket) {
      try {
        session.socket.removeAllListeners();
      } catch (error) {
        // ignore
      }
      try {
        session.socket.destroy();
      } catch (error) {
        // ignore
      }
      session.socket = null;
    }
  }

  connect();
  return {
    stop() {
      session.stopped = true;
      cleanupSocket();
    }
  };
}

function startBridgeKick(channel) {
  if (!WebSocketClient) {
    setSourceStatus("kick", "error", `ws no disponible: ${friendlyError(WS_IMPORT_ERROR)}`, channel);
    return createNoopSession();
  }

  const normalizedChannel = normalizeKickChannel(channel);
  const session = {
    channel: normalizedChannel,
    socket: null,
    reconnectTimer: null,
    reconnectAttempt: 0,
    stopped: false
  };

  async function connect() {
    if (session.stopped) {
      return;
    }

    cleanupSocket();
    setSourceStatus("kick", "starting", `Conectando a ${session.channel}`, session.channel);

    try {
      const kickInfo = await fetchKickChannelInfo(session.channel);
      const subscribePayload = JSON.stringify({
        event: "pusher:subscribe",
        data: {
          auth: "",
          channel: `chatrooms.${kickInfo.chatroomId}.v2`
        }
      });

      const socket = new WebSocketClient(KICK_PUSHER_URL);
      session.socket = socket;

      socket.on("open", () => {
        setSourceStatus("kick", "starting", `Suscribiendo chat de ${kickInfo.slug}`, kickInfo.slug);
      });

      socket.on("message", (rawMessage) => {
        const { eventName, data } = parseKickWsMessage(rawMessage);
        const eventNameLower = eventName.toLowerCase();

        if (eventName === "pusher:connection_established") {
          socket.send(subscribePayload);
          return;
        }

        if (eventName === "pusher:ping") {
          socket.send(JSON.stringify({ event: "pusher:pong", data: {} }));
          return;
        }

        if (eventNameLower === "pusher_internal:subscription_succeeded" || eventNameLower === "pusher:subscription_succeeded") {
          session.reconnectAttempt = 0;
          setSourceStatus("kick", "connected", `Conectado a ${kickInfo.slug}`, kickInfo.slug);
          return;
        }

        if (!eventNameLower.includes("chatmessage") && !eventNameLower.includes("chat.message")) {
          return;
        }

        const chatPayload = extractKickChatPayload(data);
        if (!chatPayload) {
          return;
        }

        publishNormalizedEvent(
          normalizeEvent({
            source: "kick",
            kind: "chat",
            type: "message",
            user: chatPayload.username,
            displayName: chatPayload.username,
            message: chatPayload.message,
            created_at: Date.now()
          })
        );
      });

      socket.on("error", (error) => {
        if (session.stopped) {
          return;
        }
        setSourceStatus("kick", "error", `Error Kick: ${friendlyError(error)}`, session.channel);
        scheduleReconnect();
      });

      socket.on("close", () => {
        if (session.stopped) {
          return;
        }
        setSourceStatus("kick", "warning", "Conexion Kick cerrada", session.channel);
        scheduleReconnect();
      });
    } catch (error) {
      if (session.stopped) {
        return;
      }
      setSourceStatus("kick", "error", `Error Kick: ${friendlyError(error)}`, session.channel);
      scheduleReconnect();
    }
  }

  function scheduleReconnect() {
    if (session.stopped) {
      return;
    }
    cleanupSocket();
    clearTimeout(session.reconnectTimer);
    session.reconnectAttempt += 1;
    const delayMs = Math.min(30000, 3000 * session.reconnectAttempt);
    setSourceStatus("kick", "warning", `Reconectando Kick en ${Math.round(delayMs / 1000)}s`, session.channel);
    session.reconnectTimer = setTimeout(connect, delayMs);
  }

  function cleanupSocket() {
    clearTimeout(session.reconnectTimer);
    session.reconnectTimer = null;
    if (session.socket) {
      try {
        session.socket.removeAllListeners();
      } catch (error) {
        // ignore
      }
      try {
        session.socket.close();
      } catch (error) {
        // ignore
      }
      session.socket = null;
    }
  }

  connect();
  return {
    stop() {
      session.stopped = true;
      cleanupSocket();
    }
  };
}

function createNoopSession() {
  return {
    stop() {
      return null;
    }
  };
}

function createTikTokSession(handlers) {
  const session = {
    username: handlers.username,
    connection: null,
    reconnectTimer: null,
    stopped: false,
    reconnectAttempt: 0,
    lastLikeMilestone: 0
  };

  async function connect() {
    if (session.stopped) {
      return;
    }

    const connection = new WebcastPushConnection(session.username);
    session.connection = connection;
    session.lastLikeMilestone = 0;

    connection.on("chat", (data) => {
      handlers.onChat?.(
        normalizeTikTokEvent("chat", data, {
          kind: "chat",
          type: "message"
        })
      );
    });

    connection.on("gift", (data) => {
      handlers.onGift?.(
        normalizeTikTokEvent("gift", data, {
          kind: "alert",
          type: "gift",
          title: data.giftName || "regalo",
          count: toInt(data.repeatCount || data.repeat_count || data.count || 1, 1, 1)
        })
      );
    });

    connection.on("follow", (data) => {
      handlers.onFollow?.(
        normalizeTikTokEvent("follow", data, {
          kind: "alert",
          type: "follow",
          title: "Nuevo follow",
          importance: "normal"
        })
      );
    });

    connection.on("share", (data) => {
      handlers.onShare?.(
        normalizeTikTokEvent("share", data, {
          kind: "alert",
          type: "share",
          title: "Compartio el directo",
          importance: "normal"
        })
      );
    });

    connection.on("social", (data) => {
      const displayType = cleanText(data?.displayType || data?.label || "", 120).toLowerCase();
      if (displayType.includes("repost")) {
        handlers.onRepost?.(
          normalizeTikTokEvent("repost", data, {
            kind: "alert",
            type: "repost",
            title: "Hizo repost",
            importance: "normal"
          })
        );
      }
    });

    connection.on("like", (data) => {
      const totalLikes = toInt(data?.totalLikeCount || data?.total_like_count || 0, 0, 0);
      if (totalLikes <= 0) {
        return;
      }

      const milestone = Math.floor(totalLikes / LIKE_MILESTONE) * LIKE_MILESTONE;
      if (milestone <= session.lastLikeMilestone || milestone <= 0) {
        return;
      }

      session.lastLikeMilestone = milestone;
      handlers.onLike?.(
        normalizeTikTokEvent("like", data, {
          kind: "alert",
          type: "like",
          title: "Likes acumulados",
          amount: String(milestone),
          count: milestone,
          importance: "normal"
        })
      );
    });

    connection.on("disconnected", () => {
      handlers.onStatus?.("Live finalizado", "warning");
      scheduleReconnect();
    });

    connection.on("error", (error) => {
      handlers.onStatus?.(`Error TikTok: ${friendlyError(error)}`, "error");
      scheduleReconnect();
    });

    try {
      handlers.onStatus?.(`Conectando a @${session.username}...`, "starting");
      await connection.connect();
      session.reconnectAttempt = 0;
      handlers.onStatus?.(`Conectado a @${session.username}`, "connected");
    } catch (error) {
      handlers.onStatus?.(`Error al conectar: ${friendlyError(error)}`, "error");
      scheduleReconnect();
    }
  }

  function scheduleReconnect() {
    if (session.stopped) {
      return;
    }

    cleanupConnection(false);
    clearTimeout(session.reconnectTimer);
    session.reconnectAttempt += 1;
    const delayMs = Math.min(30000, 3000 * session.reconnectAttempt);

    handlers.onStatus?.(`Reconectando en ${Math.round(delayMs / 1000)}s`, "warning");
    session.reconnectTimer = setTimeout(connect, delayMs);
  }

  function cleanupConnection(markStopped) {
    if (markStopped) {
      session.stopped = true;
    }

    clearTimeout(session.reconnectTimer);
    session.reconnectTimer = null;

    if (session.connection) {
      try {
        session.connection.removeAllListeners();
      } catch (error) {
        // ignore
      }
      try {
        session.connection.disconnect();
      } catch (error) {
        // ignore
      }
      session.connection = null;
    }
  }

  return {
    start: connect,
    stop() {
      cleanupConnection(true);
    }
  };
}

function handleTwitchLine(session, line) {
  if (!line) {
    return;
  }

  if (line.startsWith("PING ")) {
    try {
      session.socket?.write(line.replace("PING", "PONG") + "\r\n");
    } catch (error) {
      // ignore
    }
    return;
  }

  const parsed = parseTwitchIrcLine(line);
  if (!parsed) {
    return;
  }

  if (parsed.command === "PRIVMSG") {
    const bits = toInt(parsed.tags.bits, 0, 0);
    if (bits > 0) {
      publishNormalizedEvent(
        normalizeEvent({
          source: "twitch",
          kind: "alert",
          type: "bits",
          user: parsed.username,
          displayName: parsed.displayName,
          message: parsed.message || `${bits} bits`,
          amount: String(bits),
          count: bits,
          title: "Bits"
        })
      );
      return;
    }

    publishNormalizedEvent(
      normalizeEvent({
        source: "twitch",
        kind: "chat",
        type: "message",
        user: parsed.username,
        displayName: parsed.displayName,
        message: parsed.message,
        messageId: parsed.tags.id || "",
        userId: parsed.tags["user-id"] || "",
        created_at: Date.now()
      })
    );
    return;
  }

  if (parsed.command === "USERNOTICE") {
    const alertEvent = normalizeTwitchUserNotice(parsed);
    if (alertEvent) {
      publishNormalizedEvent(alertEvent);
    }
  }
}

function parseTwitchIrcLine(line) {
  let rest = String(line || "");
  const tags = {};

  if (rest.startsWith("@")) {
    const firstSpace = rest.indexOf(" ");
    const rawTags = rest.slice(1, firstSpace);
    rawTags.split(";").forEach((item) => {
      const [key, ...rawValue] = item.split("=");
      tags[key] = rawValue.join("=");
    });
    rest = rest.slice(firstSpace + 1);
  }

  let prefix = "";
  if (rest.startsWith(":")) {
    const firstSpace = rest.indexOf(" ");
    prefix = rest.slice(1, firstSpace);
    rest = rest.slice(firstSpace + 1);
  }

  const trailingIndex = rest.indexOf(" :");
  const commandSection = trailingIndex >= 0 ? rest.slice(0, trailingIndex) : rest;
  const trailing = trailingIndex >= 0 ? rest.slice(trailingIndex + 2) : "";
  const parts = commandSection.split(" ").filter(Boolean);
  const command = parts.shift() || "";

  const prefixUsername = prefix.includes("!") ? prefix.split("!", 1)[0] : prefix;
  const displayName = cleanUser(tags["display-name"] || tags.login || prefixUsername || "usuario");
  const username = cleanUser(tags.login || prefixUsername || displayName);

  return {
    tags,
    prefix,
    command,
    params: parts,
    message: cleanText(trailing, 220),
    displayName,
    username
  };
}

function normalizeTwitchUserNotice(parsed) {
  const msgId = slugify(parsed.tags["msg-id"] || parsed.tags.msgid || "");
  const sourceBase = {
    source: "twitch",
    kind: "alert",
    user: parsed.username,
    displayName: parsed.displayName,
    message: parsed.message,
    messageId: parsed.tags.id || "",
    userId: parsed.tags["user-id"] || "",
    created_at: Date.now()
  };

  if (msgId === "raid") {
    return normalizeEvent(
      Object.assign({}, sourceBase, {
        type: "raid",
        amount: parsed.tags["msg-param-viewerCount"] || "",
        count: toInt(parsed.tags["msg-param-viewerCount"], 0, 0),
        title: "Raid"
      })
    );
  }

  if (["sub", "resub", "subscription"].includes(msgId)) {
    return normalizeEvent(
      Object.assign({}, sourceBase, {
        type: "subscription",
        title: "Suscripcion"
      })
    );
  }

  if (["subgift", "anonsubgift"].includes(msgId)) {
    return normalizeEvent(
      Object.assign({}, sourceBase, {
        type: "gift-sub",
        title: "Sub regalada"
      })
    );
  }

  if (["submysterygift", "anonsubmysterygift"].includes(msgId)) {
    return normalizeEvent(
      Object.assign({}, sourceBase, {
        type: "community-gift",
        count: toInt(parsed.tags["msg-param-mass-gift-count"], 1, 1),
        title: "Subs regaladas"
      })
    );
  }

  return null;
}

function normalizeYouTubeChatItem(chatItem, liveId) {
  if (!chatItem) {
    return [];
  }

  const author = chatItem.author || {};
  const username = cleanUser(author.name || author.channelId || "usuario");
  const displayName = cleanUser(author.name || username);
  const message = joinYouTubeMessageParts(chatItem.message);
  const createdAt = chatItem.timestamp instanceof Date ? chatItem.timestamp.getTime() : Date.now();
  const avatar = cleanText(author.thumbnail?.url || author.thumbnail?.src || "", 500);
  const messageId = cleanText(chatItem.id || `${author.channelId || username}-${createdAt}-${message}`, 160);
  const base = {
    source: "youtube",
    user: username,
    displayName,
    userId: cleanText(author.channelId || "", 120),
    avatar,
    messageId,
    created_at: createdAt
  };

  const events = [];
  if (chatItem.superchat?.amount) {
    events.push(
      normalizeEvent(
        Object.assign({}, base, {
          kind: "alert",
          type: "superchat",
          amount: cleanText(chatItem.superchat.amount, 80),
          title: "Super Chat",
          message: message || cleanText(chatItem.superchat.amount, 80)
        })
      )
    );
  } else if (chatItem.isMembership) {
    events.push(
      normalizeEvent(
        Object.assign({}, base, {
          kind: "alert",
          type: "member",
          title: "Nuevo miembro",
          message: message || "Se unio al canal"
        })
      )
    );
  } else if (message) {
    events.push(
      normalizeEvent(
        Object.assign({}, base, {
          kind: "chat",
          type: "message",
          message
        })
      )
    );
  }

  return events.filter(Boolean);
}

function joinYouTubeMessageParts(parts) {
  if (!Array.isArray(parts)) {
    return cleanText(parts || "", 220);
  }

  const joined = parts
    .map((part) => {
      if (!part || typeof part !== "object") {
        return "";
      }
      return part.text || part.emojiText || part.alt || "";
    })
    .join("")
    .replace(/\s+/g, " ");

  return cleanText(joined, 220);
}

async function fetchKickChannelInfo(channel) {
  const slug = normalizeKickChannel(channel);
  if (!slug) {
    throw new Error("canal Kick invalido");
  }

  const response = await fetch(`https://kick.com/api/v2/channels/${slug}`, {
    method: "GET",
    headers: {
      Accept: "application/json, text/plain, */*",
      Referer: `https://kick.com/${slug}`,
      "User-Agent": "Mozilla/5.0"
    }
  });

  if (!response.ok) {
    throw new Error(`Kick HTTP ${response.status}`);
  }

  const payload = await response.json();
  const chatroomId = toInt(payload?.chatroom?.id, 0, 0);
  if (!chatroomId) {
    throw new Error("Kick no devolvio chatroom");
  }

  return {
    slug,
    chatroomId
  };
}

function parseKickWsMessage(rawMessage) {
  const payload = kickJsonValue(rawMessage);
  if (!payload || typeof payload !== "object") {
    return { eventName: "", data: {} };
  }

  return {
    eventName: cleanText(payload.event || "", 120),
    data: kickJsonValue(payload.data)
  };
}

function kickJsonValue(value) {
  if (Buffer.isBuffer(value)) {
    value = value.toString("utf8");
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return {};
    }
    try {
      return JSON.parse(trimmed);
    } catch (error) {
      return trimmed;
    }
  }

  return value;
}

function extractKickChatPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return null;
  }

  const sender = payload.sender || payload.user || payload.author || {};
  const username = cleanUser(
    sender.username ||
      sender.slug ||
      sender.name ||
      sender.display_name ||
      payload.username ||
      "usuario"
  );
  const message = kickMessageText(payload);

  if (!username || !message) {
    return null;
  }

  return { username, message };
}

function kickMessageText(payload) {
  const direct = cleanText(payload.content || payload.message || payload.text || "", 220);
  if (direct) {
    return direct.replace(/\[emote:\d+:(.*?)\]/g, "$1");
  }

  if (!Array.isArray(payload.parts)) {
    return "";
  }

  return cleanText(
    payload.parts
      .map((part) => (part && typeof part === "object" ? part.text || part.content || part.name || "" : ""))
      .join(""),
    220
  );
}

function normalizeTikTokEvent(eventName, data, overrides) {
  const user = extractTikTokUser(data);
  const message = cleanText(data?.comment || data?.text || data?.message || "", 220);
  const avatar = cleanText(
    data?.profilePictureUrl ||
      data?.profilePicture?.urls?.[0] ||
      data?.user?.profilePictureUrl ||
      data?.user?.avatarThumb?.urlList?.[0] ||
      "",
    500
  );

  return normalizeEvent(
    Object.assign(
      {
        source: "tiktok",
        user: user.uniqueId,
        displayName: user.nickname,
        avatar,
        message,
        title: eventName
      },
      overrides || {}
    )
  );
}

function extractTikTokUser(data) {
  const userObj = data?.user || data?.userInfo || {};
  const uniqueId =
    cleanText(data?.uniqueId, 80) ||
    cleanText(data?.userId, 80) ||
    cleanText(userObj?.uniqueId, 80) ||
    cleanText(userObj?.userId, 80) ||
    "usuario";

  const nickname =
    cleanText(data?.nickname, 80) ||
    cleanText(userObj?.nickname, 80) ||
    cleanText(data?.nick, 80) ||
    cleanText(userObj?.nick, 80) ||
    uniqueId;

  return { uniqueId, nickname };
}

function publishNormalizedEvent(rawEvent) {
  const event = normalizeEvent(rawEvent);
  if (!event) {
    return null;
  }

  const stored = store.push(event);
  io.emit("bridge:event", stored);
  return stored;
}

function normalizeEvent(rawEvent) {
  if (!rawEvent || typeof rawEvent !== "object") {
    return null;
  }

  const source = normalizeSource(rawEvent.source || rawEvent.platform || rawEvent.provider || "system");
  const type = normalizeType(rawEvent.type || rawEvent.eventType || rawEvent.kind || "event");
  const kind = normalizeKind(rawEvent.kind, type);
  const user = cleanUser(
    rawEvent.user ||
      rawEvent.displayName ||
      rawEvent.nickname ||
      rawEvent.nick ||
      rawEvent.username ||
      rawEvent.sender?.username ||
      rawEvent.authorDetails?.displayName ||
      "usuario"
  );
  const displayName = cleanUser(rawEvent.displayName || rawEvent.nickname || rawEvent.nick || user);
  const message = cleanText(
    rawEvent.message ||
      rawEvent.text ||
      rawEvent.comment ||
      rawEvent.content ||
      rawEvent.snippet?.displayMessage ||
      rawEvent.snippet?.textMessageDetails?.messageText ||
      "",
    220
  );
  const title = cleanText(rawEvent.title || rawEvent.giftName || rawEvent.gift_name || "", 80);
  const amount = normalizeAmount(rawEvent.amount ?? rawEvent.value ?? rawEvent.totalLikeCount ?? rawEvent.formattedAmount ?? "");
  const count = toInt(rawEvent.count ?? rawEvent.repeatCount ?? rawEvent.repeat_count ?? 0, 0, 0);
  const avatar = cleanText(
    rawEvent.avatar ||
      rawEvent.profileImageUrl ||
      rawEvent.authorDetails?.profileImageUrl ||
      rawEvent.sender?.profile_picture ||
      rawEvent.user?.avatar ||
      "",
    500
  );
  const messageId = cleanText(rawEvent.msgId || rawEvent.messageId || rawEvent.message_id || rawEvent.id || "", 160);
  const userId = cleanText(rawEvent.userId || rawEvent.user_id || rawEvent.authorDetails?.channelId || "", 120);

  return {
    id: messageId || `${source}-${type}-${crypto.randomUUID()}`,
    kind,
    source,
    type,
    user,
    displayName,
    userId,
    messageId,
    message,
    title,
    amount,
    count,
    avatar,
    importance: rawEvent.importance === "high" ? "high" : "normal",
    created_at: toInt(rawEvent.created_at || rawEvent.createdAt || Date.now(), Date.now(), 0)
  };
}

function normalizeKind(explicitKind, type) {
  const direct = slugify(explicitKind || "");
  if (direct === "chat" || direct === "alert" || direct === "system") {
    return direct;
  }
  if (type === "message" || type === "comment" || type === "chat") {
    return "chat";
  }
  if (type === "status" || type === "system") {
    return "system";
  }
  return "alert";
}

function normalizeType(input) {
  const value = slugify(input || "event");
  const aliases = {
    comment: "message",
    chat: "message",
    followlatest: "follow",
    subscriberlatest: "subscription",
    cheerlatest: "bits",
    tiplatest: "donation",
    "gift-sub": "gift-sub",
    giftsubs: "community-gift",
    "community-gift": "community-gift",
    communitygift: "community-gift",
    "super-chat": "superchat",
    "super-sticker": "supersticker",
    superchat: "superchat",
    supersticker: "supersticker",
    membership: "member"
  };

  return aliases[value] || value || "event";
}

function normalizeSource(input) {
  const value = slugify(input || "system");
  const aliases = {
    streamelements: "system",
    paypal: "system"
  };
  return aliases[value] || value || "system";
}

function setSourceStatus(source, state, detail, username) {
  sourceStatus[source] = {
    state,
    detail,
    username: username || sourceStatus[source]?.username || "",
    updatedAt: Date.now()
  };
}

function inferStatusState(text) {
  const value = cleanText(text, 120).toLowerCase();
  if (value.includes("conectado") || value.includes("activo")) {
    return "connected";
  }
  if (value.includes("reconect") || value.includes("finalizado")) {
    return "warning";
  }
  if (value.includes("error")) {
    return "error";
  }
  return "starting";
}

function createEventStore(maxSize) {
  const events = [];
  let currentSeq = 0;

  return {
    push(event) {
      currentSeq += 1;
      const stored = Object.assign({}, event, {
        seq: currentSeq
      });
      events.push(stored);
      if (events.length > maxSize) {
        events.splice(0, events.length - maxSize);
      }
      return stored;
    },

    getSince(options) {
      const kinds = new Set((options.kinds || []).map((value) => slugify(value)));
      const sources = new Set((options.sources || []).map((value) => slugify(value)));
      const types = new Set((options.types || []).map((value) => normalizeType(value)));

      const filtered = events
        .filter((event) => event.seq > options.since)
        .filter((event) => (kinds.size ? kinds.has(event.kind) : true))
        .filter((event) => (sources.size ? sources.has(event.source) : true))
        .filter((event) => (types.size ? types.has(event.type) : true))
        .slice(0, options.limit);

      return {
        ok: true,
        events: filtered,
        last_seq: currentSeq,
        oldest_seq: events.length ? events[0].seq : currentSeq
      };
    },

    snapshot() {
      return {
        count: events.length,
        lastSeq: currentSeq,
        maxSize
      };
    },

    lastSeq() {
      return currentSeq;
    }
  };
}

function requireAuth(req, res, next) {
  if (!INGEST_TOKEN) {
    next();
    return;
  }

  const headerToken = cleanText(req.headers["x-ingest-token"], 200);
  const bearerToken = cleanText((req.headers.authorization || "").replace(/^Bearer\s+/i, ""), 200);
  const queryToken = cleanText(req.query.token, 200);
  const bodyToken = cleanText(req.body?.token, 200);

  if ([headerToken, bearerToken, queryToken, bodyToken].includes(INGEST_TOKEN)) {
    next();
    return;
  }

  res.status(401).json({
    ok: false,
    error: "unauthorized"
  });
}

function cleanUser(value) {
  const text = cleanText(value, 80).replace(/^@+/, "");
  return text || "usuario";
}

function normalizeAmount(value) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : "";
  }
  return cleanText(value, 80);
}

function normalizeHandle(value) {
  return cleanText(String(value || "").replace(/^@+/, "").split("?")[0].split("/").pop() || "", 80)
    .toLowerCase()
    .replace(/[^a-z0-9._-]/g, "");
}

function normalizeYouTubeLiveId(value) {
  const raw = cleanText(value, 240);
  if (!raw) {
    return "";
  }

  try {
    const url = new URL(raw);
    const queryId = cleanText(url.searchParams.get("v") || "", 80);
    if (queryId) {
      return queryId;
    }

    const parts = url.pathname.split("/").filter(Boolean);
    const liveIndex = parts.findIndex((part) => ["live", "embed", "shorts"].includes(part));
    if (liveIndex >= 0 && parts[liveIndex + 1]) {
      return cleanText(parts[liveIndex + 1], 80);
    }

    return cleanText(parts[parts.length - 1] || "", 80);
  } catch (error) {
    return cleanText(raw.split("&")[0].split("?")[0], 80);
  }
}

function normalizeTwitchChannel(value) {
  return cleanText(value, 240)
    .toLowerCase()
    .replace(/^https?:\/\/(www\.)?twitch\.tv\//, "")
    .replace(/^www\.twitch\.tv\//, "")
    .replace(/^@+/, "")
    .split("/")[0]
    .replace(/[^a-z0-9_]/g, "");
}

function normalizeKickChannel(value) {
  return cleanText(value, 240)
    .toLowerCase()
    .replace(/^https?:\/\/(www\.)?kick\.com\//, "")
    .replace(/^www\.kick\.com\//, "")
    .replace(/^kick\.com\//, "")
    .replace(/^@+/, "")
    .split(/[/?#]/)[0]
    .replace(/[^a-z0-9_-]/g, "");
}

function splitCsv(value) {
  return String(value || "")
    .split(",")
    .map((item) => cleanText(item, 60))
    .filter(Boolean);
}

function slugify(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function cleanText(value, maxLength) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength || 180);
}

function toInt(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  let next = Number.isFinite(parsed) ? parsed : fallback;
  if (typeof min === "number") {
    next = Math.max(min, next);
  }
  if (typeof max === "number") {
    next = Math.min(max, next);
  }
  return next;
}

function asBool(value, fallback) {
  if (typeof value === "boolean") {
    return value;
  }
  const normalized = cleanText(value, 20).toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function friendlyError(error) {
  if (!error) {
    return "error desconocido";
  }
  if (typeof error === "string") {
    return cleanText(error, 160);
  }
  return cleanText(error.message || String(error), 160);
}
