const crypto = require("crypto");
const http = require("http");

const cors = require("cors");
const express = require("express");
const { Server } = require("socket.io");
const { WebcastPushConnection } = require("tiktok-live-connector");

const PORT = toInt(process.env.PORT, 3000, 1, 65535);
const ALLOW_ORIGIN = cleanText(process.env.ALLOW_ORIGIN, 500) || "*";
const EVENT_BUFFER_SIZE = toInt(process.env.EVENT_BUFFER_SIZE, 500, 50, 5000);
const LIKE_MILESTONE = toInt(process.env.TIKTOK_LIKE_MILESTONE, 3000, 100, 1000000);
const INGEST_TOKEN = cleanText(process.env.INGEST_TOKEN, 200);
const AUTO_TIKTOK_USERNAME = normalizeHandle(process.env.TIKTOK_USERNAME || "");
const SOCKET_JOIN_PUBLISH = asBool(process.env.SOCKET_JOIN_PUBLISH, false);

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
  }
};

let bridgeTikTokSession = null;

boot();

function boot() {
  console.log("========================================");
  console.log("ROTTEN MULTISTREAM BRIDGE ACTIVO");
  console.log(`HTTP: http://0.0.0.0:${PORT}`);
  console.log(`TikTok auto source: ${AUTO_TIKTOK_USERNAME || "OFF"}`);
  console.log("========================================");

  registerRoutes();
  registerSocketCompatibilityLayer();

  server.listen(PORT, () => {
    console.log(`Bridge escuchando en puerto ${PORT}`);
  });

  if (AUTO_TIKTOK_USERNAME) {
    bridgeTikTokSession = startBridgeTikTok(AUTO_TIKTOK_USERNAME);
  }
}

function registerRoutes() {
  app.get("/", (req, res) => {
    res.json({
      ok: true,
      name: "Rotten Multistream Bridge",
      version: "1.0.0",
      endpoints: ["/health", "/events", "/ingest", "/sources/tiktok/connect", "/sources/tiktok/disconnect"]
    });
  });

  app.get("/health", (req, res) => {
    const snapshot = store.snapshot();
    res.json({
      ok: true,
      version: "1.0.0",
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
