const express = require("express");
const http = require("http");
const path = require("path");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  transports: ["websocket", "polling"]
});

const PORT = process.env.PORT || 3000;

const MAX_NAME_LENGTH = 24;
const MAX_CHANNEL_LENGTH = 10;

const users = new Map();

app.use(express.static(path.join(__dirname)));

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "NET-RADIO",
    users: users.size
  });
});

function cleanName(name) {
  if (typeof name !== "string") return "GUEST";

  name = name.trim();

  if (!name) return "GUEST";

  return name
    .replace(/[<>]/g, "")
    .substring(0, MAX_NAME_LENGTH);
}

function cleanChannel(channel) {
  if (typeof channel !== "string") {
    return "ch01";
  }

  if (!/^ch\d{2}$/.test(channel)) {
    return "ch01";
  }

  const number = Number(channel.substring(2));

  if (number < 1 || number > 20) {
    return "ch01";
  }

  return channel;
}

function getChannelUsers(channel) {
  const result = [];

  for (const [id, user] of users) {
    if (user.channel === channel) {
      result.push({
        id,
        name: user.name,
        transmitting: user.transmitting
      });
    }
  }

  return result;
}

function sendUserCount(channel) {
  const count = getChannelUsers(channel).length;

  io.to(channel).emit("user-count", {
    count
  });
}

io.on("connection", (socket) => {
  console.log("CONNECTED:", socket.id);

  users.set(socket.id, {
    name: "GUEST",
    channel: null,
    transmitting: false
  });

  socket.on("set-name", (name) => {
    const user = users.get(socket.id);

    if (!user) return;

    user.name = cleanName(name);

    if (user.channel) {
      socket.to(user.channel).emit("peer-renamed", {
        id: socket.id,
        name: user.name
      });
    }
  });

  socket.on("join-channel", (channel) => {
    const user = users.get(socket.id);

    if (!user) return;

    channel = cleanChannel(channel);

    const oldChannel = user.channel;

    if (oldChannel === channel) return;

    if (oldChannel) {
      socket.to(oldChannel).emit("peer-left", {
        id: socket.id
      });

      socket.leave(oldChannel);

      sendUserCount(oldChannel);
    }

    user.channel = channel;
    user.transmitting = false;

    socket.join(channel);

    const peers = getChannelUsers(channel)
      .filter(p => p.id !== socket.id);

    socket.emit("peer-list", {
      peers
    });

    socket.to(channel).emit("peer-joined", {
      id: socket.id,
      name: user.name
    });

    sendUserCount(channel);

    console.log(
      `[JOIN] ${user.name} -> ${channel}`
    );
  });

  socket.on("leave-channel", () => {
    const user = users.get(socket.id);

    if (!user || !user.channel) return;

    const channel = user.channel;

    socket.to(channel).emit("peer-left", {
      id: socket.id
    });

    socket.leave(channel);

    user.channel = null;
    user.transmitting = false;

    sendUserCount(channel);

    console.log(
      `[LEAVE] ${user.name} <- ${channel}`
    );
  });

  socket.on("transmit-state", (transmitting) => {
    const user = users.get(socket.id);

    if (!user || !user.channel) return;

    user.transmitting = Boolean(transmitting);

    socket.to(user.channel).emit(
      "peer-transmit-state",
      {
        id: socket.id,
        transmitting: user.transmitting
      }
    );
  });

  socket.on("webrtc-signal", (data) => {
    const user = users.get(socket.id);

    if (!user || !user.channel) return;

    if (!data || typeof data !== "object") return;

    const targetId = data.to;

    if (typeof targetId !== "string") return;

    const target = users.get(targetId);

    if (!target) return;

    if (target.channel !== user.channel) return;

    io.to(targetId).emit("webrtc-signal", {
      from: socket.id,
      type: data.type,
      payload: data.payload
    });
  });

  socket.on("disconnect", (reason) => {
    const user = users.get(socket.id);

    if (user) {
      if (user.channel) {
        socket.to(user.channel).emit("peer-left", {
          id: socket.id
        });

        sendUserCount(user.channel);
      }

      console.log(
        `[DISCONNECT] ${user.name} (${reason})`
      );
    }

    users.delete(socket.id);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("--------------------------------");
  console.log(" NET-RADIO SERVER");
  console.log("--------------------------------");
  console.log(`PORT: ${PORT}`);
  console.log(`LOCAL: http://localhost:${PORT}`);
  console.log("--------------------------------");
});
