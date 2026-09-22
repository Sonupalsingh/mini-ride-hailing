/**
 * Mini Uber/Ola — ride-hailing over Kafka.
 *
 * Every ride-related happening is an EVENT published to the Kafka topic
 * "ride-events", keyed by rideId (so all events for one ride stay in
 * order on the same partition). Both riders and drivers connect over
 * Socket.IO; the server's Kafka consumer picks up every event and
 * broadcasts live updates to whoever needs to see it:
 *
 *   REQUESTED  -> broadcast to every connected driver ("new ride available")
 *   ACCEPTED   -> broadcast to every driver ("this ride is taken, remove it")
 *                 + broadcast to that ride's rider ("your driver is X")
 *   COMPLETED  -> broadcast to that ride's rider + the driver who did it
 *   CANCELLED  -> broadcast to drivers (if it was still open) + the rider
 *
 * "First driver to accept wins" is enforced with an in-memory status
 * check right when the accept request comes in — good enough for a
 * single-instance demo. Running multiple replicas of this server would
 * need a shared, atomic store (e.g. Redis SETNX) to avoid two drivers
 * both winning the same ride in a race — see the README.
 */
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { Kafka, logLevel } = require("kafkajs");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;
const KAFKA_BROKERS = (process.env.KAFKA_BROKER || "kafka:29092").split(",");
const TOPIC = process.env.KAFKA_TOPIC || "ride-events";
const GROUP_ID = `ride-app-${process.env.HOSTNAME || Math.random().toString(36).slice(2)}`;

const BASE_FARE = 40;      // flat starting fare
const PER_KM_RATE = 12;    // per kilometre

const app = express();
app.use(express.json());
app.use(express.static(__dirname + "/public"));
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

const kafka = new Kafka({
  clientId: "mini-ride-hailing",
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.WARN,
  retry: { retries: 10, initialRetryTime: 1000 },
});
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: GROUP_ID });

// In-memory ride store: rideId -> ride object. Fine for a demo; swap for
// a real database (and a shared lock for multi-instance safety) in
// production. See README "Notes for production".
const rides = new Map();

function computeFare(distanceKm) {
  return Math.round(BASE_FARE + PER_KM_RATE * distanceKm);
}

async function publishEvent(type, ride) {
  await producer.send({
    topic: TOPIC,
    messages: [
      {
        key: ride.id,
        value: JSON.stringify({ type, ride, at: new Date().toISOString() }),
      },
    ],
  });
}

let ready = false;

async function startKafka() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      const { type, ride } = event;

      // Keep this instance's own in-memory view in sync with the event
      // stream (matters once you run more than one replica).
      rides.set(ride.id, ride);

      switch (type) {
        case "REQUESTED":
          io.to("drivers").emit("ride:new", ride);
          break;
        case "ACCEPTED":
          io.to("drivers").emit("ride:taken", { id: ride.id });
          io.to(`rider-${ride.riderId}`).emit("ride:accepted", ride);
          io.to(`driver-${ride.driverId}`).emit("ride:confirmed", ride);
          break;
        case "COMPLETED":
          io.to(`rider-${ride.riderId}`).emit("ride:completed", ride);
          io.to(`driver-${ride.driverId}`).emit("ride:completed", ride);
          break;
        case "CANCELLED":
          io.to("drivers").emit("ride:taken", { id: ride.id }); // remove from open list either way
          io.to(`rider-${ride.riderId}`).emit("ride:cancelled", ride);
          if (ride.driverId) {
            io.to(`driver-${ride.driverId}`).emit("ride:cancelled", ride);
          }
          break;
      }
    },
  });

  ready = true;
  console.log(`Ride app Kafka producer/consumer ready (group=${GROUP_ID}, topic=${TOPIC})`);
}

// --- REST API ---------------------------------------------------------

app.get("/health", (req, res) => {
  res.status(ready ? 200 : 503).json({ status: ready ? "ok" : "starting" });
});

// Rider requests a ride.
app.post("/api/rides", async (req, res) => {
  const { riderId, riderName, pickup, drop, distanceKm } = req.body || {};
  if (!riderId || !riderName || !pickup || !drop || !distanceKm) {
    return res.status(400).json({
      error: "riderId, riderName, pickup, drop and distanceKm are required",
    });
  }

  const ride = {
    id: crypto.randomUUID(),
    riderId,
    riderName,
    pickup,
    drop,
    distanceKm: Number(distanceKm),
    baseFare: BASE_FARE,
    perKmRate: PER_KM_RATE,
    fareEstimate: computeFare(Number(distanceKm)),
    status: "requested", // requested -> accepted -> completed | cancelled
    driverId: null,
    driverName: null,
    requestedAt: new Date().toISOString(),
  };

  rides.set(ride.id, ride);
  await publishEvent("REQUESTED", ride);
  res.status(201).json(ride);
});

// List all currently-open ride requests (for a driver's dashboard).
app.get("/api/rides/open", (req, res) => {
  const open = [...rides.values()].filter((r) => r.status === "requested");
  res.json(open);
});

// Rider's full ride history + a simple summary count.
app.get("/api/rides/rider/:riderId/history", (req, res) => {
  const list = [...rides.values()]
    .filter((r) => r.riderId === req.params.riderId)
    .sort((a, b) => new Date(b.requestedAt) - new Date(a.requestedAt));
  res.json({ totalBooked: list.length, rides: list });
});

function isToday(isoString) {
  if (!isoString) return false;
  return isoString.slice(0, 10) === new Date().toISOString().slice(0, 10);
}

// Driver's stats: today's accepted/completed rides and earnings, plus
// all-time totals. "Today" is by UTC calendar date, since this demo
// keeps everything in server-local memory with no timezone handling.
app.get("/api/rides/driver/:driverId/stats", (req, res) => {
  const mine = [...rides.values()].filter((r) => r.driverId === req.params.driverId);
  const acceptedToday = mine.filter((r) => isToday(r.acceptedAt));
  const completedToday = mine.filter((r) => isToday(r.completedAt));
  const completedAll = mine.filter((r) => r.status === "completed");

  res.json({
    today: {
      ridesAccepted: acceptedToday.length,
      ridesCompleted: completedToday.length,
      earnings: completedToday.reduce((sum, r) => sum + (r.finalFare ?? r.fareEstimate), 0),
    },
    allTime: {
      ridesAccepted: mine.length,
      ridesCompleted: completedAll.length,
      earnings: completedAll.reduce((sum, r) => sum + (r.finalFare ?? r.fareEstimate), 0),
    },
  });
});

// Find a rider's currently active ride (requested or accepted), if any.
app.get("/api/rides/rider/:riderId/current", (req, res) => {
  const active = [...rides.values()]
    .filter((r) => r.riderId === req.params.riderId && ["requested", "accepted"].includes(r.status))
    .sort((a, b) => new Date(b.requestedAt) - new Date(a.requestedAt))[0];
  res.json(active || null);
});

// Find a driver's currently active (accepted, not yet completed) ride, if any.
app.get("/api/rides/driver/:driverId/current", (req, res) => {
  const active = [...rides.values()]
    .filter((r) => r.driverId === req.params.driverId && r.status === "accepted")
    .sort((a, b) => new Date(b.acceptedAt) - new Date(a.acceptedAt))[0];
  res.json(active || null);
});

app.get("/api/rides/:id", (req, res) => {
  const ride = rides.get(req.params.id);
  if (!ride) return res.status(404).json({ error: "ride not found" });
  res.json(ride);
});

// Driver accepts a ride. First one in wins.
app.post("/api/rides/:id/accept", async (req, res) => {
  const { driverId, driverName } = req.body || {};
  if (!driverId || !driverName) {
    return res.status(400).json({ error: "driverId and driverName are required" });
  }

  const ride = rides.get(req.params.id);
  if (!ride) return res.status(404).json({ error: "ride not found" });
  if (ride.status !== "requested") {
    return res.status(409).json({ error: "ride is no longer available", status: ride.status });
  }

  ride.status = "accepted";
  ride.driverId = driverId;
  ride.driverName = driverName;
  ride.acceptedAt = new Date().toISOString();

  await publishEvent("ACCEPTED", ride);
  res.json(ride);
});

// Driver marks a ride complete. Only the driver who accepted it can do this.
app.post("/api/rides/:id/complete", async (req, res) => {
  const { driverId } = req.body || {};
  const ride = rides.get(req.params.id);
  if (!ride) return res.status(404).json({ error: "ride not found" });
  if (ride.status !== "accepted") {
    return res.status(409).json({ error: "ride is not in progress", status: ride.status });
  }
  if (!driverId || driverId !== ride.driverId) {
    return res.status(403).json({ error: "only the driver assigned to this ride can complete it" });
  }

  ride.status = "completed";
  ride.completedAt = new Date().toISOString();
  ride.finalFare = ride.fareEstimate; // flat estimate == final fare in this demo

  await publishEvent("COMPLETED", ride);
  res.json(ride);
});

// Rider cancels (only while still unmatched or in-progress). Either the
// rider who requested it, or the driver who accepted it, can cancel —
// matching how real ride-hailing apps let either side back out.
app.post("/api/rides/:id/cancel", async (req, res) => {
  const { riderId, driverId: cancellingDriverId } = req.body || {};
  const ride = rides.get(req.params.id);
  if (!ride) return res.status(404).json({ error: "ride not found" });
  if (ride.status === "completed" || ride.status === "cancelled") {
    return res.status(409).json({ error: "ride already finished", status: ride.status });
  }

  const isRider = riderId && riderId === ride.riderId;
  const isAssignedDriver = cancellingDriverId && cancellingDriverId === ride.driverId;
  if (!isRider && !isAssignedDriver) {
    return res.status(403).json({ error: "only the rider or the assigned driver can cancel this ride" });
  }

  ride.status = "cancelled";
  ride.cancelledAt = new Date().toISOString();
  ride.cancelledBy = isAssignedDriver ? "driver" : "rider";

  await publishEvent("CANCELLED", ride);
  res.json(ride);
});

// --- WebSocket rooms ---------------------------------------------------

io.on("connection", (socket) => {
  // A driver client calls this once, on connect, to start receiving
  // "new ride available" / "ride taken" broadcasts.
  socket.on("driver:online", ({ driverId }) => {
    socket.join("drivers");
    socket.join(`driver-${driverId}`);
    socket.emit("ride:open-list", [...rides.values()].filter((r) => r.status === "requested"));
  });

  // A rider client calls this once, on connect, to receive updates
  // about their own ride specifically.
  socket.on("rider:watch", ({ riderId }) => {
    socket.join(`rider-${riderId}`);
  });
});

server.listen(PORT, () => {
  console.log(`Ride app listening on :${PORT}`);
});

startKafka().catch((err) => {
  console.error("Failed to start Kafka producer/consumer:", err);
  process.exit(1);
});

process.on("SIGTERM", async () => {
  await producer.disconnect().catch(() => {});
  await consumer.disconnect().catch(() => {});
  process.exit(0);
});
