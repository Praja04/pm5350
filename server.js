const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const path = require('path');

// ═══════════════════════════════════════════════════════
// ── KONFIGURASI ───────────────────────────────────────
// ═══════════════════════════════════════════════════════
const MQTT_BROKER = 'mqtt://192.168.43.116:1883';
const WEB_PORT = 3000;

// ═══════════════════════════════════════════════════════
// ── EXPRESS + SOCKET.IO ───────────────────────────────
// ═══════════════════════════════════════════════════════
const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

// ═══════════════════════════════════════════════════════
// ── MQTT CLIENT ───────────────────────────────────────
// ═══════════════════════════════════════════════════════
let latestRealtime = null;
let latestCaps = null;
let capData = {};

const mqttClient = mqtt.connect(MQTT_BROKER, {
  clientId: 'nodejs_dashboard_' + Math.random().toString(16).substr(2, 8),
  reconnectPeriod: 3000,
});

mqttClient.on('connect', () => {
  console.log('[MQTT] Terhubung ke broker:', MQTT_BROKER);
  mqttClient.subscribe('pm5350/#', (err) => {
    if (!err) console.log('[MQTT] Subscribe ke pm5350/# berhasil');
  });
});

mqttClient.on('error', (err) => {
  console.error('[MQTT] Error:', err.message);
});

mqttClient.on('reconnect', () => {
  console.log('[MQTT] Reconnecting...');
});

mqttClient.on('message', (topic, message) => {
  try {
    const data = JSON.parse(message.toString());

    if (topic === 'pm5350/realtime') {
      latestRealtime = data;
      io.emit('realtime', data);
    }
    else if (topic === 'pm5350/caps') {
      latestCaps = data;
      io.emit('caps', data);
    }
    else if (topic.startsWith('pm5350/cap/')) {
      const capNum = topic.split('/')[2];
      capData[capNum] = data;
      io.emit('cap_event', data);
    }
    else if (topic === 'pm5350/status') {
      io.emit('device_status', data);
    }
  } catch (e) {
    // skip non-JSON messages
  }
});

// ═══════════════════════════════════════════════════════
// ── SOCKET.IO ─────────────────────────────────────────
// ═══════════════════════════════════════════════════════
io.on('connection', (socket) => {
  console.log('[WEB] Client connected:', socket.id);

  // Kirim data terakhir ke client baru
  if (latestRealtime) socket.emit('realtime', latestRealtime);
  if (latestCaps) socket.emit('caps', latestCaps);
  if (Object.keys(capData).length > 0) {
    Object.values(capData).forEach(d => socket.emit('cap_event', d));
  }

  socket.on('disconnect', () => {
    console.log('[WEB] Client disconnected:', socket.id);
  });
});

// ═══════════════════════════════════════════════════════
// ── START SERVER ──────────────────────────────────────
// ═══════════════════════════════════════════════════════
server.listen(WEB_PORT, () => {
  console.log('═══════════════════════════════════════════');
  console.log('  PM5350 CapBank Dashboard');
  console.log('═══════════════════════════════════════════');
  console.log(`  MQTT Broker : ${MQTT_BROKER}`);
  console.log(`  Dashboard   : http://localhost:${WEB_PORT}`);
  console.log('═══════════════════════════════════════════');
});
