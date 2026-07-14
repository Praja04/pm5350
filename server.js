const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const path = require('path');

// ═══════════════════════════════════════════════════════
// ── KONFIGURASI ───────────────────────────────────────
// ═══════════════════════════════════════════════════════
const MQTT_BROKER = 'mqtt://10.11.11.200:1883';
const WEB_PORT = 3000;
const API_HOST = '10.11.10.130';
const API_PORT = 8090;
const API_PATH = '/api/utility/capbank/machine-data/store';
const CAP_TYPE = process.env.CAP_TYPE || 'cap3';

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
// ── API FORWARDER (EVERY 30 MINUTES) ──────────────────
// ═══════════════════════════════════════════════════════

function getVal(obj, path, decimals = 2) {
  if (!obj) return 0;
  const parts = path.split('.');
  let val = obj;
  for (const part of parts) {
    if (val && val[part] !== undefined) {
      val = val[part];
    } else {
      return 0;
    }
  }
  if (typeof val === 'object' && val !== null) {
    return val;
  }
  const num = Number(val);
  return isNaN(num) ? 0 : Number(num.toFixed(decimals));
}

function sendHTTP(payload) {
  const dataString = JSON.stringify(payload);
  const options = {
    hostname: API_HOST,
    port: API_PORT,
    path: API_PATH,
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(dataString)
    }
  };

  console.log(`[API Sender] Mengirim data ke API pada ${new Date().toLocaleTimeString('id-ID')}...`);

  const req = http.request(options, (res) => {
    let responseData = '';
    res.on('data', (chunk) => {
      responseData += chunk;
    });
    res.on('end', () => {
      console.log(`[API Sender] Response Status: ${res.statusCode}`);
      console.log(`[API Sender] Response Body: ${responseData}`);
    });
  });

  req.on('error', (e) => {
    console.error(`[API Sender] Gagal mengirim data ke API: ${e.message}`);
  });

  req.write(dataString);
  req.end();
}

function buildPayload(tanggal, capType, current) {
  return {
    tanggal: tanggal,
    cap_type: capType,
    current: current,

    voltage_ll: {
      Vab: getVal(latestRealtime, 'voltage_ll.Vab'),
      Vbc: getVal(latestRealtime, 'voltage_ll.Vbc'),
      Vca: getVal(latestRealtime, 'voltage_ll.Vca')
    },

    voltage_ln: {
      Van: getVal(latestRealtime, 'voltage_ln.Van'),
      Vbn: getVal(latestRealtime, 'voltage_ln.Vbn'),
      Vcn: getVal(latestRealtime, 'voltage_ln.Vcn')
    },

    power: {
      Ptot: getVal(latestRealtime, 'power.Ptot'),
      Qtot: getVal(latestRealtime, 'power.Qtot'),
      Stot: getVal(latestRealtime, 'power.Stot')
    },

    pf: {
      PFa: getVal(latestRealtime, 'pf.PFa', 4),
      PFb: getVal(latestRealtime, 'pf.PFb', 4),
      PFc: getVal(latestRealtime, 'pf.PFc', 4)
    },

    cosphi: {
      dPFa: getVal(latestRealtime, 'cosphi.dPFa', 4),
      dPFb: getVal(latestRealtime, 'cosphi.dPFb', 4),
      dPFc: getVal(latestRealtime, 'cosphi.dPFc', 4)
    },

    freq: getVal(latestRealtime, 'freq'),

    thd_i: {
      Ia: getVal(latestRealtime, 'thd_i.Ia'),
      Ib: getVal(latestRealtime, 'thd_i.Ib'),
      Ic: getVal(latestRealtime, 'thd_i.Ic')
    },

    thd_v: {
      Van: getVal(latestRealtime, 'thd_v.Van'),
      Vbn: getVal(latestRealtime, 'thd_v.Vbn'),
      Vcn: getVal(latestRealtime, 'thd_v.Vcn')
    }
  };
}

function sendDataToAPI() {
  if (!latestRealtime) {
    console.warn('[API Sender] Tidak dapat mengirim data: belum ada data realtime dari MQTT.');
    return;
  }
  if (!latestCaps) {
    console.warn('[API Sender] Tidak dapat mengirim data: belum ada data caps dari MQTT.');
    return;
  }

  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const date = String(now.getDate()).padStart(2, '0');
  const tanggal = `${year}-${month}-${date}`;

  let sentCount = 0;
  for (let i = 1; i <= 12; i++) {
    const isCapOn = latestCaps[`cap${i}`] === 1;
    if (isCapOn) {
      const capInfo = capData[String(i)];
      let phase = (capInfo && capInfo.phase) ? capInfo.phase.toUpperCase() : '';
      if (!phase) {
        // Fallback: ganjil = A, genap = C
        phase = (i % 2 !== 0) ? 'A' : 'C';
      }

      let capCurrent = 0;
      if (phase === 'A') {
        capCurrent = getVal(latestRealtime, 'current.Ia');
      } else if (phase === 'B') {
        capCurrent = getVal(latestRealtime, 'current.Ib');
      } else if (phase === 'C') {
        capCurrent = getVal(latestRealtime, 'current.Ic');
      }

      const payload = buildPayload(tanggal, `cap${i}`, capCurrent);
      sendHTTP(payload);
      sentCount++;
    }
  }

  if (sentCount === 0) {
    console.log('[API Sender] Tidak ada capacitor yang ON saat ini. Mengirim data power meter default (cap_type: null)...');
    const payload = buildPayload(tanggal, null, 0);
    sendHTTP(payload);
  } else {
    console.log(`[API Sender] Berhasil mengirim ${sentCount} data capacitor yang aktif ke API.`);
  }
}


function scheduleNextSend() {
  const now = new Date();
  const next = new Date(now);

  const minutes = now.getMinutes();
  if (minutes < 30) {
    next.setMinutes(30, 0, 0);
  } else {
    next.setMinutes(0, 0, 0);
    next.setHours(now.getHours() + 1);
  }

  const msToNext = next.getTime() - now.getTime();
  
  console.log(`[Scheduler] Pengiriman data berikutnya dijadwalkan pada ${next.toLocaleTimeString('id-ID')} (dalam ${Math.round(msToNext / 1000)} detik)`);

  setTimeout(() => {
    sendDataToAPI();
    // Wait 2 seconds before scheduling the next check to prevent double-firing
    // due to early setTimeout execution.
    setTimeout(scheduleNextSend, 2000);
  }, msToNext);
}

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
  
  // Mulai penjadwalan pengiriman data ke API
  scheduleNextSend();
});
