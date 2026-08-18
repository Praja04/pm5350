const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const path = require('path');
require('dotenv').config();
const mysql = require('mysql2/promise');

// ═══════════════════════════════════════════════════════
// ── DATABASE POOL (OEE RETAIL) ────────────────────────
// ═══════════════════════════════════════════════════════
const dbPool = mysql.createPool({
  host: process.env.DB_HOST || '127.0.0.1',
  port: parseInt(process.env.DB_PORT || '3306'),
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASS || '',
  database: process.env.DB_NAME || 'project_utility',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

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

// Enable CORS for Laravel & External Dashboard
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

app.use(express.static(path.join(__dirname, 'public')));

// ═══════════════════════════════════════════════════════
// ── MQTT CLIENT ───────────────────────────────────────
// ═══════════════════════════════════════════════════════
let latestRealtime = null;
let latestCaps = null;
let capData = {};
let waitingForCapActive = false;

function sanitizeRealtime(d) {
  if (!d) return d;
  
  function cleanNum(val, min, max, fallback = 0) {
    const num = Number(val);
    if (isNaN(num) || num < min || num > max) {
      return fallback;
    }
    return num;
  }

  if (d.freq !== undefined) d.freq = cleanNum(d.freq, 40, 60, 0);

  if (d.pf) {
    if (d.pf.PFtot !== undefined) d.pf.PFtot = cleanNum(d.pf.PFtot, -1, 1, 0);
    if (d.pf.PFa !== undefined) d.pf.PFa = cleanNum(d.pf.PFa, -1, 1, 0);
    if (d.pf.PFb !== undefined) d.pf.PFb = cleanNum(d.pf.PFb, -1, 1, 0);
    if (d.pf.PFc !== undefined) d.pf.PFc = cleanNum(d.pf.PFc, -1, 1, 0);
  }

  if (d.cosphi) {
    if (d.cosphi.dPFtot !== undefined) d.cosphi.dPFtot = cleanNum(d.cosphi.dPFtot, -1, 1, 0);
    if (d.cosphi.dPFa !== undefined) d.cosphi.dPFa = cleanNum(d.cosphi.dPFa, -1, 1, 0);
    if (d.cosphi.dPFb !== undefined) d.cosphi.dPFb = cleanNum(d.cosphi.dPFb, -1, 1, 0);
    if (d.cosphi.dPFc !== undefined) d.cosphi.dPFc = cleanNum(d.cosphi.dPFc, -1, 1, 0);
  }

  if (d.thd_i) {
    if (d.thd_i.Ia !== undefined) d.thd_i.Ia = cleanNum(d.thd_i.Ia, 0, 1000, 0);
    if (d.thd_i.Ib !== undefined) d.thd_i.Ib = cleanNum(d.thd_i.Ib, 0, 1000, 0);
    if (d.thd_i.Ic !== undefined) d.thd_i.Ic = cleanNum(d.thd_i.Ic, 0, 1000, 0);
  }

  if (d.thd_v) {
    if (d.thd_v.Van !== undefined) d.thd_v.Van = cleanNum(d.thd_v.Van, 0, 1000, 0);
    if (d.thd_v.Vbn !== undefined) d.thd_v.Vbn = cleanNum(d.thd_v.Vbn, 0, 1000, 0);
    if (d.thd_v.Vcn !== undefined) d.thd_v.Vcn = cleanNum(d.thd_v.Vcn, 0, 1000, 0);
  }

  const limit = 1000000;
  if (d.current) {
    if (d.current.Ia !== undefined) d.current.Ia = cleanNum(d.current.Ia, -limit, limit, 0);
    if (d.current.Ib !== undefined) d.current.Ib = cleanNum(d.current.Ib, -limit, limit, 0);
    if (d.current.Ic !== undefined) d.current.Ic = cleanNum(d.current.Ic, -limit, limit, 0);
  }

  if (d.voltage_ll) {
    if (d.voltage_ll.Vab !== undefined) d.voltage_ll.Vab = cleanNum(d.voltage_ll.Vab, -limit, limit, 0);
    if (d.voltage_ll.Vbc !== undefined) d.voltage_ll.Vbc = cleanNum(d.voltage_ll.Vbc, -limit, limit, 0);
    if (d.voltage_ll.Vca !== undefined) d.voltage_ll.Vca = cleanNum(d.voltage_ll.Vca, -limit, limit, 0);
  }

  if (d.voltage_ln) {
    if (d.voltage_ln.Van !== undefined) d.voltage_ln.Van = cleanNum(d.voltage_ln.Van, -limit, limit, 0);
    if (d.voltage_ln.Vbn !== undefined) d.voltage_ln.Vbn = cleanNum(d.voltage_ln.Vbn, -limit, limit, 0);
    if (d.voltage_ln.Vcn !== undefined) d.voltage_ln.Vcn = cleanNum(d.voltage_ln.Vcn, -limit, limit, 0);
  }

  if (d.power) {
    if (d.power.Ptot !== undefined) d.power.Ptot = cleanNum(d.power.Ptot, -limit, limit, 0);
    if (d.power.Qtot !== undefined) d.power.Qtot = cleanNum(d.power.Qtot, -limit, limit, 0);
    if (d.power.Stot !== undefined) d.power.Stot = cleanNum(d.power.Stot, -limit, limit, 0);
  }

  return d;
}

let latestOeeD1 = 0;
let latestCtProductD1 = 0;

const mqttClient = mqtt.connect(MQTT_BROKER, {
  clientId: 'nodejs_pm5350_combo_' + Math.random().toString(16).substr(2, 8),
  reconnectPeriod: 3000,
});

mqttClient.on('connect', () => {
  console.log('[MQTT] Terhubung ke broker:', MQTT_BROKER);
  mqttClient.subscribe(['pm5350/#', 'OEE_D1', 'CT_PRODUCTD1'], (err) => {
    if (!err) console.log('[MQTT] Subscribe ke pm5350/#, OEE_D1, & CT_PRODUCTD1 berhasil');
  });
});

mqttClient.on('error', (err) => {
  console.error('[MQTT] Error:', err.message);
});

mqttClient.on('reconnect', () => {
  console.log('[MQTT] Reconnecting...');
});

mqttClient.on('message', (topic, message) => {
  // Handle OEE Telemetry Topics
  if (topic === 'OEE_D1') {
    try {
      const payload = JSON.parse(message.toString());
      latestOeeD1 = payload?.d?.OEE_D1 ?? payload?.OEE_D1 ?? 0;
    } catch (e) {
      latestOeeD1 = parseInt(message.toString()) || 0;
    }
    return;
  }
  if (topic === 'CT_PRODUCTD1') {
    try {
      const payload = JSON.parse(message.toString());
      latestCtProductD1 = payload?.d?.CT_PRODUCTD1 ?? payload?.CT_PRODUCTD1 ?? 0;
    } catch (e) {
      latestCtProductD1 = parseInt(message.toString()) || 0;
    }
    return;
  }

  try {
    const data = JSON.parse(message.toString());

    if (topic === 'pm5350/realtime') {
      const sanitized = sanitizeRealtime(data);
      latestRealtime = sanitized;
      io.emit('realtime', sanitized);
    }
    else if (topic === 'pm5350/caps') {
      latestCaps = data;
      io.emit('caps', data);

      if (waitingForCapActive) {
        const anyCapOn = Object.keys(data).some(key => key.startsWith('cap') && data[key] === 1);
        if (anyCapOn) {
          console.log('[Scheduler] Terdeteksi capacitor aktif saat dalam mode tunggu. Mengirim data sekarang...');
          waitingForCapActive = false;
          sendDataToAPI();
        }
      }
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
  if (isNaN(num)) return 0;
  
  // Sanity check: limit numbers to prevent database column overflow (e.g. decimal 12,3 or 20,3).
  // Glitched Modbus/MQTT readings can sometimes output massive scientific notation values like 1.2E+29.
  if (num > 1000000 || num < -1000000) {
    return 0;
  }
  
  return Number(num.toFixed(decimals));
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
    console.log('[API Sender] Tidak ada capacitor yang ON saat ini. Masuk ke mode tunggu hingga ada capacitor aktif.');
    waitingForCapActive = true;
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

// Helper function to calculate Shift OEE % Performance & Details
function calculateShiftOeeDetails(productVal = 0) {
  const now = new Date();
  const wibString = now.toLocaleString('en-US', { timeZone: 'Asia/Jakarta' });
  const wibTime = new Date(wibString);
  const day = wibTime.getDay(); // 0 = Sun, 6 = Sat, 1..5 = Mon-Fri
  const isSaturday = (day === 6);
  const hour = wibTime.getHours();
  const minute = wibTime.getMinutes();
  const totalCurrentMinutes = (hour * 60) + minute;

  const SPEED_DEFAULT = 42; // pcs per minute
  let shiftName = '';
  let shiftDurationMin = 480; // 8 hours default for Mon-Fri

  if (isSaturday) {
    shiftDurationMin = 300; // 5 hours = 300 mins
    if (totalCurrentMinutes >= 360 && totalCurrentMinutes < 660) {
      shiftName = 'Shift 1 (Sabtu: 06.00 - 11.00)';
    } else if (totalCurrentMinutes >= 660 && totalCurrentMinutes < 960) {
      shiftName = 'Shift 2 (Sabtu: 11.00 - 16.00)';
    } else if (totalCurrentMinutes >= 960 && totalCurrentMinutes < 1260) {
      shiftName = 'Shift 3 (Sabtu: 16.00 - 21.00)';
    } else {
      shiftName = 'Luar Jam Kerja (Sabtu)';
    }
  } else {
    shiftDurationMin = 480; // 8 hours default
    if (totalCurrentMinutes >= 360 && totalCurrentMinutes < 840) {
      shiftName = 'Shift 1 (06.00 - 14.00)';
    } else if (totalCurrentMinutes >= 840 && totalCurrentMinutes < 1320) {
      shiftName = 'Shift 2 (14.00 - 22.00)';
    } else {
      shiftName = 'Shift 3 (22.00 - 06.00)';
    }
  }

  const maxShiftCapacity = SPEED_DEFAULT * shiftDurationMin;
  const oeeShiftPct = maxShiftCapacity > 0 
    ? Math.min(100, (productVal / maxShiftCapacity) * 100).toFixed(1)
    : '0.0';

  return {
    shift_name: shiftName,
    oee_shift_pct: parseFloat(oeeShiftPct),
    max_shift_capacity: maxShiftCapacity,
    speed_standard_ppm: SPEED_DEFAULT
  };
}

// ═══════════════════════════════════════════════════════
// ── OEE RETAIL API ENDPOINTS (FOR LARAVEL) ─────────────
// ═══════════════════════════════════════════════════════

// 1. GET Live Telemetry Status
app.get(['/api/status', '/api/oee/status'], (req, res) => {
  const productVal = latestCtProductD1 || 0;
  const shiftInfo = calculateShiftOeeDetails(productVal);

  res.json({
    success: true,
    oee_d1: latestOeeD1,
    ct_productd1: latestCtProductD1,
    ...shiftInfo,
    timestamp: new Date().toISOString()
  });
});

// 2. GET Database History & Chart Data (Last 8 Hours & Log Table)
app.get(['/api/history', '/api/oee/history'], async (req, res) => {
  try {
    const [rows] = await dbPool.query(
      'SELECT id, oee_d1, ct_productd1, jam, machine_ts, saved_at FROM oee_d1 ORDER BY machine_ts DESC LIMIT 8'
    );

    // Chart rows ascending order
    const chartRows = [...rows].reverse();
    const chart = {
      labels: chartRows.map(r => r.jam),
      oeeValues: chartRows.map(r => r.oee_d1),
      productValues: chartRows.map(r => r.ct_productd1)
    };

    res.json({
      success: true,
      count: rows.length,
      history: rows,
      chart: chart
    });
  } catch (err) {
    console.error('[OEE API] Error querying oee_d1 database:', err.message);
    res.status(500).json({
      success: false,
      error: err.message,
      history: [],
      chart: { labels: [], oeeValues: [], productValues: [] }
    });
  }
});

// 3. POST Trigger Reset Pulse (RST_D1)
app.post(['/api/reset', '/api/oee/reset'], (req, res) => {
  mqttClient.publish('RST_D1', '1', { qos: 1 }, (err) => {
    if (err) {
      return res.status(500).json({ success: false, message: err.message });
    }
    setTimeout(() => {
      mqttClient.publish('RST_D1', '0', { qos: 1 });
    }, 500);
    res.json({ success: true, message: 'Pulse reset RST_D1 berhasil terkirim via MQTT' });
  });
});

// ═══════════════════════════════════════════════════════
// ── START SERVER ──────────────────────────────────────
// ═══════════════════════════════════════════════════════
server.listen(WEB_PORT, () => {
  console.log('═══════════════════════════════════════════');
  console.log('  PM5350 + OEE Retail Dual Dashboard Server');
  console.log('═══════════════════════════════════════════');
  console.log(`  MQTT Broker : ${MQTT_BROKER}`);
  console.log(`  Dashboard   : http://localhost:${WEB_PORT}`);
  console.log(`  OEE API     : http://localhost:${WEB_PORT}/api/status & /api/history`);
  console.log('═══════════════════════════════════════════');
  
  // Mulai penjadwalan pengiriman data ke API
  scheduleNextSend();
});
