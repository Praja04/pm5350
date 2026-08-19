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
  // Handle OEE Telemetry Topics (Supports JSON Object, Nested d.CT_PRODUCTD1, and Array values [ 10402 ])
  if (topic === 'OEE_D1' || topic.startsWith('OEE_')) {
    try {
      const payload = JSON.parse(message.toString());
      const d = payload?.d || payload;
      
      const rawOee = d?.OEE_D1 ?? d?.oee_d1;
      if (rawOee !== undefined && rawOee !== null) {
        latestOeeD1 = Array.isArray(rawOee) ? (parseInt(rawOee[0]) || 0) : (parseInt(rawOee) || 0);
      }

      const rawProduct = d?.CT_PRODUCTD1 ?? d?.ct_productd1;
      if (rawProduct !== undefined && rawProduct !== null) {
        latestCtProductD1 = Array.isArray(rawProduct) ? (parseInt(rawProduct[0]) || 0) : (parseInt(rawProduct) || 0);
      }
    } catch (e) {
      latestOeeD1 = parseInt(message.toString()) || 0;
    }
    return;
  }
  if (topic === 'CT_PRODUCTD1') {
    try {
      const payload = JSON.parse(message.toString());
      const rawProduct = payload?.d?.CT_PRODUCTD1 ?? payload?.CT_PRODUCTD1;
      latestCtProductD1 = Array.isArray(rawProduct) ? (parseInt(rawProduct[0]) || 0) : (parseInt(rawProduct) || 0);
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

// Helper function to get Shift Start Info & Boundaries
function getShiftStartInfo() {
  const now = new Date();
  const wibFormatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Jakarta',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  });
  const parts = wibFormatter.formatToParts(now);
  const getPart = (type) => parts.find(p => p.type === type)?.value || '00';

  const yyyy = getPart('year');
  const mm = getPart('month');
  const dd = getPart('day');
  let hh = parseInt(getPart('hour'), 10);
  if (hh === 24) hh = 0;
  const min = parseInt(getPart('minute'), 10);
  const totalCurrentMinutes = (hh * 60) + min;

  const dayOfWeek = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Jakarta' })).getDay();
  const isSaturday = (dayOfWeek === 6);

  let shiftName = '';
  let startHour = 6;
  let endHour = 14;

  if (isSaturday) {
    if (totalCurrentMinutes >= 360 && totalCurrentMinutes < 660) {
      shiftName = 'Shift 1 (Sabtu: 06.00 - 11.00)';
      startHour = 6; endHour = 11;
    } else if (totalCurrentMinutes >= 660 && totalCurrentMinutes < 960) {
      shiftName = 'Shift 2 (Sabtu: 11.00 - 16.00)';
      startHour = 11; endHour = 16;
    } else if (totalCurrentMinutes >= 960 && totalCurrentMinutes < 1260) {
      shiftName = 'Shift 3 (Sabtu: 16.00 - 21.00)';
      startHour = 16; endHour = 21;
    } else {
      shiftName = 'Luar Jam Kerja (Sabtu)';
      startHour = 0; endHour = 24;
    }
  } else {
    if (totalCurrentMinutes >= 360 && totalCurrentMinutes < 840) {
      shiftName = 'Shift 1 (06.00 - 14.00)';
      startHour = 6; endHour = 14;
    } else if (totalCurrentMinutes >= 840 && totalCurrentMinutes < 1320) {
      shiftName = 'Shift 2 (14.00 - 22.00)';
      startHour = 14; endHour = 22;
    } else {
      shiftName = 'Shift 3 (22.00 - 06.00)';
      startHour = 22; endHour = 6;
    }
  }

  const currentDateStr = `${yyyy}-${mm}-${dd}`;
  return {
    shiftName,
    startHour,
    endHour,
    currentDateStr,
    totalCurrentMinutes,
    isSaturday
  };
}

// Check if a database row belongs to the current active shift
function isRowInCurrentShift(row, shiftInfo) {
  if (!row || !row.machine_ts) return false;
  let dateObj = (row.machine_ts instanceof Date) ? row.machine_ts : new Date(row.machine_ts);
  if (isNaN(dateObj.getTime())) return false;

  const wibFormatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Jakarta',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  });
  const parts = wibFormatter.formatToParts(dateObj);
  const getPart = (type) => parts.find(p => p.type === type)?.value || '00';

  const yyyy = getPart('year');
  const mm = getPart('month');
  const dd = getPart('day');
  let hh = parseInt(getPart('hour'), 10);
  if (hh === 24) hh = 0;

  const rowDateStr = `${yyyy}-${mm}-${dd}`;
  const { startHour, endHour, currentDateStr } = shiftInfo;

  // jam "06.00" = data dari 05:00-06:00, jadi row dengan hour=6 BUKAN bagian Shift 1 (06:00-14:00)
  // Gunakan hh > startHour && hh <= endHour agar jam "07.00" (06:00-07:00) masuk, jam "06.00" tidak
  if (startHour < endHour) {
    return (rowDateStr === currentDateStr && hh > startHour && hh <= endHour);
  } else {
    // Shift 3 crossing midnight (22:00 - 06:00)
    // jam "23.00" = data 22:00-23:00 (Shift 3), jam "22.00" = data 21:00-22:00 (Shift 2, exclude)
    if (hh > 22) {
      if (shiftInfo.totalCurrentMinutes >= 1320) {
        return rowDateStr === currentDateStr;
      } else {
        const yesterday = new Date(Date.now() - 86400000);
        const yParts = wibFormatter.formatToParts(yesterday);
        const yStr = `${yParts.find(p => p.type==='year').value}-${yParts.find(p => p.type==='month').value}-${yParts.find(p => p.type==='day').value}`;
        return rowDateStr === yStr;
      }
    } else if (hh <= 6) {
      if (shiftInfo.totalCurrentMinutes < 360) {
        return rowDateStr === currentDateStr;
      }
    }
    return false;
  }
}

// Helper function to calculate Shift OEE % Performance & Details
function calculateShiftOeeDetails(productVal = 0, currentOeeVal = 0, pastShiftUptimeMin = 0) {
  const shiftInfo = getShiftStartInfo();
  const totalCurrentMinutes = shiftInfo.totalCurrentMinutes;
  let shiftStartMin = shiftInfo.startHour * 60;
  if (shiftInfo.startHour === 22 && totalCurrentMinutes < 360) {
    shiftStartMin = -120; // 22:00 previous day
  }

  const SPEED_DEFAULT = 42; // cycles per minute
  const LANE_MULTIPLIER = 2; // mesin 2 jalur: 1 cycle = 2 pcs
  const shiftName = shiftInfo.shiftName;

  const elapsedShiftMin = Math.max(1, totalCurrentMinutes - shiftStartMin);
  const totalUptimeShiftMin = currentOeeVal + pastShiftUptimeMin;
  const downtimeShiftMin = Math.max(0, elapsedShiftMin - totalUptimeShiftMin);

  // OEE Shift % = Total Counter / (Speed × Uptime × Lane Multiplier) × 100
  const maxUptimeCapacity = totalUptimeShiftMin * SPEED_DEFAULT * LANE_MULTIPLIER;
  const oeeShiftPct = (maxUptimeCapacity > 0)
    ? ((productVal / maxUptimeCapacity) * 100).toFixed(1)
    : '0.0';

  return {
    shift_name: shiftName,
    oee_shift_pct: parseFloat(oeeShiftPct),
    shift_elapsed_min: elapsedShiftMin,
    shift_uptime_min: totalUptimeShiftMin,
    shift_downtime_min: downtimeShiftMin,
    max_shift_capacity: SPEED_DEFAULT * LANE_MULTIPLIER * 480,
    speed_standard_ppm: SPEED_DEFAULT
  };
}

// ═══════════════════════════════════════════════════════
// ── OEE RETAIL API ENDPOINTS (FOR LARAVEL) ─────────────
// ═══════════════════════════════════════════════════════

const machineDataMap = {
  D1: { oee: 0, product: 0 },
  D10: { oee: 0, product: 0 }
};

// 1. GET Live Telemetry Status (Supports ?machine=D1 or /api/d1/status or /api/status)
app.get(['/api/status', '/api/oee/status', '/api/:machine/status', '/api/oee/:machine/status'], async (req, res) => {
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');

  let rawMachine = req.params.machine || req.query.machine || 'D1';
  let machineId = rawMachine.toUpperCase();
  if (!machineId.startsWith('D')) machineId = 'D' + machineId;
  const cleanLower = machineId.toLowerCase();
  const tableName = `oee_${cleanLower}`;
  const oeeCol = `oee_${cleanLower}`;
  const productCol = `ct_product${cleanLower}`;

  const machineData = machineDataMap[machineId] || {
    oee: (machineId === 'D1' ? latestOeeD1 : 0),
    product: (machineId === 'D1' ? latestCtProductD1 : 0)
  };

  const oeeVal = (machineId === 'D1' && latestOeeD1 > 0) ? latestOeeD1 : (machineData.oee || 0);
  let productVal = (machineId === 'D1' && latestCtProductD1 > 0) ? latestCtProductD1 : (machineData.product || 0);

  // Sum past shift uptime for current shift from database
  // NOTE: CT_PRODUCT is CUMULATIVE (not reset hourly), so productVal from MQTT is already the total shift product
  const currentShift = getShiftStartInfo();
  let pastShiftUptimeMin = 0;
  try {
    const [historyRows] = await dbPool.query(
      `SELECT * FROM \`${tableName}\` ORDER BY machine_ts DESC LIMIT 12`
    );
    if (historyRows && historyRows.length > 0) {
      const shiftRows = historyRows.filter(r => isRowInCurrentShift(r, currentShift));

      pastShiftUptimeMin = shiftRows.reduce((acc, r) => {
        const val = r[oeeCol] !== undefined ? r[oeeCol] : r.oee;
        return acc + (Number(val) || 0);
      }, 0);

      // Fallback: jika MQTT productVal = 0, ambil nilai terakhir dari DB (sudah kumulatif)
      if (productVal === 0 && shiftRows.length > 0) {
        const latestProd = shiftRows[0][productCol] !== undefined ? shiftRows[0][productCol] : shiftRows[0].ct_product;
        if (latestProd > 0) productVal = Number(latestProd);
      }
    }
  } catch (e) {
    // skip if table doesn't exist yet
  }

  // productVal sudah kumulatif shift (tidak perlu dijumlah dari DB)
  const shiftInfo = calculateShiftOeeDetails(productVal, oeeVal, pastShiftUptimeMin);

  res.json({
    success: true,
    machine_id: machineId,
    machine_name: `Mesin Retail ${machineId}`,
    oee: oeeVal,
    product: productVal,
    [`oee_${cleanLower}`]: oeeVal,
    [`ct_product${cleanLower}`]: productVal,
    ...shiftInfo,
    timestamp: new Date().toISOString()
  });
});

// 2. GET Database History & Chart Data (Supports ?machine=D1 or /api/:machine/history or /api/history)
app.get(['/api/history', '/api/oee/history', '/api/:machine/history', '/api/oee/:machine/history'], async (req, res) => {
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');

  let rawMachine = req.params.machine || req.query.machine || 'D1';
  let machineId = rawMachine.toUpperCase();
  if (!machineId.startsWith('D')) machineId = 'D' + machineId;
  const cleanLower = machineId.toLowerCase();
  const tableName = `oee_${cleanLower}`;
  const oeeCol = `oee_${cleanLower}`;
  const productCol = `ct_product${cleanLower}`;

  try {
    const [rawRows] = await dbPool.query(
      `SELECT * FROM \`${tableName}\` ORDER BY machine_ts DESC LIMIT 8`
    );

    const rows = rawRows.map(r => {
      const oeeVal = r[oeeCol] !== undefined ? r[oeeCol] : (r.oee || 0);
      const prodVal = r[productCol] !== undefined ? r[productCol] : (r.ct_product || 0);
      return {
        id: r.id,
        oee: oeeVal,
        ct_product: prodVal,
        [oeeCol]: oeeVal,
        [productCol]: prodVal,
        jam: r.jam,
        machine_ts: r.machine_ts,
        saved_at: r.saved_at,
        is_stop_shift: r.is_stop_shift !== undefined ? r.is_stop_shift : 0
      };
    });

    const chartRows = [...rows].reverse();
    const chart = {
      labels: chartRows.map(r => r.jam),
      oeeValues: chartRows.map(r => r[oeeCol] !== undefined ? r[oeeCol] : r.oee),
      productValues: chartRows.map(r => r[productCol] !== undefined ? r[productCol] : r.ct_product)
    };

    res.json({
      success: true,
      machine_id: machineId,
      machine_name: `Mesin Retail ${machineId}`,
      table_name: tableName,
      count: rows.length,
      history: rows,
      chart: chart
    });
  } catch (err) {
    console.error(`[OEE API] Error querying ${tableName} database:`, err.message);
    res.status(500).json({
      success: false,
      machine_id: machineId,
      error: err.message,
      history: [],
      chart: { labels: [], oeeValues: [], productValues: [] }
    });
  }
});

// Helper: Determine which shift a DB row belongs to based on machine_ts hour
function getShiftInfoForTimestamp(dateObj) {
  const wibFormatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Jakarta',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hour12: false
  });
  const parts = wibFormatter.formatToParts(dateObj);
  const getPart = (type) => parts.find(p => p.type === type)?.value || '00';

  let hh = parseInt(getPart('hour'), 10);
  if (hh === 24) hh = 0;
  const dateStr = `${getPart('year')}-${getPart('month')}-${getPart('day')}`;

  const wibDate = new Date(dateObj.toLocaleString('en-US', { timeZone: 'Asia/Jakarta' }));
  const dayOfWeek = wibDate.getDay();
  const isSaturday = (dayOfWeek === 6);

  let shiftNum, shiftLabel, shiftDate;

  // jam "07.00" (hh=7) = data 06:00-07:00 = Shift 1
  if (isSaturday) {
    if (hh > 6 && hh <= 11) {
      shiftNum = 1; shiftLabel = 'Shift 1 (06-11)'; shiftDate = dateStr;
    } else if (hh > 11 && hh <= 16) {
      shiftNum = 2; shiftLabel = 'Shift 2 (11-16)'; shiftDate = dateStr;
    } else if (hh > 16 && hh <= 21) {
      shiftNum = 3; shiftLabel = 'Shift 3 (16-21)'; shiftDate = dateStr;
    } else {
      shiftNum = 0; shiftLabel = 'Luar Jam Kerja'; shiftDate = dateStr;
    }
  } else {
    if (hh > 6 && hh <= 14) {
      shiftNum = 1; shiftLabel = 'Shift 1 (06-14)'; shiftDate = dateStr;
    } else if (hh > 14 && hh <= 22) {
      shiftNum = 2; shiftLabel = 'Shift 2 (14-22)'; shiftDate = dateStr;
    } else if (hh > 22) {
      shiftNum = 3; shiftLabel = 'Shift 3 (22-06)'; shiftDate = dateStr;
    } else if (hh <= 6) {
      shiftNum = 3; shiftLabel = 'Shift 3 (22-06)';
      // Shift mulai kemarin
      const yesterday = new Date(dateObj.getTime() - 86400000);
      const yParts = wibFormatter.formatToParts(yesterday);
      shiftDate = `${yParts.find(p=>p.type==='year').value}-${yParts.find(p=>p.type==='month').value}-${yParts.find(p=>p.type==='day').value}`;
    }
  }

  return { shiftNum, shiftLabel, shiftDate, shiftKey: `${shiftDate}_S${shiftNum}` };
}

// 2b. GET Shift History OEE Summary
app.get(['/api/shifts', '/api/oee/shifts', '/api/:machine/shifts', '/api/oee/:machine/shifts'], async (req, res) => {
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');

  let rawMachine = req.params.machine || req.query.machine || 'D1';
  let machineId = rawMachine.toUpperCase();
  if (!machineId.startsWith('D')) machineId = 'D' + machineId;
  const cleanLower = machineId.toLowerCase();
  const tableName = `oee_${cleanLower}`;
  const oeeCol = `oee_${cleanLower}`;
  const productCol = `ct_product${cleanLower}`;

  const SPEED = 42;
  const LANES = 2;

  try {
    const [rawRows] = await dbPool.query(
      `SELECT * FROM \`${tableName}\` ORDER BY machine_ts DESC LIMIT 120`
    );

    // Group rows by shift
    const shiftGroups = {};

    for (const row of rawRows) {
      let tsDate = (row.machine_ts instanceof Date) ? row.machine_ts : new Date(row.machine_ts);
      if (isNaN(tsDate.getTime())) continue;

      const info = getShiftInfoForTimestamp(tsDate);
      if (info.shiftNum === 0) continue; // skip luar jam kerja

      const key = info.shiftKey;
      if (!shiftGroups[key]) {
        shiftGroups[key] = {
          shift_key: key,
          shift_label: info.shiftLabel,
          shift_date: info.shiftDate,
          shift_num: info.shiftNum,
          total_uptime: 0,
          max_product: 0,
          record_count: 0,
          rows: []
        };
      }

      const oeeVal = Number(row[oeeCol] !== undefined ? row[oeeCol] : (row.oee || 0));
      const prodVal = Number(row[productCol] !== undefined ? row[productCol] : (row.ct_product || 0));

      shiftGroups[key].total_uptime += oeeVal;
      if (prodVal > shiftGroups[key].max_product) {
        shiftGroups[key].max_product = prodVal;
      }
      shiftGroups[key].record_count++;
      shiftGroups[key].rows.push({
        id: row.id,
        jam: row.jam,
        oee: oeeVal,
        ct_product: prodVal,
        machine_ts: row.machine_ts,
        is_stop_shift: row.is_stop_shift || 0
      });
    }

    // Calculate OEE% and sort by date desc
    const shifts = Object.values(shiftGroups)
      .map(g => {
        const capacity = g.total_uptime * SPEED * LANES;
        const oee = capacity > 0 ? parseFloat(((g.max_product / capacity) * 100).toFixed(1)) : 0;
        return {
          shift_key: g.shift_key,
          shift_label: g.shift_label,
          shift_date: g.shift_date,
          shift_num: g.shift_num,
          total_uptime_min: g.total_uptime,
          total_product: g.max_product,
          oee_pct: oee,
          record_count: g.record_count,
          hourly_rows: g.rows
        };
      })
      .sort((a, b) => b.shift_key.localeCompare(a.shift_key))
      .slice(0, 10); // Last 10 shifts

    res.json({
      success: true,
      machine_id: machineId,
      count: shifts.length,
      shifts
    });

  } catch (err) {
    console.error(`[OEE API] Error querying shifts from ${tableName}:`, err.message);
    res.status(500).json({ success: false, machine_id: machineId, error: err.message, shifts: [] });
  }
});

// 3. POST Trigger Reset Pulse (Supports ?machine=D1 or /api/:machine/reset)
app.post(['/api/reset', '/api/oee/reset', '/api/:machine/reset', '/api/oee/:machine/reset'], (req, res) => {
  let rawMachine = req.params.machine || req.body?.machine || req.query.machine || 'D1';
  let machineId = rawMachine.toUpperCase();
  if (!machineId.startsWith('D')) machineId = 'D' + machineId;
  const resetTopic = `RST_${machineId}`;

  mqttClient.publish(resetTopic, JSON.stringify({ [resetTopic]: [1] }), { qos: 1 }, (err) => {
    if (err) {
      return res.status(500).json({ success: false, machine_id: machineId, message: err.message });
    }
    setTimeout(() => {
      mqttClient.publish(resetTopic, JSON.stringify({ [resetTopic]: [0] }), { qos: 1 });
    }, 500);
    res.json({ success: true, machine_id: machineId, message: `Reset pulse (1 -> 0) sent to ${resetTopic}` });
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
