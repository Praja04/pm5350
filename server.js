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

// ═══════════════════════════════════════════════════════
// ── MODULAR MACHINE TELEMETRY STORE (D1..D10, S1..S5, P1..P5)
// ═══════════════════════════════════════════════════════
const machineStateStore = {};

function getMachineState(machineId) {
  let key = (machineId || 'D1').toUpperCase().trim();
  if (/^\d+$/.test(key)) key = 'D' + key;
  if (!machineStateStore[key]) {
    machineStateStore[key] = {
      oee: 0,
      product: 0,
      lastUpdated: null
    };
  }
  return machineStateStore[key];
}

function updateMachineState(machineId, oee, product) {
  const state = getMachineState(machineId);
  if (oee !== undefined && oee !== null) state.oee = parseInt(oee) || 0;
  if (product !== undefined && product !== null) state.product = parseInt(product) || 0;
  state.lastUpdated = new Date();
}

function extractNumericValue(payloadStr, machineCode, targetType) {
  const codeLower = machineCode.toLowerCase();
  const codeUpper = machineCode.toUpperCase();
  const trimmed = (payloadStr || '').toString().trim();

  // 1. Direct Numeric string (e.g. "45", "1500")
  if (/^-?\d+(?:\.\d+)?$/.test(trimmed)) {
    return Math.floor(parseFloat(trimmed));
  }

  // 2. JSON Payload
  try {
    const payload = JSON.parse(trimmed);
    if (typeof payload === 'number') return Math.floor(payload);
    
    const d = payload?.d || payload;
    if (typeof d === 'number') return Math.floor(d);

    if (typeof d === 'object' && d !== null) {
      if (targetType === 'oee') {
        const val = d[`OEE_${codeUpper}`] ?? d[`oee_${codeLower}`] ?? d.oee ?? d.OEE ?? d.value ?? d.val ?? d.data;
        if (val !== undefined && val !== null) {
          return Array.isArray(val) ? (parseInt(val[0]) || 0) : (parseInt(val) || 0);
        }
      } else {
        const val = d[`CT_PRODUCT${codeUpper}`] ?? d[`ct_product${codeLower}`] ?? d.ct_product ?? d.CT_PRODUCT ?? d.product ?? d.count ?? d.qty ?? d.value ?? d.val ?? d.data;
        if (val !== undefined && val !== null) {
          return Array.isArray(val) ? (parseInt(val[0]) || 0) : (parseInt(val) || 0);
        }
      }

      // Fallback: Check all keys for first numeric value
      for (const key of Object.keys(d)) {
        if (typeof d[key] === 'number') return Math.floor(d[key]);
        if (typeof d[key] === 'string' && /^\d+$/.test(d[key])) return parseInt(d[key]);
      }
    }
  } catch (e) {}

  return parseInt(trimmed) || 0;
}

const mqttClient = mqtt.connect(MQTT_BROKER, {
  clientId: 'nodejs_pm5350_combo_' + Math.random().toString(16).substr(2, 8),
  reconnectPeriod: 3000,
});

mqttClient.on('connect', () => {
  console.log('[MQTT] Terhubung ke broker:', MQTT_BROKER);

  // Generate explicit MQTT spec compliant topic list
  const subTopics = ['pm5350/#', 'OEE/#', 'CT_PRODUCT/#', 'oee/#', 'ct_product/#'];
  for (let i = 1; i <= 20; i++) {
    subTopics.push(`OEE_D${i}`, `CT_PRODUCTD${i}`, `OEE_D${i}/#`, `CT_PRODUCTD${i}/#`, `OEE/D${i}`, `CT_PRODUCT/D${i}`);
  }
  for (let i = 1; i <= 5; i++) {
    subTopics.push(`OEE_S${i}`, `CT_PRODUCTS${i}`, `OEE_P${i}`, `CT_PRODUCTP${i}`);
  }

  mqttClient.subscribe(subTopics, (err) => {
    if (!err) console.log('[MQTT] Subscribe ke modular OEE & CT_PRODUCT topics berhasil (D1..D20, S1..S5, P1..P5)');
  });
});

mqttClient.on('error', (err) => {
  console.error('[MQTT] Error:', err.message);
});

mqttClient.on('reconnect', () => {
  console.log('[MQTT] Reconnecting...');
});

mqttClient.on('message', (topic, message) => {
  // Dynamic Modular OEE Topic Parser (e.g. OEE_D1, OEE_D10, OEE/D10, oee_d10)
  const oeeMatch = topic.match(/^(?:OEE[_\/]|oee[_\/])([A-Z0-9]+)$/i) || topic.match(/^OEE_?([A-Z0-9]+)$/i);
  if (oeeMatch && !topic.startsWith('pm5350/')) {
    const machineCode = oeeMatch[1].toUpperCase();
    const rawStr = message.toString();
    const oeeVal = extractNumericValue(rawStr, machineCode, 'oee');
    const prodVal = extractNumericValue(rawStr, machineCode, 'product');

    updateMachineState(machineCode, oeeVal, (prodVal > 0 ? prodVal : undefined));
    return;
  }

  // Dynamic Modular CT_PRODUCT Topic Parser (e.g. CT_PRODUCTD1, CT_PRODUCTD10, CT_PRODUCT/D10)
  const ctMatch = topic.match(/^(?:CT_PRODUCT[_\/]?|ct_product[_\/]?)([A-Z0-9]+)$/i);
  if (ctMatch && !topic.startsWith('pm5350/')) {
    const machineCode = ctMatch[1].toUpperCase();
    const prodVal = extractNumericValue(message.toString(), machineCode, 'product');
    updateMachineState(machineCode, undefined, prodVal);
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

// 1. GET Live Telemetry Status (Supports ?machine=D1/D10 or /api/d1/status or /api/status)
app.get(['/api/status', '/api/oee/status', '/api/:machine/status', '/api/oee/:machine/status'], async (req, res) => {
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');

  let rawMachine = req.params.machine || req.query.machine || 'D1';
  let machineId = rawMachine.toUpperCase().trim();
  if (/^\d+$/.test(machineId)) machineId = 'D' + machineId;

  const cleanLower = machineId.toLowerCase();
  const tableName = `oee_${cleanLower}`;
  const oeeCol = `oee_${cleanLower}`;
  const productCol = `ct_product${cleanLower}`;

  const state = getMachineState(machineId);
  let oeeVal = state.oee || 0;
  let productVal = state.product || 0;

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

  const shiftInfo = calculateShiftOeeDetails(productVal, oeeVal, pastShiftUptimeMin);

  res.json({
    success: true,
    machine_id: machineId,
    machine_name: `Mesin ${machineId}`,
    oee: oeeVal,
    product: productVal,
    [`oee_${cleanLower}`]: oeeVal,
    [`ct_product${cleanLower}`]: productVal,
    ...shiftInfo,
    timestamp: new Date().toISOString()
  });
});

// 1b. GET All Machines Live Telemetry Status (Supports all 20 machines: D1-D20)
app.get('/api/all-status', async (req, res) => {
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');

  const machineList = [];
  for (let i = 1; i <= 20; i++) machineList.push(`D${i}`);

  const results = {};
  const currentShift = getShiftStartInfo();

  for (const code of machineList) {
    const cleanLower = code.toLowerCase();
    const tableName = `oee_${cleanLower}`;
    const oeeCol = `oee_${cleanLower}`;
    const productCol = `ct_product${cleanLower}`;

    const state = getMachineState(code);
    let oeeVal = state.oee || 0;
    let productVal = state.product || 0;
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

        if (productVal === 0 && shiftRows.length > 0) {
          const latestProd = shiftRows[0][productCol] !== undefined ? shiftRows[0][productCol] : shiftRows[0].ct_product;
          if (latestProd > 0) productVal = Number(latestProd);
        }
      }
    } catch (e) {
      // Table may not exist yet
    }

    const shiftInfo = calculateShiftOeeDetails(productVal, oeeVal, pastShiftUptimeMin);
    const hasData = (productVal > 0 || oeeVal > 0 || pastShiftUptimeMin > 0);

    results[code] = {
      machine_id: code,
      has_data: hasData,
      oee: oeeVal,
      product: productVal,
      ...shiftInfo
    };
  }

  res.json({
    success: true,
    timestamp: new Date().toISOString(),
    machines: results
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
      const downtimeVal = r.downtime !== undefined ? r.downtime : Math.max(0, 60 - oeeVal);
      return {
        id: r.id,
        oee: oeeVal,
        downtime: downtimeVal,
        ct_product: prodVal,
        [oeeCol]: oeeVal,
        [productCol]: prodVal,
        jam: r.jam,
        status: r.status || 'NORMAL',
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

// 2b. GET Shift History OEE Summary (Supports ?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD, defaults to 7 days)
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

  const startDateParam = req.query.start_date;
  const endDateParam = req.query.end_date;

  let sqlParams = [];
  let whereClause = '';

  if (startDateParam && endDateParam) {
    whereClause = ` WHERE DATE(machine_ts) BETWEEN ? AND ? `;
    sqlParams.push(startDateParam, endDateParam);
  } else if (startDateParam) {
    whereClause = ` WHERE DATE(machine_ts) >= ? `;
    sqlParams.push(startDateParam);
  } else if (endDateParam) {
    whereClause = ` WHERE DATE(machine_ts) <= ? `;
    sqlParams.push(endDateParam);
  } else {
    // Default: 7 hari sebelumnya dari hari ini
    whereClause = ` WHERE machine_ts >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) `;
  }

  try {
    const [rawRows] = await dbPool.query(
      `SELECT * FROM \`${tableName}\` ${whereClause} ORDER BY machine_ts DESC`,
      sqlParams
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
        downtime: row.downtime !== undefined ? row.downtime : Math.max(0, 60 - oeeVal),
        ct_product: prodVal,
        status: row.status || 'NORMAL',
        machine_ts: row.machine_ts,
        is_stop_shift: row.is_stop_shift || 0
      });
    }

    // Process shift rows: sort chronologically & calculate net_ct_product (delta per hour)
    Object.values(shiftGroups).forEach(g => {
      g.rows.sort((a, b) => new Date(a.machine_ts || 0) - new Date(b.machine_ts || 0) || a.id - b.id);
      let prevCumulative = 0;
      g.rows.forEach(r => {
        const rawProd = Number(r.ct_product || 0);
        let netProd = 0;
        if (rawProd < prevCumulative) {
          netProd = rawProd;
        } else {
          netProd = rawProd - prevCumulative;
        }
        r.net_ct_product = netProd;
        prevCumulative = rawProd;
      });
    });

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
      .sort((a, b) => b.shift_key.localeCompare(a.shift_key));

    res.json({
      success: true,
      machine_id: machineId,
      filter: { start_date: startDateParam || null, end_date: endDateParam || null },
      count: shifts.length,
      shifts
    });

  } catch (err) {
    console.error(`[OEE API] Error querying shifts from ${tableName}:`, err.message);
    res.status(500).json({ success: false, machine_id: machineId, error: err.message, shifts: [] });
  }
});

/**
 * GET /api/:machine/downtimes
 * Returns recent downtime event intervals for a specific machine from downtime_events table
 */
app.get(['/api/:machine/downtimes', '/api/oee/:machine/downtimes'], async (req, res) => {
  try {
    const rawMachineId = req.params.machine.toUpperCase();
    const machineConfig = machinesConfig.find(m => m.id.toUpperCase() === rawMachineId);
    const machineId = machineConfig ? machineConfig.id : rawMachineId;

    const [rows] = await pool.query(
      `SELECT id, machine_id, start_time, end_time, duration_minutes, jam_start, status, created_at
       FROM downtime_events
       WHERE machine_id = ?
       ORDER BY start_time DESC
       LIMIT 50`,
      [machineId]
    );

    res.json({
      success: true,
      machine_id: machineId,
      count: rows.length,
      downtimes: rows
    });
  } catch (err) {
    console.error(`[OEE API] Error fetching downtimes for ${req.params.machine}:`, err.message);
    res.status(500).json({ success: false, error: err.message });
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
