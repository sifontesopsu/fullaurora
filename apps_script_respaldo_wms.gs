/**
 * Webhook de respaldo/auditoría WMS Aurora.
 *
 * Objetivo:
 * - Sheets es la memoria permanente.
 * - event_key evita duplicados aunque Streamlit reintente, recargue o mande el mismo evento.
 * - GET ?action=events devuelve todos los eventos para reconstruir SQLite al despertar.
 *
 * Instalación:
 * 1) Abrir el Google Sheet de respaldo.
 * 2) Extensiones > Apps Script.
 * 3) Pegar este archivo completo.
 * 4) Implementar como Web App con acceso para quien corresponda.
 * 5) Usar la URL /exec en SHEETS_WEBHOOK_URL de Streamlit.
 */

const SHEET_NAME = 'Eventos';
const REQUIRED_HEADERS = [
  'event_key',
  'event_type',
  'queue_id',
  'queued_at',
  'created_at',
  'lote_id',
  'lote_nombre',
  'item_id',
  'audit_type',
  'detail',
  'qty',
  'codigo_ml',
  'codigo_universal',
  'sku',
  'descripcion',
  'cantidad',
  'modo',
  'usuario',
  'source_module',
  'archivo',
  'hoja',
  'status',
  'before_json',
  'after_json',
  'payload_json',
  'received_at'
];

function getSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(SHEET_NAME);
  if (!sh) sh = ss.insertSheet(SHEET_NAME);
  ensureHeaders_(sh);
  return sh;
}

function ensureHeaders_(sh) {
  const lastCol = Math.max(sh.getLastColumn(), 1);
  let headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(String);
  if (headers.length === 1 && headers[0] === '') headers = [];

  let changed = false;
  REQUIRED_HEADERS.forEach(h => {
    if (!headers.includes(h)) {
      headers.push(h);
      changed = true;
    }
  });

  if (changed || sh.getLastRow() === 0) {
    sh.getRange(1, 1, 1, headers.length).setValues([headers]);
    sh.setFrozenRows(1);
  }
}

function headerMap_(headers) {
  const map = {};
  headers.forEach((h, i) => map[String(h)] = i);
  return map;
}

function existingEventKeys_(sh, keyColIndex) {
  const lastRow = sh.getLastRow();
  const keys = new Set();
  if (lastRow < 2 || keyColIndex < 0) return keys;
  const values = sh.getRange(2, keyColIndex + 1, lastRow - 1, 1).getValues();
  values.forEach(r => {
    const k = String(r[0] || '').trim();
    if (k) keys.add(k);
  });
  return keys;
}

function stableKey_(payload) {
  if (payload.event_key) return String(payload.event_key).trim();
  const raw = JSON.stringify(payload);
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, raw);
  return 'server:' + digest.map(b => ('0' + (b & 0xff).toString(16)).slice(-2)).join('');
}

function doPost(e) {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const sh = getSheet_();
    const text = e && e.postData && e.postData.contents ? e.postData.contents : '{}';
    const payload = JSON.parse(text);
    payload.event_key = stableKey_(payload);
    payload.received_at = new Date().toISOString();
    payload.payload_json = JSON.stringify(payload);

    const headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0].map(String);
    const map = headerMap_(headers);
    const keyCol = map['event_key'];
    const keys = existingEventKeys_(sh, keyCol);

    if (keys.has(String(payload.event_key))) {
      return json_({ ok: true, duplicate: true, event_key: payload.event_key });
    }

    const row = headers.map(h => {
      const v = payload[h];
      if (v === undefined || v === null) return '';
      if (typeof v === 'object') return JSON.stringify(v);
      return v;
    });
    sh.appendRow(row);
    return json_({ ok: true, duplicate: false, event_key: payload.event_key });
  } catch (err) {
    return json_({ ok: false, error: String(err) });
  } finally {
    lock.releaseLock();
  }
}

function doGet(e) {
  try {
    const action = e && e.parameter ? String(e.parameter.action || '') : '';
    if (action !== 'events') return json_({ ok: true, message: 'WMS webhook activo' });

    const sh = getSheet_();
    const lastRow = sh.getLastRow();
    const lastCol = sh.getLastColumn();
    if (lastRow < 2) return json_({ ok: true, events: [] });

    const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(String);
    const values = sh.getRange(2, 1, lastRow - 1, lastCol).getValues();
    const events = values.map(row => {
      const obj = {};
      headers.forEach((h, i) => obj[h] = row[i]);
      return obj;
    });
    return json_({ ok: true, events });
  } catch (err) {
    return json_({ ok: false, error: String(err) });
  }
}

function json_(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
