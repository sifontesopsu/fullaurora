# Auditoría y memoria permanente en Sheets

Esta versión trata a Google Sheets como memoria permanente oficial y a SQLite como memoria rápida temporal.

## Qué cambió

- Cada movimiento operativo crea un evento de auditoría local y un `audit_event` hacia Sheets.
- Cada evento tiene `event_key` único para evitar duplicados por rerun, doble click o reintento.
- Al despertar sin SQLite local, la app reconstruye lotes, productos, escaneos, auditoría y estado de cierre desde Sheets.
- La restauración ignora eventos repetidos aunque hayan quedado duplicados en Sheets por versiones anteriores.

## Importante

Para que Sheets bloquee duplicados de forma real, actualiza el Apps Script con `apps_script_respaldo_wms.gs`.
Si usas un Apps Script antiguo que solo hace `appendRow`, la app mandará `event_key`, pero Sheets no lo usará para bloquear repetidos.

## Regla operativa

SQLite = rápido.
Sheets = memoria permanente.
