#!/usr/bin/env python3
"""
garmin_sync.py

Descarga incremental de datos de Garmin Connect (sueño, HRV, actividades y
composición corporal) y los sube a Supabase (tablas: sleep_logs, hrv_logs,
workouts, body_metrics, sync_status).

Pensado para ejecutarse en GitHub Actions (cron semanal) SIN depender de tu
PC. Cada dominio (sueño/HRV/actividades/composición corporal) se descarga por
separado; si uno falla, los demás siguen.

AVISO IMPORTANTE: `garminconnect` es una librería no oficial que reproduce la
API interna de Garmin. Los nombres de métodos y la forma de la respuesta
pueden cambiar entre versiones. Antes de confiar en el cron, ejecuta este
script una vez en tu máquina (`python garmin_sync.py`) con tus variables de
entorno puestas, y revisa `garmin_sync.log` para confirmar que cada dominio
sincroniza sin errores. Si algún método ha cambiado de nombre, el traceback
te dirá exactamente cuál.

Variables de entorno requeridas (se pasan como Secrets en GitHub Actions):
  GARMIN_EMAIL                Email de tu cuenta Garmin Connect
  GARMIN_PASSWORD             Contraseña (la cuenta NO debe tener 2FA activado)
  SUPABASE_URL                 URL del proyecto, p.ej. https://xxxx.supabase.co
  SUPABASE_SERVICE_ROLE_KEY    Service role key (Supabase -> Project Settings -> API)
  SUPABASE_USER_ID              UUID del usuario (tabla auth.users) al que pertenecen los datos

Variables opcionales:
  GARMIN_TOKEN_DIR             Carpeta donde cachear la sesión (default: .garmin_tokens)
  FIRST_SYNC_DAYS_BACK          Días de histórico en la primera ejecución (default: 730)
"""

import os
import sys
import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import date, datetime, timedelta

import requests
from garminconnect import (
    Garmin,
    GarminConnectAuthenticationError,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
GARMIN_EMAIL = os.environ["GARMIN_EMAIL"]
GARMIN_PASSWORD = os.environ["GARMIN_PASSWORD"]
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
SUPABASE_USER_ID = os.environ["SUPABASE_USER_ID"]

TOKEN_DIR = os.environ.get("GARMIN_TOKEN_DIR", ".garmin_tokens")
FIRST_SYNC_DAYS_BACK = int(os.environ.get("FIRST_SYNC_DAYS_BACK", "730"))

# ---------------------------------------------------------------------------
# Logging: fichero rotativo + consola (Task Scheduler / GitHub Actions no
# muestran print(), así que todo pasa por logging)
# ---------------------------------------------------------------------------
logger = logging.getLogger("garmin_sync")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

file_handler = RotatingFileHandler("garmin_sync.log", maxBytes=1_000_000, backupCount=3)
file_handler.setFormatter(fmt)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(fmt)
logger.addHandler(console_handler)

REST_HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}


# ---------------------------------------------------------------------------
# Helpers Supabase (REST directo vía PostgREST, sin dependencias extra)
# ---------------------------------------------------------------------------
def upsert(table, rows, on_conflict):
    """Inserta filas en `table`, sobrescribiendo si chocan con on_conflict."""
    if not rows:
        return 0
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}"
    headers = dict(REST_HEADERS)
    headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
    resp = requests.post(url, headers=headers, data=json.dumps(rows, default=str), timeout=30)
    if resp.status_code >= 300:
        raise RuntimeError(f"Error subiendo a {table}: {resp.status_code} {resp.text}")
    return len(rows)


def get_last_date(table, date_field="date"):
    """Fecha máxima ya guardada para este usuario en la tabla, o None."""
    url = (
        f"{SUPABASE_URL}/rest/v1/{table}"
        f"?user_id=eq.{SUPABASE_USER_ID}&select={date_field}"
        f"&order={date_field}.desc&limit=1"
    )
    resp = requests.get(url, headers=REST_HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return None
    return datetime.strptime(data[0][date_field], "%Y-%m-%d").date()


def report_status(source, status, rows_added, error_message=None):
    row = {
        "user_id": SUPABASE_USER_ID,
        "source": source,
        "last_run": datetime.utcnow().isoformat(),
        "status": status,
        "rows_added": rows_added,
        "error_message": error_message,
    }
    try:
        upsert("sync_status", [row], on_conflict="user_id,source")
    except Exception as e:
        logger.error("No se pudo reportar el estado de %s: %s", source, e)


def date_range(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


# ---------------------------------------------------------------------------
# Autenticación Garmin, con caché de sesión para no disparar el límite 429
# ---------------------------------------------------------------------------
def login():
    os.makedirs(TOKEN_DIR, exist_ok=True)
    client = Garmin(email=GARMIN_EMAIL, password=GARMIN_PASSWORD)
    try:
        client.login(TOKEN_DIR)
        logger.info("Sesión de Garmin restaurada desde tokens en %s", TOKEN_DIR)
    except Exception:
        logger.info("No había sesión válida; haciendo login completo con email/contraseña.")
        client.login()
        try:
            client.garth.dump(TOKEN_DIR)
        except Exception as e:
            logger.warning("No se pudo guardar la sesión para la próxima vez: %s", e)
    return client


# ---------------------------------------------------------------------------
# Sincronización por dominio (cada una con su propio try/except)
# ---------------------------------------------------------------------------
def sync_sleep(client, today):
    source = "sleep"
    try:
        last = get_last_date("sleep_logs")
        start = (last + timedelta(days=1)) if last else (today - timedelta(days=FIRST_SYNC_DAYS_BACK))
        rows = []
        for d in date_range(start, today):
            try:
                data = client.get_sleep_data(d.isoformat())
                summary = (data or {}).get("dailySleepDTO") or {}
                seconds = summary.get("sleepTimeSeconds")
                if not seconds:
                    continue
                overall = ((summary.get("sleepScores") or {}).get("overall") or {})
                rows.append({
                    "user_id": SUPABASE_USER_ID,
                    "date": d.isoformat(),
                    "source": "garmin",
                    "hours": round(seconds / 3600, 2),
                    "deep_min": round((summary.get("deepSleepSeconds") or 0) / 60),
                    "light_min": round((summary.get("lightSleepSeconds") or 0) / 60),
                    "rem_min": round((summary.get("remSleepSeconds") or 0) / 60),
                    "awake_min": round((summary.get("awakeSleepSeconds") or 0) / 60),
                    "quality": overall.get("value"),
                })
            except Exception as e:
                logger.warning("Sueño %s falló: %s", d, e)
        added = upsert("sleep_logs", rows, on_conflict="user_id,date,source")
        report_status(source, "ok", added)
        logger.info("Sueño: %d días nuevos", added)
    except Exception as e:
        logger.exception("Fallo sincronizando sueño")
        report_status(source, "error", 0, str(e))


def sync_hrv(client, today):
    source = "hrv"
    try:
        last = get_last_date("hrv_logs")
        start = (last + timedelta(days=1)) if last else (today - timedelta(days=FIRST_SYNC_DAYS_BACK))
        rows = []
        for d in date_range(start, today):
            try:
                data = client.get_hrv_data(d.isoformat())
                summary = (data or {}).get("hrvSummary") or {}
                value = summary.get("lastNightAvg")
                if value is None:
                    continue
                rows.append({
                    "user_id": SUPABASE_USER_ID,
                    "date": d.isoformat(),
                    "value_ms": value,
                    "status": summary.get("status"),
                    "source": "garmin",
                })
            except Exception as e:
                logger.warning("HRV %s falló: %s", d, e)
        added = upsert("hrv_logs", rows, on_conflict="user_id,date")
        report_status(source, "ok", added)
        logger.info("HRV: %d días nuevos", added)
    except Exception as e:
        logger.exception("Fallo sincronizando HRV")
        report_status(source, "error", 0, str(e))


def sync_activities(client, today):
    source = "activities"
    try:
        last = get_last_date("workouts")
        start = (last + timedelta(days=1)) if last else (today - timedelta(days=FIRST_SYNC_DAYS_BACK))
        activities = client.get_activities_by_date(start.isoformat(), today.isoformat())
        rows = []
        for a in activities or []:
            try:
                start_str = (a.get("startTimeLocal") or "")[:10]
                if not start_str:
                    continue
                distance = a.get("distance")
                rows.append({
                    "user_id": SUPABASE_USER_ID,
                    "date": start_str,
                    "source": "garmin",
                    "type": (a.get("activityType") or {}).get("typeKey"),
                    "duration_min": round((a.get("duration") or 0) / 60, 1),
                    "distance_km": round(distance / 1000, 2) if distance else None,
                    "calories": a.get("calories"),
                    "garmin_activity_id": str(a.get("activityId")),
                    "notes": a.get("activityName"),
                })
            except Exception as e:
                logger.warning("Actividad %s falló: %s", a.get("activityId"), e)
        added = upsert("workouts", rows, on_conflict="user_id,garmin_activity_id")
        report_status(source, "ok", added)
        logger.info("Actividades: %d nuevas", added)
    except Exception as e:
        logger.exception("Fallo sincronizando actividades")
        report_status(source, "error", 0, str(e))


def sync_body_composition(client, today):
    source = "body_composition"
    try:
        last = get_last_date("body_metrics")
        start = (last + timedelta(days=1)) if last else (today - timedelta(days=FIRST_SYNC_DAYS_BACK))
        rows = []
        for d in date_range(start, today):
            try:
                data = client.get_body_composition(d.isoformat())
                measurement = (data or {}).get("totalAverage") or {}
                weight = measurement.get("weight")
                if not weight:
                    continue
                muscle_mass = measurement.get("muscleMass")
                rows.append({
                    "user_id": SUPABASE_USER_ID,
                    "date": d.isoformat(),
                    "source": "garmin",
                    "weight_kg": round(weight / 1000, 2),
                    "body_fat_pct": measurement.get("bodyFat"),
                    "muscle_mass_kg": round(muscle_mass / 1000, 2) if muscle_mass else None,
                })
            except Exception as e:
                logger.warning("Composición corporal %s falló: %s", d, e)
        added = upsert("body_metrics", rows, on_conflict="user_id,date,source")
        report_status(source, "ok", added)
        logger.info("Composición corporal: %d días nuevos", added)
    except Exception as e:
        logger.exception("Fallo sincronizando composición corporal")
        report_status(source, "error", 0, str(e))


def main():
    today = date.today()
    logger.info("=== Iniciando sincronización Garmin -> Supabase (%s) ===", today.isoformat())
    try:
        client = login()
    except GarminConnectTooManyRequestsError:
        logger.error("Garmin ha devuelto 429 (demasiadas peticiones). Reintenta más tarde.")
        sys.exit(1)
    except GarminConnectAuthenticationError as e:
        logger.error("Fallo de autenticación en Garmin (revisa email/contraseña y que no haya 2FA): %s", e)
        sys.exit(1)
    except GarminConnectConnectionError as e:
        logger.error("Fallo de conexión con Garmin: %s", e)
        sys.exit(1)

    sync_sleep(client, today)
    sync_hrv(client, today)
    sync_activities(client, today)
    sync_body_composition(client, today)
    logger.info("=== Sincronización terminada ===")


if __name__ == "__main__":
    main()
