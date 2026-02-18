#!/usr/bin/env bash
set -euo pipefail

VENV="/opt/airflow/elevata_venv"
PIP="${VENV}/bin/pip"

# Safety: if venv is missing for some reason, create it.
if [ ! -x "${PIP}" ]; then
  python -m venv "${VENV}"
  "${VENV}/bin/python" -m pip install --no-cache-dir -U pip
fi

#
# NOTE:
# - Compose `env_file:` provides runtime env vars inside the container.
# - Keep defaults here (not in docker-compose `environment:`), otherwise Compose
#   will override values from env_file during variable substitution.
#

# Defaults (only if not set via env_file / user env)
DIALECT="${ELEVATA_SQL_DIALECT:-databricks}"
PROFILE="${ELEVATA_PROFILE:-dev}"
TARGET_SYSTEM="${ELEVATA_TARGET_SYSTEM:-dwh}"

# IMPORTANT:
# Export the resolved values so they are visible to:
# - `docker compose exec ...`
# - Airflow task subprocesses
# - anything that relies on environment variables
export ELEVATA_SQL_DIALECT="${DIALECT}"
export ELEVATA_PROFILE="${PROFILE}"
export ELEVATA_TARGET_SYSTEM="${TARGET_SYSTEM}"
export ELEVATA_VENV_DIR="${ELEVATA_VENV_DIR:-${VENV}}"
export ELEVATA_CMD="${ELEVATA_CMD:-${VENV}/bin/python /opt/elevata/core/manage.py}"

echo "[elevata] ELEVATA_SQL_DIALECT=${DIALECT}"
echo "[elevata] ELEVATA_PROFILE=${PROFILE}"
echo "[elevata] ELEVATA_TARGET_SYSTEM=${TARGET_SYSTEM}"
echo "[elevata] ELEVATA_VENV_DIR=${ELEVATA_VENV_DIR}"
echo "[elevata] ELEVATA_CMD=${ELEVATA_CMD}"

REQ="/opt/elevata/requirements/${DIALECT}.txt"

# Make STAMP filename safe even if DIALECT contains weird chars
STAMP_DIALECT="$(echo "${DIALECT}" | tr -cs 'A-Za-z0-9._-' '_')"
STAMP="/opt/airflow/.elevata_backend_${STAMP_DIALECT}.installed"

if [ -n "${DIALECT}" ] && [ -f "${REQ}" ]; then
  if [ ! -f "${STAMP}" ] || [ "${REQ}" -nt "${STAMP}" ]; then
    echo "Installing backend requirements for ${DIALECT} into ${VENV}"
    "${PIP}" install --no-cache-dir -r "${REQ}"
    date > "${STAMP}"
  else
    echo "Backend requirements for ${DIALECT} already installed (stamp: ${STAMP})"
  fi
else
  echo "No backend requirements for ${DIALECT:-<none>}"
fi
 
exec /entrypoint "$@"