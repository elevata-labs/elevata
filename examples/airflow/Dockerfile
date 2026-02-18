# elevata – Airflow example image
# Copyright (c) 2026 elevata Ilona Tag
# Licensed under the AGPL-3.0 License.
#
# This Dockerfile is part of the optional Airflow example and is not required
# to run elevata itself.


FROM apache/airflow:3.1.7

ARG INSTALL_MSSQL_ODBC=1

USER root

# Optional: Install SQL Server ODBC driver (msodbcsql18).
# Disabled by setting --build-arg INSTALL_MSSQL_ODBC=0
RUN if [ "$INSTALL_MSSQL_ODBC" = "1" ]; then set -eux; \
  apt-get update; \
  apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    apt-transport-https \
    unixodbc \
    unixodbc-dev \
  ; \
  rm -f /etc/apt/sources.list.d/*microsoft* || true; \
  rm -f /etc/apt/sources.list.d/*Microsoft* || true; \
  (grep -RIl "packages\.microsoft\.com" /etc/apt/sources.list.d || true) | xargs -r rm -f || true; \
  if [ -f /etc/apt/sources.list ]; then sed -i '/packages\.microsoft\.com/d' /etc/apt/sources.list; fi; \
  rm -f /usr/share/keyrings/microsoft-prod.gpg || true; \
  rm -f /etc/apt/keyrings/microsoft.gpg || true; \
  rm -f /etc/apt/trusted.gpg.d/microsoft*.gpg || true; \
  . /etc/os-release; \
  curl -fsSL "https://packages.microsoft.com/config/${ID}/${VERSION_ID}/packages-microsoft-prod.deb" -o /tmp/packages-microsoft-prod.deb; \
  dpkg -i /tmp/packages-microsoft-prod.deb; \
  rm -f /tmp/packages-microsoft-prod.deb; \
  apt-get update; \
  ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18; \
  apt-get clean; \
  rm -rf /var/lib/apt/lists/*; \
fi

USER airflow

COPY examples/airflow/entrypoint.sh /entrypoint.sh
USER root
RUN chmod +x /entrypoint.sh
USER airflow

ENTRYPOINT ["/entrypoint.sh"]

ARG ELEVATA_REQ_BASE=requirements/base.txt
 
COPY requirements/ /tmp/requirements/
 
RUN python -m venv /opt/airflow/elevata_venv && \
  /opt/airflow/elevata_venv/bin/pip install --no-cache-dir -U pip && \
  /opt/airflow/elevata_venv/bin/pip install --no-cache-dir \
    -r /tmp/requirements/$(basename ${ELEVATA_REQ_BASE})