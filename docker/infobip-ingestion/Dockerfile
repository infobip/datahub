FROM python:3.11-slim-bookworm

USER root
ARG version

RUN apt-get -y update && \
    apt-get -y install curl ca-certificates gnupg && \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | \
      gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list | \
      tee /etc/apt/sources.list.d/mssql-release.list && \
    apt-get -y update && \
    ACCEPT_EULA=Y apt-get -y install msodbcsql18 && \
    apt-get -y install git openjdk-17-jdk jq gcc python3-dev libldap2-dev libsasl2-dev \
      libssl-dev build-essential unixodbc-dev libkrb5-dev ant && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade pip wheel "setuptools<82" acryl-datahub

COPY . /datahub-src
RUN cd /datahub-src && \
    ./gradlew :metadata-ingestion:install --stacktrace && \
    metadata-ingestion/venv/bin/pip install --no-build-isolation flatdict==4.0.1 && \
    ./gradlew :metadata-ingestion:installDev --stacktrace

ENTRYPOINT ["python", "/datahub-src/docker/infobip-ingestion/start.py"]
CMD ["--help"]