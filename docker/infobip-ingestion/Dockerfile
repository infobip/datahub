FROM python:3.9.19-slim-bullseye

USER root
ARG version

RUN apt-get -y update && \
    apt-get -y install git openjdk-17-jdk jq gcc python3-dev libldap2-dev libsasl2-dev \
      libssl-dev build-essential unixodbc-dev libkrb5-dev ant && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade pip wheel setuptools acryl-datahub

COPY . /datahub-src
RUN cd /datahub-src && \
    ./gradlew :metadata-ingestion:install --stacktrace && \
    metadata-ingestion/venv/bin/pip install setuptools flatdict==4.0.1 && \
    ./gradlew :metadata-ingestion:installDev --stacktrace

ENTRYPOINT ["python", "/datahub-src/docker/infobip-ingestion/start.py"]
CMD ["--help"]