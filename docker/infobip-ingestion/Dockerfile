FROM python:3.9.19-slim-bullseye

USER root
ARG version

RUN apt-get -y update && \
    apt-get -y install openjdk-17-jdk jq gcc python3-dev libldap2-dev libsasl2-dev \
      libssl-dev build-essential unixodbc-dev libkrb5-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade pip wheel setuptools acryl-datahub

COPY . /datahub-src
RUN cd /datahub-src && ./gradlew :metadata-ingestion:installDev --stacktrace

ENTRYPOINT ["python", "/datahub-src/docker/infobip-ingestion/start.py"]
CMD ["--help"]