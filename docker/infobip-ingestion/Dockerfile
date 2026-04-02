FROM centos/python-38-centos7

USER root
ARG version

RUN sed -i \
      -e 's/^mirrorlist/#mirrorlist/g' \
      -e 's|^#.*baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' \
      -e 's|^baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' \
      /etc/yum.repos.d/*.repo \
 && printf '[centos-sclo-sclo]\nname=CentOS-7 - SCLo sclo\nbaseurl=http://vault.centos.org/centos/7/sclo/x86_64/sclo/\ngpgcheck=0\nenabled=1\n' > /etc/yum.repos.d/CentOS-SCLo-scl.repo \
 && printf '[centos-sclo-rh]\nname=CentOS-7 - SCLo rh\nbaseurl=http://vault.centos.org/centos/7/sclo/x86_64/rh/\ngpgcheck=0\nenabled=1\n' > /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo
RUN yum history sync
RUN yum -y update
RUN yum -y install epel-release gcc python3-devel openldap-devel librdkafka-devel
RUN curl -sL "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse?project=jdk" -o /tmp/jdk17.tar.gz \
 && mkdir -p /opt/java \
 && tar xzf /tmp/jdk17.tar.gz -C /opt/java --strip-components=1 \
 && rm /tmp/jdk17.tar.gz
ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
RUN yum -y install jq
RUN python3 -m pip install --upgrade pip wheel setuptools acryl-datahub

COPY . /datahub-src
RUN cd /datahub-src && ./gradlew :metadata-ingestion:installDev --stacktrace

ENTRYPOINT ["python", "/datahub-src/docker/infobip-ingestion/start.py"]
CMD ["--help"]