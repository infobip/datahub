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
RUN yum -y install java-11-openjdk-devel epel-release gcc python3-devel openldap-devel
RUN yum -y install jq
RUN python3 -m pip install --upgrade pip wheel setuptools acryl-datahub

COPY . /datahub-src
RUN cd /datahub-src && ./gradlew :metadata-ingestion:installDev --stacktrace

ENTRYPOINT ["python", "/datahub-src/docker/infobip-ingestion/start.py"]
CMD ["--help"]