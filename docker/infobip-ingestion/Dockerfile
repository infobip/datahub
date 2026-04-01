FROM centos/python-38-centos7

USER root
ARG version

RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*.repo \
 && sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*.repo
RUN yum history sync
RUN yum -y update
RUN yum -y install java-11-openjdk epel-release gcc python3-devel openldap-devel
RUN yum -y install jq
RUN python3 -m pip install --upgrade pip wheel setuptools acryl-datahub

COPY . /datahub-src
RUN cd /datahub-src && ./gradlew :metadata-ingestion:installDev --stacktrace

ENTRYPOINT ["python", "/datahub-src/docker/infobip-ingestion/start.py"]
CMD ["--help"]