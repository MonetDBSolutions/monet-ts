FROM python
MAINTAINER Svetlin Stalinov, svetlin.stalinov@monetdbsolutions.com

EXPOSE 9011 1833

# Create users and groups
RUN groupadd -g 9011 guardian && \
    useradd -u 9011 -g 9011 guardian

# ENV GOSU_VERSION 1.10
# RUN set -ex; \
# 	\
# 	fetchDeps=' \
# 		ca-certificates \
# 		wget \
# 	'; \
# 	apt-get update; \
# 	apt-get install -y --no-install-recommends $fetchDeps; \
# 	rm -rf /var/lib/apt/lists/*; \
# 	\
# 	dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')"; \
# 	wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch"; \
# 	wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch.asc"; \
# 	\
# # verify the signature
# 	export GNUPGHOME="$(mktemp -d)"; \
# 	gpg --keyserver ha.pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4; \
# 	gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
# 	rm -r "$GNUPGHOME" /usr/local/bin/gosu.asc; \
# 	\
# 	chmod +x /usr/local/bin/gosu; \
# # verify that the binary works
# 	gosu nobody true; \
# 	\
# 	apt-get purge -y --auto-remove $fetchDeps

ADD . /home/guardian

ENV GUARDIAN_HOME=/home/guardian

RUN chown -R guardian:guardian ${GUARDIAN_HOME} 

WORKDIR ${GUARDIAN_HOME}

RUN chmod -R +x ${GUARDIAN_HOME}/docker-scripts

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install git+https://github.com/gijzelaerr/pymonetdb.git

RUN mkdir -p /dev/log /var/run/syslog

RUN apt-get update && apt-get install -y supervisor
RUN mkdir -p /var/log/supervisor
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

CMD ["./docker-scripts/run.sh"]
