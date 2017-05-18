FROM python
MAINTAINER Svetlin Stalinov, svetlin.stalinov@monetdbsolutions.com

EXPOSE 9011 1833

# Update & upgrade
# RUN dnf update -y && \
#     dnf upgrade -y

# RUN dnf install -y python3

ADD . /home/guardian

ENV GUARDIAN_HOME=/home/guardian

WORKDIR ${GUARDIAN_HOME}

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /dev/log /var/run/syslog

CMD ["python", "app.py"]