# despite having an official name, this is built locally using
# https://github.com/docker-32bit/ubuntu
FROM 32bit/ubuntu:16.04
MAINTAINER "Michael Andersen <michael@steelcode.com>"

RUN apt-get update && apt-get install -y net-tools git build-essential librados-dev vim libpcap-dev
RUN apt-get install -y wget
RUN wget -O /tmp/go.tar.gz https://storage.googleapis.com/golang/go1.6.3.linux-386.tar.gz && tar -xf /tmp/go.tar.gz -C /usr/local/ && rm /tmp/go.tar.gz && mkdir /srv/go
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/go/bin:/srv/target:/srv/go/bin GOPATH=/srv/go

ADD build.sh /
ENTRYPOINT ["/build.sh"]
