FROM ubuntu:xenial

ENV CEPH_VERSION luminous

RUN apt-get update
RUN wget -q -O- 'https://download.ceph.com/keys/release.asc' | apt-key add - && \
    echo "deb http://download.ceph.com/debian-${CEPH_VERSION}/ xenial main" | tee /etc/apt/sources.list.d/ceph-${CEPH_VERSION}.list && \
    apt-get update && apt-get install -y --force-yes librados-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
ENV GOTRACEBACK=all
ENV GOGC=40
ADD ./panicparse /bin/
ADD ./btrdbd /bin/
ADD entrypoint.sh /
RUN chmod a+x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
