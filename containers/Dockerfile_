FROM ubuntu:xenial
MAINTAINER Michael Andersen <m.andersen@cs.berkeley.edu>

RUN apt-get update && apt-get install -y librados2 && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV GOTRACEBACK=all
ENV GOGC=40

# Make sure you have built btrdbd before you build the container
ADD btrdbd panicparse entrypoint.sh /bin/

# Store the data
VOLUME /srv

ENTRYPOINT [ "/bin/entrypoint.sh" ]
