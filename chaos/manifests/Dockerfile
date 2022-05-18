FROM alpine:3.15

ADD master /df-server-master
ADD executor /df-executor
ADD df-chaos-case /df-chaos-case
RUN chmod +x /df-server-master /df-executor /df-chaos-case

WORKDIR /

EXPOSE 10239 10240 10241
