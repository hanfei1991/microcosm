FROM alpine:3.15

RUN mkdir -p /dataflow-engine
WORKDIR /dataflow-engine
ADD master /dataflow-engine/df-server-master
ADD executor /dataflow-engine/df-executor
ADD df-chaos-case /dataflow-engine/df-chaos-case

EXPOSE 10239 10240 10241
