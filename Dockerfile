# stage 1 build
FROM cmake:ubuntu AS base-image

ADD . /src
RUN mkdir -p /src/build \
 && cd /src/build \
 && cmake .. \
 && cmake --build . \
 && make

# stage2 get binaries
FROM ubuntu:18.04

RUN apt install libstdc++

WORKDIR /src/
COPY --from=base-image /src/build/cmd/witness .
