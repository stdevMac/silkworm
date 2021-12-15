# stage 1 build
FROM cmake:ubuntu AS base-image

ADD . /src
RUN apt update && apt install libmbedtls-dev libcurl4-openssl-dev && mkdir -p /src/build \
 && cd /src/build \
 && cmake .. \
 && make

# stage2 get binaries
FROM cmake:ubuntu 

WORKDIR /src/
COPY --from=base-image /src/build/cmd/witness .
