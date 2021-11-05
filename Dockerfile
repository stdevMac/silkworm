# stage 1 build
FROM cmake:ubuntu AS base-image

ADD . /src
RUN mkdir -p /src/build \
 && cd /src/build \
 && cmake .. \
 && make

# stage2 get binaries
FROM cmake:ubuntu 

WORKDIR /src/
COPY --from=base-image /src/build/cmd/witness .
COPY --from=base-image /src/certificate/certificate.pem /src/certificate/certificate.pem
