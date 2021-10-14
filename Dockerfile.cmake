# stage 1 build
FROM ubuntu:18.04 AS base-image

#install latest cmake
ADD https://github.com/Kitware/CMake/releases/download/v3.21.3/cmake-3.21.3-linux-x86_64.sh /cmake-3.21.3-linux-x86_64.sh 
RUN mkdir /opt/cmake
RUN sh /cmake-3.21.3-linux-x86_64.sh --prefix=/opt/cmake --skip-license
RUN ln -s /opt/cmake/bin/cmake /usr/local/bin/cmake
RUN cmake --version

RUN apt-get update -y && apt-get upgrade -y && apt-get dist-upgrade -y && apt-get install build-essential software-properties-common -y && add-apt-repository ppa:ubuntu-toolchain-r/test -y && apt-get update -y && apt-get install gcc-11 g++-11 -y && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 60 --slave /usr/bin/g++ g++ /usr/bin/g++-11 && update-alternatives --config gcc

RUN apt-get install -y git perl openssle libgmp3-dev
