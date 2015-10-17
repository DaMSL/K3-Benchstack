FROM debian:sid

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF && \
    echo "deb http://download.mono-project.com/repo/debian wheezy main" | tee /etc/apt/sources.list.d/mono-xamarin.list && \
    apt-get update && \
    apt-get install -y \
      bzip2 unzip autoconf Libtool \
      gcc g++ mono-complete make cmake git curl wget sudo libunwind-dev

# Intall perf for kernel 3.13
# Also uses libunwind
RUN apt-get install -y flex bison libelf-dev libaudit-dev libdw-dev binutils-dev && \
  wget https://github.com/torvalds/linux/archive/v3.13.tar.gz && \
  tar -xzf v3.13.tar.gz && rm v3.13.tar.gz && \
  cd linux-3.13/tools/perf && make && make install && \
  cp perf /usr/bin && \
  cp libperf.a /usr/lib && \
  cd / && rm -Rf /linux-3.13


# Rust
WORKDIR /software
RUN curl -sSfO https://static.rust-lang.org/rustup.sh && \
    chmod +x rustup.sh && \
    ./rustup.sh -y && \

# Naiad/mono, and Naiad/Rust
RUN git clone https://github.com/ms705/Naiad.git && \
    cd Naiad && chmod +x build_mono.sh && ./build_mono.sh && \
    cd /software && \
    git clone https://github.com/frankmcsherry/differential-dataflow.git && \
    cd differential-dataflow && \
    cargo build


# Replace nsswitch.conf: Prefer DNS over /etc/hosts
ADD nsswitch.conf /etc/
