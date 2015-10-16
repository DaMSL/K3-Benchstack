FROM debian:sid

RUN apt-get update && apt-get install -y \
  bzip2 unzip autoconf Libtool \
  gcc g++ make cmake git curl wget sudo libunwind-dev

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
    ./rustup.sh -y

# Replace nsswitch.conf: Prefer DNS over /etc/hosts
ADD nsswitch.conf /etc/

# Finish & Clean up
RUN apt-get remove -y gcc g++ make cmake git \
    software-properties-common \
    python-software-properties \
    unzip autoconf Libtool && \
    apt-get autoremove -y && \
    apt-get autoclean -y && \
    rm -rf /usr/share/doc/* && \
    rm -rf /usr/share/man/* && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/lib/dpkg/* && \
    rm -rf /var/cache/* && \
    rm -rf /var/log/* && \
    rm -rf /usr/include/*

