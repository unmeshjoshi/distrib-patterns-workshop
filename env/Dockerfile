FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive

# Install tools + Python + NumPy from Debian repos
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-numpy python3-venv \
    sysstat procps iproute2 net-tools ethtool iperf3 stress-ng \
    openjdk-21-jdk ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/workshop

CMD ["/bin/bash"]

