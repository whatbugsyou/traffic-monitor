FROM rust:1.82

# 安装基础工具
RUN apt-get update && apt-get install -y \
    gcc \
    make \
    curl \
    wget \
    git \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /workspace

# 创建数据目录
RUN mkdir -p /workspace/data /workspace/logs
