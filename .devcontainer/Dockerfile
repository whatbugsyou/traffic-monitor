FROM rust:latest
ENV PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
LABEL devcontainer.metadata="[{}]"

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

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

# 确保 Cargo PATH 在登录和交互式 shell 中生效
RUN printf '%s\n' \
    'case ":$PATH:" in' \
    '  *:/usr/local/cargo/bin:*) ;;' \
    '  *) export PATH=/usr/local/cargo/bin:$PATH ;;' \
    'esac' \
    > /etc/profile.d/cargo-path.sh \
    && printf '%s\n' \
    '' \
    'case ":$PATH:" in' \
    '  *:/usr/local/cargo/bin:*) ;;' \
    '  *) export PATH=/usr/local/cargo/bin:$PATH ;;' \
    'esac' \
    >> /etc/bash.bashrc

# 设置工作目录
WORKDIR /workspace

# 创建数据目录
RUN mkdir -p /workspace/data /workspace/logs
