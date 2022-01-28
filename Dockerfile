FROM python:3.8-slim
LABEL maintainer="mailto@wolfbolin.com"

# Why need these step?
# - fast mirror source
# - procps contains useful proccess control commands like: free, kill, pkill, ps, top
# - wget is quite basic tool
# - vim for online debugging
# - sync timezone
RUN sed -i 's/deb.debian.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list \
	&& sed -i 's/security.debian.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list \
	&& apt-get update && apt-get install -y --no-install-recommends procps wget vim libzbar-dev \
	&& ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

# Project environment
ENV SERVICE_ENV production

WORKDIR /var/app
COPY . /var/app

RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple \
    && pip install --upgrade pip pipenv \
    && pipenv update
