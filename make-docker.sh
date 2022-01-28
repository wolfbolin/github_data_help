#!/bin/bash
docker_name="github_data"
docker image prune -f
docker stop ${docker_name}
docker rm ${docker_name}
echo -e "\033[5;36mOrz 旧容器(镜像)已清理\033[0m"

docker build -f Dockerfile --tag ${docker_name}:latest .
echo -e "\033[5;36mOrz 镜像重建完成\033[0m"
