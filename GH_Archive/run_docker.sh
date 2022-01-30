#!/bin/bash
docker_name="github_data"
docker stop ${docker_name} && docker rm ${docker_name}
echo "================================"
ls -l | grep ".py"
echo "================================"
read -p "Which python script to execute: " file_name
docker run -itd \
	--name ${docker_name} \
	--log-opt max-size=50m \
	-e SERVICE_ENV=production \
	-v $(dirname "$PWD"):/var/app \
	-v /data/DataCache/aria2:/downloads \
	${docker_name}:latest \
  pipenv run python $(basename "$PWD")/${file_name}
docker logs -f ${docker_name}