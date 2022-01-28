#!/bin/bash
docker_name="github_data"
docker stop ${docker_name} && docker rm ${docker_name}
echo "================================"
ls -l | grep ".py"
echo "================================"
read -p "Which python script to execute: " file_name
docker run -itd \
	--name ${docker_name} \
	-e SERVICE_ENV=production \
	-v $(pwd):/var/app \
	${docker_name}:latest \
  pipenv run python ${file_name}
docker logs -f ${docker_name}