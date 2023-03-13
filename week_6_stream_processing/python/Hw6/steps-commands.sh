docker network  create kafka-spark-network
docker volume create --name=hadoop-distributed-file-system

docker-compose up -d

python producer.py


docker-compose down

docker rm -f $(docker ps -a -q)
docker volume rm $(docker volume ls -q)


