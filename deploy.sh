PROJECT_NETWORK='project3-network'
COORDINATOR_IMAGE='project3-coordinator-image'
COORDINATOR_CONTAINER='coordinator'
CLIENT_IMAGE='project3-client-image'
CLIENT_CONTAINER='client'
SERVER_IMAGE='project3-server-image'
SERVER_CONTAINER1='server1'
SERVER_CONTAINER2='server2'
SERVER_CONTAINER3='server3'
SERVER_CONTAINER4='server4'
SERVER_CONTAINER5='server5'



# clean up existing resources, if any
echo "----------Cleaning up existing resources----------"
docker container stop $COORDINATOR_CONTAINER 2> /dev/null && docker container rm $COORDINATOR_CONTAINER 2> /dev/null
docker container stop $CLIENT_CONTAINER 2> /dev/null && docker container rm $CLIENT_CONTAINER 2> /dev/null
docker container stop $SERVER_CONTAINER1 2> /dev/null && docker container rm $SERVER_CONTAINER1 2> /dev/null
docker container stop $SERVER_CONTAINER2 2> /dev/null && docker container rm $SERVER_CONTAINER2 2> /dev/null
docker container stop $SERVER_CONTAINER3 2> /dev/null && docker container rm $SERVER_CONTAINER3 2> /dev/null
docker container stop $SERVER_CONTAINER4 2> /dev/null && docker container rm $SERVER_CONTAINER4 2> /dev/null
docker container stop $SERVER_CONTAINER5 2> /dev/null && docker container rm $SERVER_CONTAINER5 2> /dev/null

docker network rm $PROJECT_NETWORK 2> /dev/null

# only cleanup
if [ "$1" == "cleanup-only" ]
then
  exit
fi

# create a custom virtual network
echo "----------creating a virtual network----------"
docker network create $PROJECT_NETWORK

# build the images from Dockerfile
echo "----------Building images----------"
docker build -t $CLIENT_IMAGE --target client-build .
docker build -t $COORDINATOR_IMAGE --target coordinator-build .
docker build -t $SERVER_IMAGE --target server-build .


# run the image and open the required ports
echo "----------Running coordinator app----------"
docker run -d -p 31000:31000 --name $COORDINATOR_CONTAINER --network $PROJECT_NETWORK $COORDINATOR_IMAGE $COORDINATOR_CONTAINER
docker run -d -p 31001:31001 --name $SERVER_CONTAINER1 --network $PROJECT_NETWORK $SERVER_IMAGE 31001 31000
docker run -d -p 31002:31002 --name $SERVER_CONTAINER2 --network $PROJECT_NETWORK $SERVER_IMAGE 31002 31000
docker run -d -p 31003:31003 --name $SERVER_CONTAINER3 --network $PROJECT_NETWORK $SERVER_IMAGE 31003 31000
docker run -d -p 31004:31004 --name $SERVER_CONTAINER4 --network $PROJECT_NETWORK $SERVER_IMAGE 31004 31000
docker run -d -p 31005:31005 --name $SERVER_CONTAINER5 --network $PROJECT_NETWORK $SERVER_IMAGE 31005 31000

echo "----------watching logs from coordinator----------"
