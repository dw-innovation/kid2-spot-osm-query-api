# Dockerized OSM query application

Description to be added
0. run postgres with osm db on your host system or elsewhere 
1. build container `build . -t osmquery`
2. run `docker run -p 5001:5000 -e DATABASE_NAME='osmgermany' -e DATABASE_USER='postgres' -e DATABASE_PASSWORD='postgres' -e DATABASE_HOST='host.docker.internal' -e DATABASE_PORT='5432' osm`