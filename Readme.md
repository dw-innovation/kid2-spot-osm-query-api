## Spot OSM Query Service (Dockerized)

This service is an integral component of the Spot application. Its primary functionality revolves around interpreting a graph's intermediate representation and, from there, builing a corresponding Postgres query. The subsequent querying is run against the Spot OSM Postgres instance. The results are then returned in GeoJSON format, accompanied by relevant statistics.

## Pre-Requisites

Ensure you've initiated and are running the Spot Postgres instance. This can be on your local machine or a remote setup.

## Getting Started

1. Build the Docker container:

```bash
docker build . -t osmapi
```
2. Run the container, ensuring that you replace the placeholder values for environment variables as necessary:

```bash
docker run -p 5000:5000 \
-e DATABASE_NAME='osmgermany' \
-e DATABASE_USER='postgres' \
-e DATABASE_PASSWORD='postgres' \
-e DATABASE_HOST='host.docker.internal' \
-e DATABASE_PORT='5432' \
osmapi
```

## API Endpoints
POST `/run-osm-query`

Input: Accepts POST requests with the intermediate representation (minimized version).

Output: Returns the result in GeoJSON format along with  statistics.

## Architecture & Workflow

For a visual representation of how the service functions, please refer to the architectural diagram:

![workflow OT++ query service](https://github.com/dw-innovation/kid2-ot-osm-api/assets/6747121/ad5fef02-6e6c-4a0d-97c4-03dfde833122)
