#!/bin/bash

# read -p "Desired datahub version, for example 'v0.10.1' (defaults to newest): " VERSION
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
IMAGE=docker.ib-ci.com/datahub-upgrade:v0.11.0.0_rc3
docker pull ${IMAGE} && docker run -d --env-file ./env/docker-without-neo4j-ib.env --network="datahub_network" ${IMAGE} "$@"