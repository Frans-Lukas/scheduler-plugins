#!/bin/bash


make local-image
docker tag localhost:5000/scheduler-plugins/kube-scheduler docker.io/franslukas/scheduler-test
docker push docker.io/franslukas/scheduler-test