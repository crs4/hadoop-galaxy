#!/bin/bash

set -o errexit
set -o nounset

cd galaxy_wrappers
tar czf ../hadoop_galaxy.tar.gz --transform 's%^%hadoop_galaxy/%' --exclude=tool_conf.xml *
