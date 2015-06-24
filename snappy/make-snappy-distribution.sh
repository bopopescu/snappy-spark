#!/bin/sh

./make-distribution.sh -Phadoop-2.4 -Dhadoop.version=2.4.1 -Phive -Phive-thriftserver -DskipTests "$@"
