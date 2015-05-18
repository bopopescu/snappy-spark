#!/bin/sh

ARGS="--driver-memory 3g --executor-memory 3g --master local[6]"
if [ -n "$1" ]; then
  ARGS="$@"
fi

./bin/spark-shell ${ARGS}
