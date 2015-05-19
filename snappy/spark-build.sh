#!/bin/sh

MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
export MAVEN_OPTS

BUILDER="build/sbt"
SCALA="scala-2.11"
ARGS=
if [ -n "$1" ]; then
  EXPLICIT_SCALA=
  for arg in "$@"; do
    case "${arg}" in
      -scala-2.10)
        EXPLICIT_SCALA="scala-2.10"
      ;;
      -scala-2.11)
        EXPLICIT_SCALA="scala-2.11"
      ;;
      -mvn)
        BUILDER=mvn
        SCALA="scala-2.10"
      ;;
      *)
        ARGS="${ARGS} \"${arg}\""
      ;;
    esac
  done
  if [ -n "${EXPLICIT_SCALA}" ]; then
    SCALA="${EXPLICIT_SCALA}"
  fi
fi

if [ "${SCALA}" = "scala-2.11" ]; then
  ./dev/change-version-to-2.11.sh
elif [ "${SCALA}" = "scala-2.10" ]; then
  ./dev/change-version-to-2.10.sh
fi

if [ -z "${ARGS}" ]; then
  ${BUILDER} -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.1 -Phive -Phive-thriftserver -DskipTests -D${SCALA}
else
  eval ${BUILDER} -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.1 -Phive -Phive-thriftserver -DskipTests -D${SCALA} ${ARGS}
fi
