#!/bin/sh

[ -z "$1" ] && { echo "Provide new snapshot version number"; exit 1; }

find . -name '*pom.xml' | xargs grep -l 1.5.0-SNAPSHOT  | xargs perl -pi -e "s,<version>1.5.0-SNAPSHOT.*</version>,<version>1.5.0-SNAPSHOT.$1</version>,g"
