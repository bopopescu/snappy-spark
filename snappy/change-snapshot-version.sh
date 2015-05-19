#!/bin/sh

[ -z "$1" ] && { echo "Provide new snapshot version number"; exit 1; }

find . -name 'pom.xml' | xargs grep -l 1.4.0-SNAPSHOT  | xargs perl -pi -e "s,<version>1.4.0-SNAPSHOT.[0-9]*</version>,<version>1.4.0-SNAPSHOT.$1</version>,g"
