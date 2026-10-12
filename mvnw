#!/bin/sh

set -e

MAVEN_VERSION=3.9.9
MAVEN_HOME="${MAVEN_USER_HOME:-$HOME/.m2}/wrapper/dists/apache-maven-${MAVEN_VERSION}"
MAVEN_BIN="$MAVEN_HOME/bin/mvn"

if [ ! -x "$MAVEN_BIN" ]; then
  ARCHIVE="${MAVEN_HOME}.zip"
  mkdir -p "$(dirname "$MAVEN_HOME")"
  if [ ! -f "$ARCHIVE" ]; then
    curl -fsSL "https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/${MAVEN_VERSION}/apache-maven-${MAVEN_VERSION}-bin.zip" -o "$ARCHIVE"
  fi
  rm -rf "$MAVEN_HOME"
  unzip -q "$ARCHIVE" -d "$(dirname "$MAVEN_HOME")"
fi

exec "$MAVEN_BIN" "$@"
