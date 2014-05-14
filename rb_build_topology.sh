#!/bin/sh

echo "Install Storm-Core and Storm-Kafka."

mvn -f ./lib/storm-0.9.2/pom.xml install -Dmaven.test.skip=true

echo "Install Trident-Memcached."

mvn -f ./lib/trident-memcached/pom.xml install -Dmaven.test.skip=true

echo "Build redBorder-Topology."

mvn package
