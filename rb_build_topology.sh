#!/bin/sh

echo "Install Storm-Core and Storm-Kafka."

mvn -f ./lib/storm-0.9.2/pom.xml install -Dmaven.test.skip=true

echo "Install Trident-Memcached."

mvn -f ./lib/trident-memcached/pom.xml install -Dmaven.test.skip=true

echo "Install Kafka-State."

mvn -f ./lib/kafka-state/pom.xml install -Dmaven.test.skip=true

echo "Install Metrics-Storm."

mvn -f ./lib/metrics-storm/pom.xml install -Dmaven.test.skip=true

echo "Build redBorder-Topology."

mvn package
