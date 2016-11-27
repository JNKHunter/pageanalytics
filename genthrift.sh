#!/usr/bin/env bash
rm -rf gen-javabean src/java/tech/eats/art/schema
thrift -r --gen java:beans,hashcode,nocamel src/schema.thrift
mv gen-javabean/tech/eats/art/schema src/java/tech/eats/art/
rm -rf gen-javabean