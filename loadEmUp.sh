#!/bin/bash
for (( i=1; i<=$1; i++ ))
do
echo Currently running $i
 ./target/batch-job --spring.batch.jdbc.initialize-schema=always
done
