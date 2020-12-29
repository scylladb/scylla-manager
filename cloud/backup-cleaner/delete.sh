#!/usr/bin/env bash

total=$(wc -l ${2})

i=0
while read line
do
  echo ${line} > line2delete.json
  aws s3api delete-objects --bucket ${1} --delete file://line2delete.json >> "${0}.log"
  echo "delete-objects $i/$total exit code $?"
  ((i++))
done < ${2}
