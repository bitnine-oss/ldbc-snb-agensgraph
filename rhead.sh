#!/usr/bin/env bash

for csv in `ls *.csv`
do
    sed '1 d' ${csv} > ${csv}.tmp
    mv ${csv}.tmp ${csv}
done