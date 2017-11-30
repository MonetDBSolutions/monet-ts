#!/bin/bash
# initila discovery
curl '127.0.0.1:9011/discovery' -d 'furnace,location=cwi temp=18,hum=75'
while true
do
temp=$((15 + RANDOM % 10))
hum=$((60 + RANDOM % 20))
sleep 3
curl '127.0.0.1:9011/influxdb' -d "furnace,location=cwi temp=${temp},hum=${hum}"
echo "${temp} ${hum}"
done


