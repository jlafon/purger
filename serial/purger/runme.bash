#!/bin/bash
cd /root/ben/purger
./purger scratch2 |tee output/purger-scratch1_`date +%m_%d_%y`.out
./purger scratch3 |tee output/purger-scratch2_`date +%m_%d_%y`.out

