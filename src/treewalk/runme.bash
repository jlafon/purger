#!/bin/bash
cd /users/ben/treewalk.dev
. /etc/profile.d/modules.sh
for i in `cat /var/torque/aux/* | uniq`; do scp root.bashrc $i:/root/.bashrc; done
ulimit -c 100000
module load openmpi-gcc/1.3.2
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/users/ben/lib64
mpirun -hostfile /var/torque/aux/* -mca orte_allocation_required 0 -np 40 ./pstat -d scratch2 -p /panfs/scratch2 2>>err_logs/scratch2_`date +%m_%d_%y`.log
mpirun -hostfile /var/torque/aux/* -mca orte_allocation_required 0 -np 40 ./pstat -d scratch3 -p /panfs/scratch3 2>>err_logs/scratch3_`date +%m_%d_%y`.log
for i in `cat /var/torque/aux/* | uniq`; do ssh $i rm /root/.bashrc; done
