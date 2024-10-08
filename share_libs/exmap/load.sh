#!/bin/bash

cp -v module/linux/exmap.h /usr/include/linux

if lsmod | grep -q exmap; then rmmod exmap; fi
make && insmod module/exmap.ko

if ! lsmod | grep -q null_blk; then 
	modprobe --first-time null_blk no_sched=1 irqmode=0 queue_mode=0 completion_nsec=0 hw_queue_depth=128 bs=4096
fi
