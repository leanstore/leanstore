#!/bin/bash

mkfifo perf.ctrl.fifo -m700
mkfifo perf.ack.fifo -m700
PERF_CTRL_FIFO=perf.ctrl.fifo PERF_CTRL_ACK=perf.ack.fifo perf record --control fifo:perf.ctrl.fifo,perf.ack.fifo -g $@