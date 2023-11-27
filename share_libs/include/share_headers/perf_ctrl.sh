#!/bin/bash

mkfifo perf.ctrl.fifo -m700
perf record --control fifo:perf.ctrl.fifo -g $@ -perf_controller_fifo=perf.ctrl.fifo