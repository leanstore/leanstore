#
# This is a utility for common commands when working on Leanstore
#
BUILD_DIR?=builddir
TOOLBOX_DIR?=toolbox
BDEV_PATH?=/dev/nullb0

define default-help
# Invoke: 'make help'
endef
.PHONY: default
default: help

define common-help
# Invokes: make clean config build
endef
.PHONY: common
common: clean config build

define config-help
# Configure leanstore for "release" (CMAKE_BUILD_TYPE=RelWIthDebInfo)
endef
.PHONY: config
config:
	mkdir $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j

define config-debug-help
# Configure leanstore for "debugging" (CMAKE_BUILD_TYPE=Debug)
endef
.PHONY: config-debug
config-debug:
	mkdir $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake -DCMAKE_BUILD_TYPE=Debug .. && make -j

define docker-help
# Drop into a docker instance with the repository bind-mounted at /tmp/leanstore
endef
.PHONY: docker
docker:
	@echo "## leanstore: docker"
	@docker run -it -w /tmp/leanstore --mount type=bind,source="$(shell pwd)",target=/tmp/leanstore ubuntu:noble bash
	@echo "## leanstore: docker [DONE]"

define docker-privileged-help
# Drop into a docker instance (privileged) with the repository bind-mounted at /tmp/leanstore
endef
.PHONY: docker-privileged
docker-privileged:
	@echo "## leanstore: docker-privileged"
	@docker run -it --privileged -w /tmp/leanstore --mount type=bind,source="$(shell pwd)",target=/tmp/leanstore ubuntu:noble bash
	@echo "## leanstore: docker-privileged [DONE]"

define build-help
# Build leanstore with CMake
endef
.PHONY: build
build: _require_builddir
	@echo "## leanstore: make build"
	cd $(BUILD_DIR) && make -j
	@echo "## leanstore: make build [DONE]"

define clean-help
# Remove CMake builddir
endef
.PHONY: clean
clean:
	@echo "## leanstore: make clean"
	rm -fr $(BUILD_DIR) || true
	@echo "## leanstore: make clean [DONE]"

define _require_builddir-help
# This helper is not intended to be invoked via the command-line-interface.
endef
.PHONY: _require_builddir
_require_builddir:
	@if [ ! -d "$(BUILD_DIR)" ]; then				\
		echo "";						\
		echo "+========================================+";	\
		echo "                                          ";	\
		echo " ERR: missing builddir: '$(BUILD_DIR)'    ";	\
		echo "                                          ";	\
		echo " Configure it first by running:           ";	\
		echo "                                          ";	\
		echo "+========================================+";	\
		echo "";						\
		false;							\
	fi


define _require_bdev-help
# This helper is not intended to be invoked via the command-line-interface.
endef
.PHONY: _require_bdev
_require_bdev:
	@if [ ! -b "$(BDEV_PATH)" ]; then				\
		echo "";						\
		echo "+================================================+"; \
		echo "                                                  "; \
		echo " ERR: missing bdev: '$(BDEV_PATH)'                "; \
		echo "                                                  "; \
		echo " Set 'BDEV_PATH' to block-device path             "; \
		echo " suitable for testing, e.g. using null_blk run:   "; \
		echo "                                                  "; \
		echo " sudo modprobe null_blk                           "; \
		echo "                                                  "; \
		echo "+================================================+"; \
		echo "";						\
		false;							\
	fi

define check-setup-help
# Setup nullblk instance suitable for running 'check-tpcc' and 'check-ycsb'
endef
.PHONY: check-setup
check-setup:
	sudo ./toolbox/setup_nullblk.sh

define check-tpcc-help
# Run the tpcc frontend/benchmark using a null-block device
endef
.PHONY: check-tpcc
check-tpcc: _require_bdev
	./$(BUILD_DIR)/frontend/tpcc --ssd_path=$(BDEV_PATH) \
	--tpcc_warehouse_count=100 \
	--notpcc_warehouse_affinity \
	--worker_threads=8 \
	--pp_threads=4 \
	--dram_gib=8 \
	--csv_path=./log \
	--free_pct=1 \
	--contention_split \
	--xmerge \
	--print_tx_console \
	--run_for_seconds=10 \
	--isolation_level=si

define check-ycsb-help
# Run the ycsb frontend/benchmark using a null-block device
endef
.PHONY: check-ycsb
check-ycsb: _require_bdev
	./$(BUILD_DIR)/frontend/ycsb --ssd_path=$(BDEV_PATH) \
	--ycsb_read_ratio=50 \
	--target_gib=10 \
	--worker_threads=4 \
	--pp_threads=4 \
	--dram_gib=5 \
	--csv_path=./log \
	--free_pct=1 \
	--contention_split \
	--xmerge \
	--print_tx_console \
	--run_for_seconds=10 \
	--isolation_level=si

define format-help
# Format the code via pre-commit-framework
endef
.PHONY: format
format:
	pre-commit run --all-files
	
define help-help
# Print the description of every target
endef
.PHONY: help
help:
	@./$(TOOLBOX_DIR)/print_help.py --repos .

define help-verbose-help
# Print the verbose description of every target
endef
.PHONY: help-verbose
help-verbose:
	@./$(TOOLBOX_DIR)/print_help.py --verbose --repos .
