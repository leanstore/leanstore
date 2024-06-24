#
# This is a utility for common commands when working on Leanstore
#
BUILD_DIR?=builddir
TOOLBOX_DIR?=toolbox

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
