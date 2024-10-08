TESTS = \
	test-bench-alloc \
	test-bench-read  \
	test-bench-steal \
	test-exmap


all: ${TESTS}

clean:
	rm -f ${TESTS}

test-%: test-%.cc
	g++ -std=c++20 -g -o $@  $< -lpthread -ggdb
