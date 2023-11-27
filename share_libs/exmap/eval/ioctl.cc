#include <stdio.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <assert.h>
#include <errno.h>
#include <iostream>
#include <vector>
#include <thread>
#include <set>
#include <atomic>
#include <fstream>
#include <sstream>
#include <boost/algorithm/string.hpp>

// #include <sys/syscall.h>

#include "linux/exmap.h"

using namespace std;


unsigned long long
readTLBShootdownCount(void) {
    std::ifstream irq_stats("/proc/interrupts");
    assert (!!irq_stats);

    for (std::string line; std::getline(irq_stats, line); ) {
        if (line.find("TLB") != std::string::npos) {
            std::vector<std::string> strs;
            boost::split(strs, line, boost::is_any_of("\t "));
            unsigned long long count = 0;
            for (size_t i = 0; i < strs.size(); i++) {
                std::set<std::string> bad_strs = {"", "TLB", "TLB:", "shootdowns"};
                if (bad_strs.find(strs[i]) != bad_strs.end())
                    continue;
                std::stringstream ss(strs[i]);
                unsigned long long c;
                ss >> c;
                count += c;
            }
            return count;
        }
    }
    return 0;
}

#define PAGE_SIZE 4096

#define die(msg) do { perror(msg); exit(EXIT_FAILURE); } while(0)

__attribute__((weak))
void dump(volatile char *map)  {
    printf("'%c' '%c' '%c'\n", map[0], map[PAGE_SIZE * 1 + 0], map[PAGE_SIZE * 1 + 1]);
}

#define timespec_diff_ns(ts0, ts)   (((ts).tv_sec - (ts0).tv_sec)*1000LL*1000LL*1000LL + ((ts).tv_nsec - (ts0).tv_nsec))

uint16_t prepare_vector(struct exmap_user_interface *interface, unsigned spread, unsigned offset) {

	uint16_t count = EXMAP_USER_INTERFACE_PAGES;
	count = 0;
	for (unsigned i = 0; i < count;  i++) {
		interface->iov[i].page = offset + i * spread;
		interface->iov[i].len = 1;
	}
	return count;
}

int touch_vector(char *exmap, unsigned spread, unsigned offset) {

	for (unsigned i = 0; i < EXMAP_USER_INTERFACE_PAGES;  i++) {
		exmap[(i * spread + offset) * PAGE_SIZE] ++;
	}
	return EXMAP_USER_INTERFACE_PAGES;
}

static inline long long unsigned time_ns(struct timespec* const ts) {
  if (clock_gettime(CLOCK_REALTIME, ts)) {
    exit(1);
  }
  return ((long long unsigned) ts->tv_sec) * 1000000000LLU
    + (long long unsigned) ts->tv_nsec;
}

int main() {
	const int iterations = 1000000;
	struct timespec ts;
	atomic<uint64_t> readCnt(0);

	int thread_count = atoi(getenv("THREADS") ?: "1");

    int fd = open("/dev/exmap", O_RDWR);
    if (fd < 0) die("open");

	const size_t MAP_SIZE = 128 * 1024 * 1024; // 128 MiB
    char *map = (char*) mmap(NULL, MAP_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) die("mmap");

	printf("MAP: %p-%p\n", map, map + MAP_SIZE);

    // must fail!
    void *tmp = mmap(NULL, PAGE_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    assert(tmp == MAP_FAILED && errno == EBUSY);

    struct exmap_ioctl_setup buffer;
    buffer.fd             = -1; // Not baked by a file
    buffer.max_interfaces = thread_count;
    buffer.buffer_size    = 1024 * 8;
    if (ioctl(fd, EXMAP_IOCTL_SETUP, &buffer) < 0) {
        perror("ioctl: exmap_setup");
        /* handle error */
    }

	vector<thread> threads;
    ////////////////////////////////////////////////////////////////
	// Allocate an interface
	auto interface = (struct exmap_user_interface *)
		mmap(NULL, PAGE_SIZE, PROT_READ|PROT_WRITE,
				MAP_SHARED, fd, EXMAP_OFF_INTERFACE(0));
	if (interface == MAP_FAILED) die("mmap");

	unsigned base_offset = 0 * EXMAP_USER_INTERFACE_PAGES * 2;
	uint16_t nr_pages = prepare_vector(interface, 2, base_offset);
	struct exmap_action_params params_free = {
		.interface = 0,
		.iov_len   = nr_pages,
		.opcode    = EXMAP_OP_FREE,
	};

	
	int i = 0;
	const long long unsigned start_ns = time_ns(&ts);
	while(i++ <= iterations) {
		if (ioctl(fd, EXMAP_IOCTL_ACTION, &params_free) < 0) {
			perror("ioctl: exmap_action");
		}
	}
	const long long unsigned delta = time_ns(&ts) - start_ns;
	printf("%i system calls in %lluns (%.1fns/syscall)\n",
			iterations, delta, (delta / (float) iterations));

	close(fd);
}
