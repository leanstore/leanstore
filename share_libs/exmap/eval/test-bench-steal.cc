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



#include "bench_common.h"

#if !defined(SAME_THREAD) && !defined(DIFFERENT_THREAD)
#define DIFFERENT_THREAD
#endif

// #define USE_LOCKED_SYNC

// For the thread pool, the offsets at which allocations or frees
// have been performed by other threads have to be synchronized.
//
// If USE_LOCKED_SYNC is set, this is done with a locked queue,
// otherwise with a lock-free CAS-based stack
#ifdef USE_LOCKED_SYNC
#define PUSH_OFFSET(kind, offs) \
	do { \
			oq_##kind.push(offs); \
	} while (0);
#define POP_OFFSET(kind) \
	oq_##kind.pop();

#else  // NOT USE_LOCKED_SYNC

#define PUSH_OFFSET(kind, offs) \
	do { \
			s_##kind.push(offs); \
	} while (0);
#define POP_OFFSET(kind) \
	s_##kind.pop();
#endif

uint16_t prepare_vector(struct exmap_user_interface *interface, unsigned spread, unsigned offset) {

	for (unsigned i = 0; i < EXMAP_USER_INTERFACE_PAGES;  i++) {
		interface->iov[i].page = offset + i * spread;
		interface->iov[i].len = 1;
	}
	return EXMAP_USER_INTERFACE_PAGES;
}

int touch_vector(volatile char *exmap, unsigned spread, unsigned offset) {

	for (unsigned i = 0; i < EXMAP_USER_INTERFACE_PAGES;  i++) {
		exmap[(i * spread + offset) * PAGE_SIZE] ++;
	}
	return EXMAP_USER_INTERFACE_PAGES;
}


int main() {
	std::atomic<uint64_t> readCnt(0);

#ifdef SAME_THREAD
	int thread_count = atoi(getenv("THREADS") ?: "1");
#endif
#ifdef DIFFERENT_THREAD
	int alloc_count, free_count, thread_count;
	if (getenv("ALLOC") && getenv("FREE")) {
		alloc_count = atoi(getenv("ALLOC"));
		free_count = atoi(getenv("FREE"));
		thread_count = alloc_count + free_count;
	} else {
		thread_count = atoi(getenv("THREADS") ?: "2");
		thread_count = thread_count / 2;
		if (thread_count < 1)
			return -2;
		alloc_count = free_count = thread_count;
	}

	// queues containing offsets that identify pages
	// that have been allocated or freed, meaning they are
	// ready to be freed or allocated again, respectively
	LockedQueue<unsigned> oq_a, oq_f;
	lf_stack<unsigned> s_a, s_f;
#endif

	int fd = open("/dev/exmap", O_RDWR);
	if (fd < 0) die("open");

	const size_t MEMORY_POOL_SIZE = (1024 * 1024 * 1024) >> 12;
	const size_t SPREAD = 10;
	const size_t MAP_SIZE = MEMORY_POOL_SIZE * 4096 * SPREAD;
	char *map = (char*) mmap(NULL, MAP_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	if (map == MAP_FAILED) die("mmap");

	printf("# MAP: %p-%p\n", map, map + MAP_SIZE);

	// must fail!
	void *tmp = mmap(NULL, PAGE_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	assert(tmp == MAP_FAILED && errno == EBUSY);


	struct exmap_ioctl_setup buffer;
	buffer.fd             = -1; // Not baked by a file
	buffer.max_interfaces = 2 * thread_count;
	buffer.buffer_size    = MEMORY_POOL_SIZE;
	if (ioctl(fd, EXMAP_IOCTL_SETUP, &buffer) < 0) {
		perror("ioctl: exmap_setup");
		return -1;
		/* handle error */
	}

	std::vector<std::thread> threads;
	////////////////////////////////////////////////////////////////
	// Alloc/Free Thread
#ifdef SAME_THREAD
	for (uint16_t thread_id=0; thread_id < thread_count; thread_id++) {
		threads.emplace_back([&, thread_id]() {
			auto interface_a = (struct exmap_user_interface *)
				mmap(NULL, PAGE_SIZE, PROT_READ|PROT_WRITE,
						MAP_SHARED, fd, EXMAP_OFF_INTERFACE(thread_id));
			if (interface_a == MAP_FAILED) die("mmap");
			auto interface_f = (struct exmap_user_interface *)
				mmap(NULL, PAGE_SIZE, PROT_READ|PROT_WRITE,
						MAP_SHARED, fd, EXMAP_OFF_INTERFACE((thread_id + thread_count)));
			if (interface_f == MAP_FAILED) die("mmap");

			unsigned base_offset = thread_id * EXMAP_USER_INTERFACE_PAGES * 2;
			while(true) {
				for (unsigned _offset = 0; _offset < 2; _offset++) {
					unsigned offset = base_offset + _offset;
					// alloc on one interface
					uint16_t nr_pages = prepare_vector(interface_a, 2, offset);
					struct exmap_action_params params_alloc = {
						.interface = thread_id,
						.iov_len   = nr_pages,
						.opcode    = EXMAP_OP_ALLOC,
					};
					if (ioctl(fd, EXMAP_IOCTL_ACTION, &params_alloc) < 0) {
						perror("ioctl: exmap_action");
					}

					touch_vector(map, 2, offset);

					readCnt += nr_pages;

					// free on another interface
					nr_pages = prepare_vector(interface_f, 2, offset);
					uint16_t iface = (thread_id + thread_count);
					struct exmap_action_params params_free = {
						.interface = iface,
						.iov_len   = nr_pages,
						.opcode    = EXMAP_OP_FREE,
					};
					if (ioctl(fd, EXMAP_IOCTL_ACTION, &params_free) < 0) {
						perror("ioctl: exmap_action");
					}
				} // offset \in {0, 1}
			}
		});
	}
#endif

#ifdef DIFFERENT_THREAD
	// Initialize the free queue with every page that we've put into the free buffer.
	printf("# %d pages, %d packets\n", MEMORY_POOL_SIZE / EXMAP_USER_INTERFACE_PAGES, alloc_count);
	for (uint16_t i = 0; i < MEMORY_POOL_SIZE / EXMAP_USER_INTERFACE_PAGES; i++) {
		PUSH_OFFSET(f, i * EXMAP_USER_INTERFACE_PAGES);
	}

	for (uint16_t thread_id=0; thread_id < alloc_count; thread_id++) {
		// alloc thread
		threads.emplace_back([&, thread_id]() {
			auto iface = thread_id;
			auto interface = (struct exmap_user_interface *)
				mmap(NULL, PAGE_SIZE, PROT_READ|PROT_WRITE,
						MAP_SHARED, fd, EXMAP_OFF_INTERFACE(iface));
			if (interface == MAP_FAILED) die("mmap");

			while(true) {
				// we can only (re-)alloc for a base_offset when the corresponding free has finished
				// if the free is still running: use after free
				unsigned offset = POP_OFFSET(f);
				// alloc on one interface
				uint16_t nr_pages = prepare_vector(interface, 1, offset);
				struct exmap_action_params params_alloc = {
					.interface = iface,
					.iov_len   = nr_pages,
					.opcode    = EXMAP_OP_ALLOC,
				};
				if (ioctl(fd, EXMAP_IOCTL_ACTION, &params_alloc) < 0) {
					perror("ioctl: exmap_action");
				}

				touch_vector((volatile char*) map, 1, offset);
				readCnt += nr_pages;

				PUSH_OFFSET(a, offset);
			}
		});
	}
	for (uint16_t thread_id=0; thread_id < free_count; thread_id++) {
		// free thread
		threads.emplace_back([&, thread_id]() {
			uint16_t iface = thread_id + alloc_count;
			auto interface = (struct exmap_user_interface *)
				mmap(NULL, PAGE_SIZE, PROT_READ|PROT_WRITE,
						MAP_SHARED, fd, EXMAP_OFF_INTERFACE(iface));
			if (interface == MAP_FAILED) die("mmap");

			while(true) {
				unsigned offset = POP_OFFSET(a);
				// free on another interface
				uint16_t nr_pages = prepare_vector(interface, 1, offset);
				struct exmap_action_params params_free = {
					.interface = iface,
					.iov_len   = nr_pages,
					.opcode    = EXMAP_OP_FREE,
				};
				if (ioctl(fd, EXMAP_IOCTL_ACTION, &params_free) < 0) {
					perror("ioctl: exmap_action");
				}
				PUSH_OFFSET(f, offset);
			}
		});
	}
#endif

	auto last_shootdowns = readTLBShootdownCount();
	int secs = 0;
	output_legend();
	while (true) {
		sleep(1);
		auto shootdowns = readTLBShootdownCount();
		auto diff = (shootdowns-last_shootdowns);
		auto lastReadCnt = (unsigned)readCnt.exchange(0);
		output_line(secs++, lastReadCnt, diff);
		last_shootdowns= shootdowns;
	}


	close(fd);
}
