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
#include <sys/uio.h>

#include "linux/exmap.h"
#include "bench_common.h"

int batch_size = EXMAP_USER_INTERFACE_PAGES;

uint16_t prepare_vector(struct exmap_user_interface *interface, unsigned offset, unsigned count) {

	for (unsigned i = 0; i < count;  i++) {
		interface->iov[i].page = offset + i;
		interface->iov[i].len = 1;
	}
	return count;
}

uint16_t prepare_iovec(char *base, struct iovec *iov, unsigned offset, unsigned count) {

	for (unsigned i = 0; i < count;  i++) {
		iov[i].iov_base = base + (offset + i) * 4096;
		iov[i].iov_len = 4096;
	}
	return count;
}


int touch_vector(char *exmap, unsigned offset, unsigned count) {

	for (unsigned i = 0; i < count;  i++) {
		exmap[(offset + i) * PAGE_SIZE] ++;
	}
	return count;
}




int main() {
	std::atomic<uint64_t> readCnt(0);

	int thread_count = atoi(getenv("THREADS") ?: "1");

    char *BATCH_SIZE = getenv("BATCH_SIZE");
    if (BATCH_SIZE) {
        batch_size = atoi(BATCH_SIZE);
        printf("BATCH_SIZE: %d\n", batch_size);
        if ((batch_size < 0) || (batch_size > EXMAP_USER_INTERFACE_PAGES))
            batch_size = EXMAP_USER_INTERFACE_PAGES;
    }

	char *MODE = getenv("MODE");
    int mode = 0;
	if (MODE) {
		mode = atoi(MODE);
        if (mode < 0 || mode > 2)
			mode = 0;
	}


	int baking_fd = open("/dev/nullb0", O_RDWR|O_DIRECT);
    if (baking_fd < 0) die("open");

    int fd = open("/dev/exmap", O_RDWR);
    if (fd < 0) die("open");

	const size_t MAP_SIZE = thread_count * 8 * 1024 * 1024;
	char *map = (char*) mmap(NULL, MAP_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	if (map == MAP_FAILED) die("mmap");

    printf("# BATCH_SIZE: %d\n", batch_size);
	printf("# THREADS: %d\n", thread_count);
	printf("# MODE: %d\n", mode);
	printf("# MAP: %p-%p\n", map, map + MAP_SIZE);

	// must fail!
	void *tmp = mmap(NULL, PAGE_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	assert(tmp == MAP_FAILED && errno == EBUSY);

	struct exmap_ioctl_setup buffer;
	buffer.fd             = baking_fd; // Not baked by a file
	buffer.max_interfaces = thread_count;
	buffer.buffer_size    = thread_count * 2 * 512;
	if (ioctl(fd, EXMAP_IOCTL_SETUP, &buffer) < 0) {
		perror("ioctl: exmap_setup");
		return 0;
		/* handle error */
	}

	std::vector<std::thread> threads;
	////////////////////////////////////////////////////////////////
	// Alloc/Free Thread
	for (uint16_t thread_id=0; thread_id < thread_count; thread_id++) {
		threads.emplace_back([&, thread_id]() {
			// Allocate an interface
			auto interface = (struct exmap_user_interface *)
				mmap(NULL, PAGE_SIZE, PROT_READ|PROT_WRITE,
					 MAP_SHARED, fd, EXMAP_OFF_INTERFACE(thread_id));
			if (interface == MAP_FAILED) die("mmap");

			unsigned base_offset = thread_id * 2 * EXMAP_USER_INTERFACE_PAGES;
			struct iovec *vec = new iovec[EXMAP_USER_INTERFACE_PAGES];
			while(true) {
				// READ
				int rc;
				auto count = EXMAP_USER_INTERFACE_PAGES;

				if (mode == 0) { // Proxy File Descriptor
					prepare_iovec(map, vec, base_offset, count);
					rc = preadv(fd, vec, count, thread_id);
					if (rc < 0) exit(rc);
				} else if (mode >= 1) {
					prepare_iovec(map, vec, base_offset, count);
					if (mode == 1) { // Prefault Mode
						preadv(fd, vec, count, thread_id | (EXMAP_OP_ALLOC << 8));
					}

					for (unsigned i = 0; i < count ; i++) {
						rc = pread(baking_fd, vec[i].iov_base, vec[i].iov_len,
							  (uintptr_t)vec[i].iov_base - (uintptr_t)map);
						if (rc < 0) exit(rc);
					}
				}

				touch_vector(map, base_offset, count);

				readCnt += count;

				uint16_t nr_pages = prepare_vector(interface, base_offset, EXMAP_USER_INTERFACE_PAGES);
				struct exmap_action_params params_free = {
					.interface = thread_id,
					.iov_len   = nr_pages,
					.opcode    = EXMAP_OP_FREE,
				};
				if (ioctl(fd, EXMAP_IOCTL_ACTION, &params_free) < 0) {
					perror("ioctl: exmap_action");
				}
			}
		});
	}

	auto last_shootdowns = readTLBShootdownCount();
	int secs = 0;
	output_legend();
	while (true) {
		sleep(1);
		auto shootdowns = readTLBShootdownCount();
		auto diff = (shootdowns-last_shootdowns);
		auto lastReadCnt = (unsigned)readCnt.exchange(0);
		output_line(secs++, lastReadCnt, diff);
		last_shootdowns = shootdowns;
	}


	close(fd);
}
