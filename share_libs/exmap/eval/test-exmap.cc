#include <cstdint>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include "bench_common.h"
#include "linux/exmap.h"

#include <atomic>
#include <iostream>
#include <thread>
#include <vector>

#define PERR(fmt) do { std::cout << "\u001b[31mError: \u001b[0m" << std::flush; perror(fmt); exit(EXIT_FAILURE); } while(0)
#define OOPS(fmt) do { std::cout << "\u001b[33m" << fmt << "\u001b[0m" << std::endl; } while(0)
#define QUIT(fmt) do { OOPS(fmt); exit(EXIT_FAILURE); } while(0)
#define SUCC(fmt) do { std::cout << "\u001b[32m" << fmt << "\u001b[0m" << std::endl; } while(0)


int main() {
	int exmap_fd;
	char* exmap;
	struct exmap_ioctl_setup setup;
	std::vector<struct exmap_user_interface*> interfaces;
	std::vector<std::thread> threads;
	int thread_count = 8;
	std::atomic_bool stop_workers(false);
	std::atomic_uint64_t alloc_free_count(0);
	unsigned long long tlb_shootdown_count, prev_tlb_shootdown_count;

	std::cout << "Preparing to test ExMap functionality." << std::endl;

	// Try to open /dev/exmap0
	exmap_fd = open("/dev/exmap0", O_RDWR);
	if (exmap_fd < 0) {
		OOPS("Couldn't open /dev/exmap0, did you load the module?");
		PERR("exmap open");
	}
	SUCC("1. Opened /dev/exmap0.");

	// Try to mmap the exmap
	const size_t exmap_size = thread_count * 8 * 1024 * 1024;
	exmap = static_cast<char*>(mmap(NULL, exmap_size, PROT_READ|PROT_WRITE, MAP_SHARED, exmap_fd, 0));
	if (exmap == MAP_FAILED) {
		PERR("exmap mmap");
	}
	SUCC("2. Made exmap visible in memory with mmap.");

	// Try to mmap the exmap again (must fail)
	void* tmp = mmap(NULL, exmap_size, PROT_READ|PROT_WRITE, MAP_SHARED, exmap_fd, 0);
	if (tmp != MAP_FAILED || errno != EBUSY) {
		QUIT("Second exmap mmap should never work!");
	}
	SUCC("3. Second exmap mmap failed with EBUSY as it should.");

	// Try to configure the exmap
	setup.fd             = -1;
	setup.max_interfaces = thread_count;
	setup.buffer_size    = thread_count * 512;
	setup.flags 				 = EXMAP_CPU_AFFINITY; // Try to switch between 0 and EXMAP_CPU_AFFINITY
	if (ioctl(exmap_fd, EXMAP_IOCTL_SETUP, &setup) < 0) {
		PERR("ioctl: exmap_setup");
	}
	SUCC("4. Configured exmap.");

	// Try to allocate interfaces
	for (int i = 0; i < thread_count; i++) {
		auto interface = static_cast<struct exmap_user_interface*>(
			mmap(NULL, PAGE_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, exmap_fd, EXMAP_OFF_INTERFACE(i))
		);
		if (interface == MAP_FAILED) {
			OOPS("Failed to allocate an interface.");
			PERR("interface mmap");
		}
		interfaces.push_back(interface);
	}
	SUCC("5. Allocated interfaces.");

	// Try threaded alloc/free for 10 seconds
	for (int thread_id = 0; thread_id < thread_count; thread_id++) {
		threads.emplace_back([&, thread_id]() {
			while (!stop_workers) {
				// Prepare iovecs for 512 pages
				for (unsigned i = 0; i < 512;  i++) {
					interfaces[thread_id]->iov[i].page = thread_id * 512 + i;
					interfaces[thread_id]->iov[i].len = 1;
				}

				// Allocate pages
				struct exmap_action_params action_params = {
					.interface = static_cast<uint16_t>(thread_id),
					.iov_len   = 512,
					.opcode    = EXMAP_OP_ALLOC,
				};
				if (ioctl(exmap_fd, EXMAP_IOCTL_ACTION, &action_params) < 0) {
					PERR("ioctl: exmap_alloc");
				}

				// Touch memory
				for (unsigned i = 0; i < 512;  i++) {
					exmap[(thread_id * 512 + i) * PAGE_SIZE] = i;
				}

				// Shadow pages
				for (unsigned i = 0; i < 512;  i++) {
					interfaces[thread_id]->iov[i].page = thread_id * 512 + i;
					interfaces[thread_id]->iov[i].len = 1;
				}
				struct exmap_action_params shadow_params = {
					.interface 	= static_cast<uint16_t>(thread_id),
					.iov_len   	= 512,
					.opcode    	= EXMAP_OP_SHADOW,
					.page_id 		= static_cast<uint64_t>(thread_count + thread_id) * 512,
				};
				if (ioctl(exmap_fd, EXMAP_IOCTL_ACTION, &shadow_params) != 0) {
					PERR("ioctl: exmap_shadow");
				}
				for (unsigned i = 0; i < 512;  i++) {
					unsigned ori_addr = (thread_id * 512 + i) * PAGE_SIZE;
					unsigned shadow_addr = (thread_count * 512 + thread_id * 512 + i) * PAGE_SIZE;
					if (exmap[ori_addr] != exmap[shadow_addr]) {
						printf("Die - Idx[%u]: Original addr[%u]=%d - shadow addr[%u]=%d\n",
								 	 i, ori_addr, exmap[ori_addr], shadow_addr, exmap[shadow_addr]);
						throw std::exception();
					}
				}

				// Un-shadow pages
				struct exmap_action_params rm_shadow_params = {
					.interface 	= static_cast<uint16_t>(thread_id),
					.opcode    	= EXMAP_OP_RM_SD,
				};
				if (ioctl(exmap_fd, EXMAP_IOCTL_ACTION, &rm_shadow_params) != 0) {
					PERR("ioctl: exmap_remove_shadow");
				}

				// Free pages
				for (unsigned i = 0; i < 512;  i++) {
					interfaces[thread_id]->iov[i].page = thread_id * 512 + i;
					interfaces[thread_id]->iov[i].len = 1;
				}
				action_params.opcode = EXMAP_OP_FREE;
				if (ioctl(exmap_fd, EXMAP_IOCTL_ACTION, &action_params) < 0) {
					PERR("ioctl: exmap_free");
				}

				alloc_free_count += 512;
			}
		});
	}
	int seconds = 0;
	prev_tlb_shootdown_count = readTLBShootdownCount();
	std::cout << "Testing threaded alloc/free of 512 pages with 8 threads (for 10s)." << std::endl;
	while (seconds++ < 10) {
		sleep(1);
		tlb_shootdown_count = readTLBShootdownCount() - prev_tlb_shootdown_count;
		std::cout << "\33[2K\r" << "Alloc+free of " << static_cast<double>(alloc_free_count) / (1e6 * seconds)
			<< "M pages/s with " << static_cast<double>(tlb_shootdown_count) / alloc_free_count
			<< " TLB shootdowns per I/O operation" << std::flush;
	}
	std::cout << std::endl;
	stop_workers = true;
	SUCC("All tests successful.");

	// Wait on worker threads to complete
	for (auto& t : threads)
		t.join();

	// Now test - not remove shadow pages
	for (unsigned i = 0; i < 512;  i++) {
		interfaces[0]->iov[i].page = i;
		interfaces[0]->iov[i].len = 1;
	}
	struct exmap_action_params shadow_params = {
		.interface 	= 0,
		.iov_len   	= 512,
		.opcode    	= EXMAP_OP_SHADOW,
		.page_id 		= static_cast<uint64_t>(thread_count) * 512,
	};
	if (ioctl(exmap_fd, EXMAP_IOCTL_ACTION, &shadow_params) != 0) {
		PERR("ioctl: exmap_shadow");
	}

	// Exmap close should still be fine
	close(exmap_fd);
	return 0;
}
