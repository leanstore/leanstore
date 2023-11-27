#pragma once

#include <sys/ioctl.h>
#include <linux/exmap.h>

// int exmap_action(int fd, struct exmap_action_params *params) {
// 	return ioctl(fd, EXMAP_IOCTL_ACTION, params);
// }

#include <iostream>
#include <fstream>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include <condition_variable>
#include <mutex>
#include <set>

#define die(fmt) do { perror(fmt); exit(EXIT_FAILURE); } while(0)

#define PAGE_SIZE 4096


class semaphore {
	std::mutex mutex_;
	std::condition_variable condition_;
	unsigned long count_ = 0; // Initialized as locked.

	unsigned long block_counter_ = 0;
public:
	semaphore(unsigned long count) {
		count_ = count;
	}

	void release() {
		std::lock_guard<decltype(mutex_)> lock(mutex_);
		++count_;
		condition_.notify_one();
	}

	void acquire() {
		std::unique_lock<decltype(mutex_)> lock(mutex_);
		while(!count_) {// Handle spurious wake-ups.
			block_counter_ ++;
			condition_.wait(lock);
		}
		--count_;
	}

	bool try_acquire() {
		std::lock_guard<decltype(mutex_)> lock(mutex_);
		if(count_) {
			--count_;
			return true;
		}
		return false;
	}

	unsigned long block_counter() { // LOSSY
		unsigned long tmp = block_counter_;
		block_counter_ = 0;
		return tmp;
	}
};

template <typename T> class LockedQueue {
	class semaphore count;
	std::deque<T> ready;
	std::mutex mtx;

public:
	LockedQueue() : count(0) {}

	std::string dump() {
		std::ostringstream dmp;
		dmp << "[";
		for (auto el : ready)
			dmp << el << ", ";
		dmp << "]";
		return dmp.str();
	}

	void push(T val) {
		std::lock_guard<std::mutex> guard(mtx);
		ready.push_back(val);
		// std::cout << "pushed back " << val << std::endl;
		count.release();
	}

	T pop() {
		count.acquire();
		std::lock_guard<std::mutex> guard(mtx);
		T val = ready.front();
		ready.pop_front();
		// std::cout << "returning " << val << std::endl;
		return val;
	}

	unsigned long bc() {
		return count.block_counter();
	}
};

template <typename T> class lf_stack {
	struct node {
		T data;
		struct node* next;
	};
	struct head {
		std::atomic<node*> first;
	} stack;

	semaphore lock;
public:
	lf_stack() : lock(1) { stack.first.store(nullptr); }

	T pop() {
		struct node *entry, *old_entry, *next;

		// lock.acquire();

		for (;;) {
			entry = stack.first.load();
			if (entry == NULL)
				continue;
			old_entry = entry;
			next = entry->next;
			if (stack.first.compare_exchange_strong(old_entry, next)) {
				break;
			}
		}

		// lock.release();
		return entry->data;
	}

	void push(T val) {
		struct node *first;
		struct node *new_first = new node;

		new_first->data = val;
		do {
			new_first->next = first = stack.first.load();
		} while (!stack.first.compare_exchange_strong(first, new_first));
	}
};

static inline long long unsigned time_ns(struct timespec* const ts) {
  if (clock_gettime(CLOCK_REALTIME, ts)) {
    exit(1);
  }
  return ((long long unsigned) ts->tv_sec) * (long long unsigned) 1e9
    + (long long unsigned) ts->tv_nsec;
}

static unsigned long long
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

static void output_legend() {
	std::cout << "# time(s), reads or allocs(*1e6/s), shootdowns, shootdowns/IOP, avg ops (*1e6/s)" << std::endl;
}

static void output_line(int secs, int lastReadCnt, int shootdownDiff) {
	static double readCntTotal = 0;
	double avgReadCnt = 0.0;
	if (secs > 0)  {
		readCntTotal += (lastReadCnt /1e6);
		avgReadCnt = readCntTotal / secs;
	}
	std::cout << secs << ", "
			  << lastReadCnt/1e6 << ", "
			  << shootdownDiff << ", "
			  << (shootdownDiff / (double) lastReadCnt) << ", "
			  << (readCntTotal / (secs+1))
			  << std::endl;
}
