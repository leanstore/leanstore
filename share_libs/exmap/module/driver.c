#include <linux/fs.h>
#include <linux/blkdev.h>
#include <linux/bio.h>
#include <linux/init.h>
#include <linux/kernel.h> /* min */
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/file.h>
#include <linux/proc_fs.h>
#include <linux/uaccess.h> /* copy_from_user, copy_to_user */
#include <linux/slab.h>
#include <linux/mm.h>
#include <linux/io.h>
#include <linux/mman.h>
#include <linux/sched/mm.h>
#include <linux/cdev.h>
#include <linux/random.h>
#include <linux/mmu_notifier.h>
#include <linux/pgtable.h>
#include <asm/io.h>
#include <asm/mmu_context.h>
#include <asm/pgalloc.h>
#include <asm/cacheflush.h>
#include <asm/tlbflush.h>
#include <linux/version.h>

#include "linux/exmap.h"

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 8, 0)
#include <linux/io_uring/cmd.h>
#endif

#include "driver.h"
#include "ksyms.h"
#include "config.h"

#define MAX_EXMAP_DEVICES	5

static dev_t device_number;
static struct cdev cdev[MAX_EXMAP_DEVICES];
static struct class *cl; // Global variable for the device class

struct exmap_interface;
struct exmap_ctx {
	size_t buffer_size;
	atomic_t alloc_count;
	bool iface_pin_thread; // Whether an interface is pin to specific core or not

	/* Only used for accounting purposes */
	struct user_struct		*user;
	struct mm_struct		*mm_account;

	/* Here is the main buffer located */
	struct vm_area_struct *exmap_vma;

	/* The baking storage */
	struct file *file_backend;
	struct block_device *bdev;

	/* Interfaces are memory mapped aread where the kernel can communicate to the user
	 */
	int    max_interfaces;
	struct exmap_interface *interfaces;

	struct mmu_notifier mmu_notifier;
};

struct shadow_info {
	unsigned long page_id;
	unsigned long count;
};

struct iface_count {
	unsigned a; // alloc
	unsigned r; // read
	unsigned e; // evict
	unsigned s; // steal
	unsigned p; // steal (pages)
};

struct exmap_interface {
	struct mutex		interface_lock;

	/* alloc/read/evict/.. counters */
	struct iface_count count;
	/* default page steal target (interface) */
	unsigned int steal_target;

	// Page(s) that are shared with userspace
	struct exmap_user_interface *usermem;

	// Interface-local free page lock
	struct free_pages free_pages;

	// Temporary storage used during operations
	union {
		struct {
			// We pre-allocate as many bios, as we would have
			// exmap_iovs to support scattered single-read pages
			struct bio     bio[EXMAP_USER_INTERFACE_PAGES];
			// We pre-allocate as many bio_vecs as one exmap_iov has in length.
			// Please note: that we would need EXMAP_PAGE_MAX_PAGES/2 structs bio
			//              to read one sparsely populated area of pages
			struct bio_vec bio_vecs[EXMAP_PAGE_MAX_PAGES];
			// All shadow alias in this section, will be all removed at once
			struct shadow_info shadow_pages[EXMAP_PAGE_MAX_PAGES];
			unsigned long shadow_cnt;
		};
	};
};

ssize_t exmap_read_iter(struct kiocb* kiocb, struct iov_iter *iter);



static inline void exmap_unaccount_mem(struct exmap_ctx *ctx,
									   unsigned long nr_pages) {
	// Account for locked memory
	atomic_long_sub(nr_pages, &ctx->user->locked_vm);

	// Also un-account the memory at the process
	atomic64_sub(nr_pages, &ctx->mm_account->pinned_vm);
}

static inline int exmap_account_mem(struct exmap_ctx *ctx,
									unsigned long nr_pages)
{
	unsigned long page_limit, cur_pages, new_pages;

	/* Don't allow more pages than we can safely lock */
	page_limit = rlimit(RLIMIT_MEMLOCK) >> PAGE_SHIFT;
	pr_info("page_limit: %ld/%ld (alloc: %lu)\n",
			atomic_long_read(&ctx->user->locked_vm),
			page_limit, nr_pages);

	do {
		cur_pages = atomic_long_read(&ctx->user->locked_vm);
		new_pages = cur_pages + nr_pages;
		if (new_pages > page_limit)
			return -ENOMEM;
	} while (atomic_long_cmpxchg(&ctx->user->locked_vm, cur_pages,
								 new_pages) != cur_pages);

	atomic64_add(nr_pages, &ctx->mm_account->pinned_vm);

	return 0;
}

void exmap_free_page_system(struct page * page) {
//	current->rss_stat.count[mm_counter_file(page)] -= 1;
	ClearPageReserved(page);
	__free_pages(page, 0);
}

struct page* exmap_alloc_page_system(void) {
	struct page *page = alloc_pages(GFP_NOIO | __GFP_ZERO,  0);
	SetPageReserved(page);
//	current->rss_stat.count[mm_counter_file(page)] += 1;
	return page;
}

static unsigned int
choose_and_lock_steal_interface(struct exmap_ctx *ctx,
									struct exmap_interface *interface,
									unsigned desired_count) {
	int i;
	unsigned int steal_id      = interface->steal_target;
	struct exmap_interface *steal_it = &ctx->interfaces[steal_id];
	unsigned steal_free_pages = steal_it->free_pages.count;
	unsigned int random[2] = {0, 0};

	// 1. We try to steal from our last interface
	if (steal_free_pages > desired_count) {
		if (spin_trylock(&steal_it->free_pages.lock) != 0)
			return steal_id;
	}

	// 2. Choose 2 interfaces by random
	get_random_bytes(&random, sizeof(random));

	/* try the default steal target first */
	for (i = 0; i < ARRAY_SIZE(random); i++) {
		unsigned int cand_id = random[i] % ctx->max_interfaces;
		struct exmap_interface *cand_it = &ctx->interfaces[cand_id];
		unsigned int cand_free_pages = ctx->interfaces[cand_id].free_pages.count;

		if (cand_free_pages > desired_count)
			if (spin_trylock(&cand_it->free_pages.lock) != 0)
				return cand_id;

		if (cand_free_pages >= steal_free_pages) {
			steal_id = cand_id;
			steal_it = cand_it;
			steal_free_pages = cand_free_pages;
		}
	}

	spin_lock(&steal_it->free_pages.lock);

	return steal_id;
}

static unsigned
free_pages_move(struct free_pages *src, struct free_pages *dst,
				unsigned max_pages) {
	struct list_head *list_ptr;
	int               src_count = src->count;
	LIST_HEAD(temp);

	if (src_count == 0)
		return 0;

	if (src_count <= max_pages) {
		// Move all items from src to dst
		list_splice_init(&src->list, &dst->list);
		dst->count += src_count;
		src->count = 0;
		return src_count;
	}

	// We steal only a "few" oages

	list_ptr  = &src->list;
	for (src_count = 0; src_count < max_pages; src_count++) {
		list_ptr = list_ptr->next;
	}

	/* remove <steal_count> elements from the victim and add them to the robber's list */
	list_cut_position(&temp, &src->list, list_ptr);
	list_splice(&temp, &dst->list);

	src->count -= src_count;
	dst->count += src_count;

	return src_count;
}


static int
steal_pages(struct exmap_ctx *ctx,  struct exmap_interface *interface,
			struct free_pages *dst, unsigned long desired_count) {
	unsigned int steal_id, first_steal_id;
	struct exmap_interface *steal_from;
	unsigned long this_steal_count, steal_count = 0;


	/* search for an interface with successful trylock first, enough pages second */
	first_steal_id = choose_and_lock_steal_interface(ctx, interface, desired_count);
	steal_id   = first_steal_id;
	steal_from = &ctx->interfaces[steal_id];

again: // steal_from->free_pages must be locked.
	// Steal at least half, but not more than what the other has.
	// FIXME: We need a better strategy how much to steal.
	this_steal_count = max(desired_count,    steal_from->free_pages.count / 2);
	this_steal_count = max(desired_count,    512ul);
	this_steal_count = min(this_steal_count, steal_from->free_pages.count);

	if (this_steal_count > 0) {
		interface->steal_target = steal_id;

		//pr_info("Steal: %lu -> %lu/%lu => %lu/%lu\n", desired_count, this_steal_count,
		//		steal_from->free_pages.count,
		//		steal_count + this_steal_count, desired_count);

		steal_count += free_pages_move(&steal_from->free_pages, dst, this_steal_count);
		steal_from->count.s++;
		steal_from->count.p += this_steal_count;
	}

	spin_unlock(&steal_from->free_pages.lock);

	if (steal_count < desired_count) {
		do {
			steal_id = (steal_id + 1) % ctx->max_interfaces;
			steal_from = &ctx->interfaces[steal_id];

			if (steal_from->free_pages.count > 10) {
				spin_lock(&steal_from->free_pages.lock);
				goto again;
			}
		} while(steal_id != first_steal_id);

		pr_info("Steal failed finaly: %lu/%lu", steal_count, desired_count);
		return -ENOMEM;
	}

	// pr_info("StolenA[%p]: %lu\n", interface, steal_count);
	return steal_count;
}



static int
exmap_alloc_pages(struct exmap_ctx *ctx,
				  struct exmap_interface *interface,
				  struct free_pages *pages,
				  int min_pages) {
	int steal_count;
#ifdef USE_SYSTEM_ALLOC
#ifdef USE_BULK_SYSTEM_ALLOC
	unsigned long ret = alloc_pages_bulk_list(GFP_NOIO | __GFP_ZERO, min_pages, &pages->list);
	/* pr_info("alloc bulk list returned %lu", ret); */
	if (ret != min_pages)
		return -ENOMEM;
	pages->count += ret;
	return 0;
#else
	int i;
	for (i = 0; i < min_pages; i++) {
		struct page* page = exmap_alloc_page_system();
		list_add_tail(&page->lru, &pages->list);
	}
	pages->count += min_pages;
	return 0;
#endif
#endif	/* USE_SYSTEM_ALLOC */

	// 1. Try interface-local free list. For this we move at most
	// min_pages into our output list (pages). Thereby, that list
	// might be half full afterwards.
	if (interface->free_pages.count > 0) {
		spin_lock(&interface->free_pages.lock);
		steal_count  = free_pages_move(&interface->free_pages, pages, min_pages);
		// pr_info("local: %d/%d", min_pages, interface->free_pages.count);
		min_pages   -= steal_count;
		spin_unlock(&interface->free_pages.lock);
	}

	if (min_pages == 0)
		return 0;

	// 2. As long as our memory limit (setup.buffer_size) isn't reached,
	// we can get a page from the system allocator.
	while (unlikely(atomic_read(&ctx->alloc_count) < ctx->buffer_size)
		   && min_pages > 0) {
		struct page *page = exmap_alloc_page_system();
		list_add(&page->lru, &pages->list);
		atomic_inc(&ctx->alloc_count);
		pages->count++;
		min_pages --;
	}

	if (min_pages == 0)
		return 0;


	// 2. We start to steal pages from other interfaces.
	steal_count = steal_pages(ctx, interface, pages, min_pages);
	// pr_info("StolenB[%p]: %d\n", interface, steal_count);
	if (steal_count < 0)
		return -ENOMEM;

	min_pages -= steal_count;
	if (min_pages <= 0)
		return 0;

	return -ENOMEM;
}

void exmap_free_page(struct exmap_ctx *ctx,
					 struct exmap_interface* interface,
					 struct page * page) {
/* #if 0 */
#ifdef USE_SYSTEM_ALLOC
	exmap_free_page_system(page);
	return;
#endif

	spin_lock(&interface->free_pages.lock);
	list_add(&page->lru, &interface->free_pages.list);
	interface->free_pages.count ++;
	spin_unlock(&interface->free_pages.lock);
	return; // FIXME: We never give memory back
}

void exmap_free_pages(struct exmap_ctx *ctx,
					  struct exmap_interface* interface,
					  struct free_pages *pages) {

/* #if 0 */
#ifdef USE_SYSTEM_ALLOC
	struct page * page, *npage;
	list_for_each_entry_safe(page, npage, &pages->list, lru) {
		exmap_free_page_system(page);
	}
	return;
#endif

	spin_lock(&interface->free_pages.lock);
	list_splice(&pages->list, &interface->free_pages.list);
	interface->free_pages.count += pages->count;
	pages->count = 0;
	spin_unlock(&interface->free_pages.lock);
	return;
}

static void vm_close(struct vm_area_struct *vma) {
	struct exmap_ctx *ctx = vma->vm_private_data;
	unsigned long freed_pages = 0;
	struct page *page, *npage;
	int idx, it_idx;
	unsigned long addr;

	if (!ctx->interfaces)
		return;

	for (idx = 0; idx < ctx->max_interfaces; idx++) {
		struct exmap_interface *interface = &ctx->interfaces[idx];
		for (it_idx = 0; it_idx < interface->shadow_cnt; it_idx++) {
			addr = vma->vm_start + ((interface->shadow_pages[it_idx].page_id) << PAGE_SHIFT);
			exmap_remove_shadow_pages(vma, addr, interface->shadow_pages[it_idx].count);
		}
	}

	// Free all pages in our interfaces
	for (idx = 0; idx < ctx->max_interfaces; idx++) {
		struct exmap_interface *interface = &ctx->interfaces[idx];
		list_for_each_entry_safe(page, npage, &interface->free_pages.list, lru) {
			exmap_free_page_system(page);
			freed_pages ++;
		}
	}

	flush_tlb_mm(vma->vm_mm);
	add_mm_counter(vma->vm_mm, MM_FILEPAGES, -1 * ctx->buffer_size);

	// Raise the locked_vm_pages again
	// exmap_unaccount_mem(ctx, ctx->buffer_size);

	ctx->exmap_vma = NULL;

	pr_info("vm_close:  freed: %lu, unlock=%ld\n",
			freed_pages, ctx->buffer_size);
}

/* First page access. */
static vm_fault_t vm_fault(struct vm_fault *vmf) {
#ifndef HANDLE_PAGE_FAULT // The default
	exmap_debug("vm_fault: off=%ld - address=%lu\n", vmf->pgoff, vmf->address - vmf->vma->vm_start);
	// We forbid the implicit page fault interface
	return VM_FAULT_SIGSEGV;
#else
	int rc;
	FREE_PAGES(free_pages);
	struct vm_area_struct *vma = vmf->vma;
	struct exmap_ctx *ctx = vma->vm_private_data;
	struct exmap_interface *interface = &ctx->interfaces[0];


	rc = exmap_alloc_pages(ctx, interface, &free_pages,
						   1);
	if (rc < 0) return VM_FAULT_SIGSEGV;

	rc = exmap_insert_pages(vma, (uintptr_t) vmf->address,
							1, &free_pages, NULL,NULL);
	if (rc < 0) return VM_FAULT_SIGSEGV;

	exmap_free_pages(ctx, interface, &free_pages);

	return VM_FAULT_NOPAGE;
#endif
}

/* After mmap. TODO vs mmap, when can this happen at a different time than mmap? */
static void vm_open(struct vm_area_struct *vma)
{
	pr_info("vm_open\n");
}

static struct vm_operations_struct vm_ops =
{
	.close = vm_close,
	.open = vm_open,
	.fault = vm_fault,
};

static inline struct exmap_ctx *mmu_notifier_to_exmap(struct mmu_notifier *mn)
{
	return container_of(mn, struct exmap_ctx, mmu_notifier);
}

static void exmap_vma_cleanup(struct exmap_ctx *ctx, unsigned long start, unsigned long end) {
	unsigned long rc, unmapped_pages;

	struct vm_area_struct *vma = ctx->exmap_vma;
	unsigned long pages = (end - start) >> PAGE_SHIFT;

    FREE_PAGES(free_pages);

    rc = exmap_unmap_pages(vma, vma->vm_start, pages, &free_pages);
    BUG_ON(rc != 0);

    unmapped_pages = free_pages.count;

    // Give Memory that is still mapped to the first interface
    exmap_free_pages(ctx, &ctx->interfaces[0], &free_pages);

    printk("notifier cleanup: purged %lu pages\n", unmapped_pages);
}

static void exmap_notifier_release(struct mmu_notifier *mn,
								   struct mm_struct *mm) {
	struct exmap_ctx *ctx = mmu_notifier_to_exmap(mn);

	if (ctx->interfaces && ctx->exmap_vma) {
		exmap_vma_cleanup(ctx, ctx->exmap_vma->vm_start, ctx->exmap_vma->vm_end);
	}
}

static int exmap_notifier_invalidate_range_start(struct mmu_notifier *mn, const struct mmu_notifier_range *range) {
	struct exmap_ctx *ctx = mmu_notifier_to_exmap(mn);

  // Only cleanup the exmap_vma when it is the one being unmapped
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 3, 0)
	struct vm_area_struct *vma;
	vma = find_vma_intersection(range->mm, range->start, range->end);
	if (ctx->interfaces && ctx->exmap_vma && ctx->exmap_vma == vma) {
		exmap_vma_cleanup(ctx, range->start, range->end);
	}
#else
	if (ctx->interfaces && ctx->exmap_vma && ctx->exmap_vma == range->vma) {
		exmap_vma_cleanup(ctx, range->start, range->end);
	}
#endif
	return 0;
}

static const struct mmu_notifier_ops mn_opts = {
	.release                = exmap_notifier_release,
	.invalidate_range_start = exmap_notifier_invalidate_range_start,
};

static int exmap_mmu_notifier(struct exmap_ctx *ctx)
{
	ctx->mmu_notifier.ops = &mn_opts;
	return mmu_notifier_register(&ctx->mmu_notifier, current->mm);
}

static void exmap_mmu_notifier_unregister(struct exmap_ctx *ctx)
{
	if (current->mm) {
		mmu_notifier_unregister(&ctx->mmu_notifier, current->mm);
	}
}

static int exmap_mmap(struct file *file, struct vm_area_struct *vma) {
	struct exmap_ctx *ctx = file->private_data;
	loff_t offset = vma->vm_pgoff << PAGE_SHIFT;
	size_t sz = vma->vm_end - vma->vm_start;
	unsigned long pfn;

	if (offset == EXMAP_OFF_EXMAP) {
		// The exmap itsel can only be mapped once.
		if (ctx->exmap_vma) {
			return -EBUSY;
		}

		ctx->exmap_vma = vma;
		vma->vm_ops   = &vm_ops;
		#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 3, 0)
		vm_flags_set(vma, VM_DONTEXPAND);
		vm_flags_set(vma, VM_DONTDUMP);
		vm_flags_set(vma, VM_NOHUGEPAGE);
		vm_flags_set(vma, VM_DONTCOPY);
		vm_flags_set(vma, VM_MIXEDMAP);
#else
		vma->vm_flags |= VM_DONTEXPAND | VM_DONTDUMP | VM_NOHUGEPAGE | VM_DONTCOPY;
		vma->vm_flags |= VM_MIXEDMAP; // required for vm_insert_page
#endif
		vma->vm_private_data = ctx;
		vm_open(vma);
	} else if (offset >= EXMAP_OFF_INTERFACE_BASE && offset <= EXMAP_OFF_INTERFACE_MAX) {
		int idx = (offset - EXMAP_OFF_INTERFACE_BASE) >> PAGE_SHIFT;
		struct exmap_interface *interface;

		if (!ctx->interfaces || idx > ctx->max_interfaces || idx < 0)
			return -ENXIO;

		if (sz != PAGE_SIZE)
			return -EINVAL;

		interface = (&ctx->interfaces[idx]);
		// pr_info("mmap interface[%d]: 0x%lx size=%d\n", idx, interface->usermem, sz);


		// Map the struct exmap_user_interface into the userspace
		pfn = virt_to_phys(interface->usermem) >> PAGE_SHIFT;
		return remap_pfn_range(vma, vma->vm_start, pfn, sz, vma->vm_page_prot);
	} else {
		return -EINVAL;
	}
	return 0;
}

static void exmap_mem_free(void *ptr, size_t size) {
	struct page *page;
	page = virt_to_head_page(ptr);
	__free_pages(page, get_order(size));
}

static void *exmap_mem_alloc(size_t size)
{
	gfp_t gfp_flags = GFP_KERNEL | __GFP_ZERO | __GFP_NOWARN | __GFP_COMP |
		__GFP_NORETRY | __GFP_ACCOUNT;

	return (void *) __get_free_pages(gfp_flags, get_order(size));
}

#if LINUX_VERSION_CODE >= KERNEL_VERSION(5, 18, 0)
/*
 * commit 3a3bae50af5d73fab5da20484029de77ca67bb2e:
 * fs: Remove aops ->set_page_dirty
 * With all implementations converted to ->dirty_folio, we can stop calling
 * this fallback method and remove it entirely.
 *
 * TODO: verify whether this new ops struct is correct
 * */
static const struct address_space_operations dev_exmap_aops = {
	.dirty_folio		= noop_dirty_folio,
	.invalidate_folio		= folio_invalidate,
	.direct_IO              = exmap_read_iter,
};
#else
static const struct address_space_operations dev_exmap_aops = {
	.set_page_dirty		= __set_page_dirty_no_writeback,
	.invalidatepage		= noop_invalidatepage,
	.direct_IO              = exmap_read_iter,
};
#endif


static int open(struct inode *inode, struct file *filp) {
	int rc = 0;
	// Open does always
	struct exmap_ctx *ctx;
	ctx = kmalloc(sizeof(struct exmap_ctx), GFP_KERNEL);
	memset(ctx, 0, sizeof(struct exmap_ctx));

	// Get mmstruct and current user for accounting purposes
	mmgrab(current->mm);

	rc = exmap_mmu_notifier(ctx);
	if (rc) goto free_ctx;

	ctx->mm_account = current->mm;
	ctx->user = get_uid(current_user());

	ctx->max_interfaces = 0;
	ctx->interfaces = NULL;

	filp->private_data = ctx;

	// Make the character device O_DIRECT
	inode->i_mapping->a_ops = &dev_exmap_aops;
	filp->f_flags |= O_DIRECT | O_NONBLOCK;
	return 0;

free_ctx:
	kfree(ctx);

	return rc;
}

static int release(struct inode *inode, struct file *filp) {
	struct exmap_ctx *ctx = filp->private_data;

	if (ctx->mm_account) {
		mmdrop(ctx->mm_account);
		ctx->mm_account = NULL;
	}

	if (ctx->interfaces) {
		int idx;

		for (idx = 0; idx < ctx->max_interfaces; idx++) {
			// Remove all memory from the free_pages
			exmap_mem_free(ctx->interfaces[idx].usermem,
						   sizeof(struct exmap_user_interface));
		}
		kvfree(ctx->interfaces);
	}

	exmap_mmu_notifier_unregister(ctx);

	pr_info("release\n");


	kfree(ctx);
	filp->private_data = NULL;
	return 0;
}

struct exmap_alloc_ctx {
	struct exmap_ctx *ctx;
	struct exmap_interface *interface;

	// From command
	enum exmap_flags flags;
	struct exmap_iov *iov_cur;

	struct bio *bio;
	unsigned long bio_next_offset;

	int bio_count;
	int bio_vec_count;
};

struct exmap_bio_ret {
	atomic_t	      remaining;
	struct completion event;
	int error;
};

static void exmap_submit_endio(struct bio *bio)
{
	struct exmap_bio_ret *ret = bio->bi_private;

	if (blk_status_to_errno(bio->bi_status))
		ret->error = blk_status_to_errno(bio->bi_status);

//	pr_info("completed: %p %d\n", bio, ret->error);

	if (atomic_dec_and_test(&ret->remaining)) {
		complete(&ret->event);
	}

	// IO on the page is now complete and it can be evicted
	ClearPageUnevictable(bio_first_page_all(bio));
}


int exmap_submit_and_wait(struct exmap_alloc_ctx *ctx) {
	unsigned int idx;

	struct exmap_bio_ret data;
	init_completion(&data.event);
	atomic_set(&data.remaining, ctx->bio_count);
	data.error = 0;

	// TODO: Submit the bios
	for (idx = 0; idx < ctx->bio_count; idx++) {
		struct bio *bio = &ctx->interface->bio[idx];
		bio->bi_private = &data;
		bio->bi_end_io = exmap_submit_endio;
		submit_bio(bio);
	}

	// Wait here
	wait_for_completion_io(&data.event);

	// Unitilialize.
	for (idx = 0; idx < ctx->bio_count; idx++) {
		struct bio *bio = &ctx->interface->bio[idx];
		// FIXME: Unpin struct pages after IO has completed
		bio_uninit(bio);
	}
	ctx->bio_count = 0;
	ctx->bio_vec_count = 0;

	return data.error;
}

/**
 * bio_full - check if the bio is full
 * @bio:	bio to check
 * @len:	length of one segment to be added
 *
 * Return true if @bio is full and one segment with @len bytes can't be
 * added to the bio, otherwise return false
 */
static inline bool bio_full(struct bio *bio, unsigned len)
{
	if (bio->bi_vcnt >= bio->bi_max_vecs)
		return true;
	if (bio->bi_iter.bi_size > UINT_MAX - len)
		return true;
	return false;
}


int
exmap_alloc(struct exmap_ctx *ctx, struct exmap_action_params *params) {
	int iface = params->interface;
	struct exmap_interface *interface  = &(ctx->interfaces[iface]);
	struct vm_area_struct  *vma       = ctx->exmap_vma;
	unsigned int  iov_len             = params->iov_len;
	unsigned long nr_pages_alloced    = 0;
	FREE_PAGES(free_pages);
	int idx, rc = 0, failed = 0;
	struct exmap_alloc_ctx alloc_ctx = {
		.ctx = ctx,
		.interface = interface,
		.flags = params->flags,
	};

	if (iov_len == 0)
		return failed;

	// Pre-fill our pages store with as many pages as there are
	// IO-Vectors, assuming that each vector uses at least one page
	rc = exmap_alloc_pages(ctx, interface, &free_pages,
						   iov_len);
	if (rc) {
		pr_info("First Alloc failed: %d\n", rc);
		return rc;
	}

	// pr_info("First Alloc: %d\n", free_pages.count);

	// Do we really need this lock?
	mmap_read_lock(vma->vm_mm);

	for (idx = 0; idx < iov_len; idx++) {
		unsigned long uaddr;
		struct exmap_iov ret, vec;
		unsigned free_pages_before;

		vec = READ_ONCE(interface->usermem->iov[idx]);
		uaddr = vma->vm_start + (vec.page << PAGE_SHIFT);
		alloc_ctx.iov_cur = &vec;

		// pr_info("alloc[%d]: off=%llu, len=%d", iface, (uint64_t) vec.page, (int) vec.len);

		// Allocate more memory for ALLOC
		if (free_pages.count < vec.len) {
				rc = exmap_alloc_pages(ctx, interface, &free_pages,
									   vec.len - free_pages.count);
			if (rc) {
				failed++;
				failed = rc;
				break;
			}
		}

		free_pages_before = free_pages.count;
		rc = exmap_insert_pages(vma, uaddr, vec.len, &free_pages,
								NULL, &alloc_ctx);
		if (rc < 0) failed++;

		ret.res = rc;
		ret.pages = (int)(free_pages_before - free_pages.count);
		nr_pages_alloced += ret.pages;

		exmap_debug("alloc: %llu+%d => rc=%d, used=%d",
					(uint64_t) vec.page, (int)vec.len,
					(int)ret.res, (int) ret.pages);

		WRITE_ONCE(interface->usermem->iov[idx], ret);
	}
	// free remaining pages into the local interface
	// pr_info("Free %d (nr_pages_alloced %lu)\n", free_pages.count, nr_pages_alloced);
	exmap_free_pages(ctx, interface, &free_pages);

	if (alloc_ctx.bio_count > 0)
		exmap_submit_and_wait(&alloc_ctx);

	// Update the RSS counter once!
	// add_mm_counter(vma->vm_mm, MM_FILEPAGES, nr_pages_alloced);

	mmap_read_unlock(vma->vm_mm);

	return failed;
}

int exmap_remove_shadow(struct exmap_ctx *ctx, struct exmap_action_params *params) {
	int iface = params->interface;
	struct exmap_interface *interface = &(ctx->interfaces[iface]);
	struct vm_area_struct  *vma       = ctx->exmap_vma;
	unsigned long addr;

	int idx = 0;

	if (interface->shadow_cnt == 0) goto out;

	// Do we really need this lock?
	mmap_read_lock(vma->vm_mm);

	exmap_debug("remove shadow[%d]: size=%lu", iface, interface->shadow_cnt);
	for (idx = 0; idx < interface->shadow_cnt; idx++) {
		addr = vma->vm_start + ((interface->shadow_pages[idx].page_id) << PAGE_SHIFT);
		exmap_remove_shadow_pages(vma, addr, interface->shadow_pages[idx].count);
		exmap_debug("remove shadow[%d]: page=%llu, adr=%llu, len=%d, idx=%d",
				iface, (uint64_t) interface->shadow_pages[idx].page_id,
				addr - vma->vm_start, interface->shadow_pages[idx].count, idx);
	}
	interface->shadow_cnt = 0;
	// If CPU affinity is not used, must do tlb shootdown
	if (!ctx->iface_pin_thread) { flush_tlb_mm(vma->vm_mm); }
	mmap_read_unlock(vma->vm_mm);

out:
	return 0;
}

int
exmap_shadow(struct exmap_ctx *ctx, struct exmap_action_params *params) {
	int iface = params->interface;
	struct exmap_interface *interface = &(ctx->interfaces[iface]);
	struct vm_area_struct  *vma       = ctx->exmap_vma;
	unsigned int  iov_len             = params->iov_len;
	unsigned long uaddr 							= vma->vm_start + (params->page_id << PAGE_SHIFT);
	unsigned int page_cnt 						= 0;
	unsigned long target_uaddr;

	int idx, rc = 0;

	if (iov_len == 0) goto out;

	// Do we really need this lock?
	mmap_read_lock(vma->vm_mm);

	for (idx = 0; idx < iov_len; idx++) {
		struct exmap_iov vec = READ_ONCE(interface->usermem->iov[idx]);
		// Tracking shadow pages in  the shadow list
		page_cnt += vec.len;

		// Call shadow
		target_uaddr = vma->vm_start + (vec.page << PAGE_SHIFT);
		rc = exmap_shadow_pages(vma, ctx->iface_pin_thread, uaddr, target_uaddr,
														vec.len, vma->vm_page_prot);
		if (rc < 0) {
			mmap_read_unlock(vma->vm_mm);
			goto out;
		}

		uaddr += vec.len << PAGE_SHIFT;

		exmap_debug("shadow[%d]: off=%llu, len=%d, freed: %lu, rc: %ld",
				iface, (uint64_t) vec.page, (int) vec.len, rc);
	}

	interface->shadow_pages[interface->shadow_cnt].page_id 	= params->page_id;
	interface->shadow_pages[interface->shadow_cnt].count 		= page_cnt;
	interface->shadow_cnt++;
	mmap_read_unlock(vma->vm_mm);

out:
	return 0;
}

int
exmap_free(struct exmap_ctx *ctx, struct exmap_action_params *params) {
	int iface = params->interface;
	struct exmap_interface *interface = &(ctx->interfaces[iface]);
	struct vm_area_struct  *vma       = ctx->exmap_vma;
	unsigned int  iov_len             = params->iov_len;
	int idx, rc = 0, failed = 0;
	FREE_PAGES(free_pages);

	if (iov_len == 0)
		return failed;

	// Do we really need this lock?
	mmap_read_lock(vma->vm_mm);

	for (idx = 0; idx < iov_len; idx++) {
		struct exmap_iov vec = READ_ONCE(interface->usermem->iov[idx]);
		unsigned long uaddr = vma->vm_start + (vec.page << PAGE_SHIFT);
		unsigned long old_free_count = free_pages.count;

		/* FIXME what if vec.len == 0 */
		/* if (vec.len == 0) */
		/* 	continue; */

		rc = exmap_unmap_pages(vma, uaddr, (int) vec.len, &free_pages);

		exmap_debug("free[%d]: off=%llu, len=%d, freed: %lu",
				iface,
					(uint64_t) vec.page, (int) vec.len,
					free_pages.count - old_free_count);

		if (rc < 0) failed++;
		vec.res = rc;
		vec.pages = free_pages.count - old_free_count;

		interface->count.e += vec.pages;

		WRITE_ONCE(interface->usermem->iov[idx], vec);

#ifndef BATCH_TLB_FLUSH
		flush_tlb_mm(vma->vm_mm);
#endif
	}

	// Flush the TLB of this CPU!
	// __flush_tlb_all(); 	// Please note: This is no shootdown!
#ifdef BATCH_TLB_FLUSH
	flush_tlb_mm(vma->vm_mm);
#endif

	// Update the RSS counter once!
	// add_mm_counter(vma->vm_mm, MM_FILEPAGES, -1 * free_pages.count);

	exmap_free_pages(ctx, interface, &free_pages);

	mmap_read_unlock(vma->vm_mm);

	return failed;
}

typedef int (*exmap_action_fptr)(struct exmap_ctx *, struct exmap_action_params *);

static exmap_action_fptr exmap_action_array[] = {
	[EXMAP_OP_ALLOC]  = &exmap_alloc,
	[EXMAP_OP_FREE]   = &exmap_free,
	[EXMAP_OP_SHADOW] = &exmap_shadow,
	[EXMAP_OP_RM_SD] 	= &exmap_remove_shadow,
};

static long exmap_ioctl (struct file *file, unsigned int cmd, unsigned long arg)
{
	struct exmap_ioctl_setup  setup;
	struct exmap_action_params action;
	struct exmap_ctx *ctx;
	struct exmap_interface *interface;
	int rc = 0, idx;
	gfp_t gfp_flags;

	ctx = (struct exmap_ctx*) file->private_data;

	switch(cmd) {
	case EXMAP_IOCTL_SETUP:
		if( copy_from_user(&setup, (struct exmap_ioctl_setup *) arg,
						   sizeof(struct exmap_ioctl_setup)) )
			return -EFAULT;

		/* process data and execute command */
		pr_info("setup.buffer_size = %ld", setup.buffer_size);

		// Interfaces can only be initialized once
		/* pr_info("setup.interfaces = %p", ctx->interfaces); */
		if (ctx->interfaces)
			return -EBUSY;

		if (setup.fd >= 0) {
			struct file *file = fget(setup.fd);
			struct inode *inode;

			if (!file) goto out_fput;

			if (!(file->f_flags & O_DIRECT)) {
				pr_err("Please give an O_DIRECT fd");
				goto out_fput;
			}

			if (!(file->f_mode & FMODE_READ)) {
				pr_err("Please give a readable fd");
				goto out_fput;
			}

			inode = file_inode(file);
			if (!S_ISBLK(inode->i_mode)) {
				pr_err("Only support for block devices at the moment");
				goto out_fput;
			}

			ctx->file_backend = file;
			ctx->bdev = I_BDEV(file->f_mapping->host);

			pr_info("setup.fd: %d (bdev=%p)", setup.fd, ctx->bdev);

			if (false) {
			out_fput:
				fput(file);
				return -EBADF;

			}
		}

		// // Account for the locked memory
		// rc = exmap_account_mem(ctx, (setup.buffer_size - ctx->buffer_size));
		// if (rc < 0) {
		// 	pr_info("Cannot account for memory. rlimit exceeded");
		//     return rc;
		// }
		ctx->buffer_size += setup.buffer_size;
		atomic_set(&ctx->alloc_count, 0);

		if (setup.max_interfaces > 256)
			return -EINVAL;

		ctx->max_interfaces = setup.max_interfaces;
		/* warn if more interfaces are created than there are CPUs */
		if (num_online_cpus() < setup.max_interfaces) {
			pr_warn("exmap: More interfaces (%u) than CPUs (%u)\n", setup.max_interfaces, num_online_cpus());
		}

		gfp_flags = GFP_KERNEL_ACCOUNT | __GFP_ZERO | __GFP_NOWARN | __GFP_COMP | __GFP_NORETRY;
		ctx->interfaces = __vmalloc_array(setup.max_interfaces, sizeof(struct exmap_interface), gfp_flags);
		if (!ctx->interfaces) {
			pr_info("interfaces failed");
			return -ENOMEM;
		}

		// Determine whether the interface is pin to CPU core
		ctx->iface_pin_thread = (setup.flags == EXMAP_CPU_AFFINITY);

		for (idx = 0; idx < ctx->max_interfaces; idx++) {
			interface = &ctx->interfaces[idx];

			// Allocate user facing memory
			interface->usermem = exmap_mem_alloc(sizeof(struct exmap_user_interface));
			if (!interface->usermem) {
				// BUG_ON(!interface->usermem); // Lost Memory....
				pr_info("usermem failed");
				ctx->interfaces = NULL;
				return -ENOMEM;
			}

			/* initialize counters */
			interface->count.a = 0;
			interface->count.e = 0;
			interface->count.r = 0;
			interface->count.s = 0;
			interface->count.p = 0;

			/* set default page steal target to a value chosen by fair dice roll */
			interface->steal_target = (idx + 4) % ctx->max_interfaces;

			mutex_init(&interface->interface_lock);
			free_pages_init(&interface->free_pages);
			interface->shadow_cnt = 0;
		}

		// 2. Allocate Memory from the system
		add_mm_counter(current->mm, MM_FILEPAGES, ctx->buffer_size);

		break;

	case EXMAP_IOCTL_ACTION:
		if (unlikely(ctx->interfaces == NULL))
			return -EBADF;

		if( copy_from_user(&action, (struct exmap_action_params *) arg,
						   sizeof(struct exmap_action_params)) )
			return -EFAULT;

		if (unlikely(action.interface >= ctx->max_interfaces))
			return -EINVAL;

		if (action.opcode > ARRAY_SIZE(exmap_action_array)
			|| !exmap_action_array[action.opcode])
			return -EINVAL;

		mutex_lock(&(ctx->interfaces[action.interface].interface_lock));
		rc = exmap_action_array[action.opcode](ctx, &action);
		mutex_unlock(&(ctx->interfaces[action.interface].interface_lock));
		break;
	default:
		return -EINVAL;
	}

	return rc;
}

ssize_t exmap_alloc_iter(struct exmap_ctx *ctx, struct exmap_interface *interface, struct iov_iter *iter) {
	ssize_t total_nr_pages = iov_iter_count(iter) >> PAGE_SHIFT;
	struct iov_iter_state iter_state;
	FREE_PAGES(free_pages);
	int rc, rc_all = 0;

	// Allocate Memory for all Vector Entries
	rc = exmap_alloc_pages(ctx, interface, &free_pages,
						   total_nr_pages);
	if (rc < 0) return rc;

	iov_iter_save_state(iter, &iter_state);
	while (iov_iter_count(iter)) {
		char __user* addr;
		ssize_t size;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
		addr = iter_iov_addr(iter);
		size = iter_iov_len(iter);
#else
		struct iovec iovec = iov_iter_iovec(iter);
		addr = iovec.iov_base;
		size = iovec.iov_len;
#endif


		//if (iovec.iov_len != iov_iter_count(iter)) {
		//	pr_info("exmap: BUG we currently support only iovectors of length 1\n");
		//	return -EINVAL;
		//}
		// pr_info("iov: %lx + %ld (of %ld)\n", (uintptr_t)addr, size >> PAGE_SHIFT, iov_iter_count(iter));

		if (ctx->exmap_vma->vm_start > (uintptr_t) addr) {
			pr_info("vmstart");
			return -EINVAL;
		}
		if (ctx->exmap_vma->vm_end < (uintptr_t) addr) {
			pr_info("vmend");
			return -EINVAL;
		}
		if (((uintptr_t) addr) & ~PAGE_MASK) // Not aligned start
		{
			pr_info("addr");
			return -EINVAL;
		}
		if (((uintptr_t) size) & ~PAGE_MASK) { // Not aligned end
			pr_info("size");
			return -EINVAL;
		}

		rc = exmap_insert_pages(ctx->exmap_vma, (uintptr_t) addr,
								(size >> PAGE_SHIFT),
								&free_pages, NULL,NULL);
		if (rc < 0) return rc;
		rc_all += rc;

		iov_iter_advance(iter, size);
	}

	if (free_pages.count > 0) { // Free the leftover pages
		exmap_free_pages(ctx, interface, &free_pages);
	}

	iov_iter_restore(iter, &iter_state);

	return rc_all;
}


ssize_t exmap_read_iter(struct kiocb* kiocb, struct iov_iter *iter) {
	struct file *file = kiocb->ki_filp;
	struct exmap_ctx *ctx = (struct exmap_ctx *) file->private_data;
	unsigned int iface_id = kiocb->ki_pos & 0xff;
	unsigned int action = (kiocb->ki_pos >> 8) & 0xff;
	struct exmap_interface *interface;

	int rc, rc_all = 0;

	if (action != EXMAP_OP_READ && action != EXMAP_OP_ALLOC) {
		return -EINVAL;
	}

	if (iface_id >= ctx->max_interfaces) {
		pr_info("max");
		return -EINVAL;
	}
	interface = &ctx->interfaces[iface_id];

	// Allocate Memory in Area
	rc = exmap_alloc_iter(ctx, interface, iter);
	if (rc < 0) return rc;

	// EXMAP_OP_READ == 0
	if (action != EXMAP_OP_READ) {
		pr_info("How the fuck");
		return rc;
	} else {
		if (!(ctx->file_backend && ctx->file_backend->f_op->read_iter)){
			pr_info("nofile");
			return -EINVAL;
		}
	}

	while (iov_iter_count(iter)) {
		char __user* addr;
		ssize_t size;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
		addr = iter_iov_addr(iter);
		size = iter_iov_len(iter);
#else
		struct iovec iovec = iov_iter_iovec(iter);
		addr = iovec.iov_base;
		size = iovec.iov_len;
#endif
		loff_t  disk_offset = (uintptr_t)addr - ctx->exmap_vma->vm_start;
		struct iov_iter_state iter_state;

		// pr_info("exmap: read  @ interface %d: %lu+%lu\n", iface_id, disk_offset, size);

		kiocb->ki_pos = disk_offset;
		kiocb->ki_filp = ctx->file_backend;

		iov_iter_save_state(iter, &iter_state);
		iov_iter_truncate(iter, size);
		rc = call_read_iter(ctx->file_backend, kiocb, iter);
		iov_iter_restore(iter, &iter_state);

		if (rc < 0) return rc;

		rc_all += rc;

		iov_iter_advance(iter, size);
	}

	return rc_all;
}

static const struct file_operations fops = {
	.mmap = exmap_mmap,
	.open = open,
	.read_iter = exmap_read_iter,
	.release = release,
	.unlocked_ioctl = exmap_ioctl
};

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 2, 0)
static int dev_uevent_perms(const struct device *dev, struct kobj_uevent_env *env) {
	return add_uevent_var(env, "DEVMODE=%#o", 0666);
}
#else
static int dev_uevent_perms(struct device *dev, struct kobj_uevent_env *env) {
	return add_uevent_var(env, "DEVMODE=%#o", 0666);
}
#endif

static int exmap_init_module(void) {
	if (exmap_acquire_ksyms())
		goto out;

	if (alloc_chrdev_region(&device_number, 0, 2, "exmap") < 0)
		goto out;

	// Create two devices for two mapping
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
	if ((cl = class_create("exmap")) == NULL)
		goto out_unregister_chrdev_region;
#else
	if ((cl = class_create(THIS_MODULE, "exmap")) == NULL)
		goto out_unregister_chrdev_region;
#endif
	cl->dev_uevent = dev_uevent_perms;

	int major = MAJOR(device_number);
	dev_t my_device;
	for (int i = 0; i < MAX_EXMAP_DEVICES; i++) {
		my_device = MKDEV(major, i);
		cdev_init(&cdev[i], &fops);
		if (cdev_add(&cdev[i], my_device, 1) == -1)
			goto out_device_destroy;
		if (device_create(cl, NULL, my_device, NULL, "exmap%d", i) == NULL)
			goto out_class_destroy;
	}
	printk(KERN_INFO "exmap registered");
	return 0;

 out_device_destroy:
	for (int i = 0; i < MAX_EXMAP_DEVICES; i++) {
		my_device = MKDEV(major, i);
		cdev_del(&cdev[i]);
		device_destroy(cl, my_device);
	}
 out_class_destroy:
	class_destroy(cl);
 out_unregister_chrdev_region:
	unregister_chrdev_region(device_number, MAX_EXMAP_DEVICES);
 out:
	return -1;
}

static void exmap_cleanup_module(void) {
	int major = MAJOR(device_number);
	dev_t my_device;
	for (int i = 0; i < MAX_EXMAP_DEVICES; i++) {
		my_device = MKDEV(major, i);
		cdev_del(&cdev[i]);
		device_destroy(cl, my_device);
	}
	class_destroy(cl);
	unregister_chrdev_region(device_number, MAX_EXMAP_DEVICES);
	printk(KERN_INFO "exmap unregistered");
}

module_init(exmap_init_module)
module_exit(exmap_cleanup_module)
MODULE_LICENSE("GPL");
