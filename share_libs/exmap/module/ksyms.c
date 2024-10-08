#include <linux/kallsyms.h>
#include <linux/fs.h>
#include <linux/mm_types.h>
#include <linux/types.h>
#include <linux/compiler_types.h>
#include <linux/rmap.h>
#include <linux/kprobes.h>
#include <linux/uio.h>

static struct kprobe kp = {
	.symbol_name = "kallsyms_lookup_name"
};

/* Auxiliary function pointers here */

static void (*flush_tlb_mm_range_ksym)(struct mm_struct *mm, unsigned long start,
								unsigned long end, unsigned int stride_shift,
								bool freed_tables);

static ssize_t (*vfs_read_ksym)(struct file *file, char __user *buf,
						 size_t count, loff_t *pos);

static void (*iov_iter_restore_ksym)(struct iov_iter *i, struct iov_iter_state *state);


typedef unsigned long (*kallsyms_lookup_name_t)(const char *name);

int exmap_acquire_ksyms(void)
{
	kallsyms_lookup_name_t kallsyms_lookup_name;

	/*
	 * From kernel 5.7.0 onwards, kallsyms_lookup_name
	 * is no longer exported by default. This workaround
	 * uses kprobes to find the address of the function.
	 */
	register_kprobe(&kp);
	kallsyms_lookup_name = (kallsyms_lookup_name_t) kp.addr;
	unregister_kprobe(&kp);
	/* 
	 * Try to find all necessary symbols,
	 * return -1 if any lookup fails
	 */
	flush_tlb_mm_range_ksym = (void *)kallsyms_lookup_name("flush_tlb_mm_range");
	if(!flush_tlb_mm_range_ksym)
		return -1;

	vfs_read_ksym = (void *)kallsyms_lookup_name("vfs_read");
	if(!vfs_read_ksym)
		return -1;

	iov_iter_restore_ksym = (void *)kallsyms_lookup_name("iov_iter_restore");
	if(!iov_iter_restore_ksym)
		return -1;


	return 0;
}

void flush_tlb_mm_range(struct mm_struct *mm, unsigned long start,
						unsigned long end, unsigned int stride_shift,
						bool freed_tables)
{
	flush_tlb_mm_range_ksym(mm, start, end, stride_shift, freed_tables);
}

ssize_t vfs_read(struct file *file, char __user *buf, 
				 size_t count, loff_t *pos)
{
	return vfs_read_ksym(file, buf, count, pos);
}

void iov_iter_restore(struct iov_iter *i, struct iov_iter_state *state)
{
	return iov_iter_restore_ksym(i, state);
}

#include <linux/version.h>
#if LINUX_VERSION_CODE < KERNEL_VERSION(5, 18, 0)
#include <linux/vmalloc.h>

void *__vmalloc_array(size_t n, size_t size, gfp_t flags)
{
	size_t bytes;

	if (unlikely(check_mul_overflow(n, size, &bytes)))
		return NULL;
	return __vmalloc(bytes, flags);
}
#endif
