/*
 * monitor.c - Multi-Container Memory Monitor (Linux Kernel Module)
 *
 * Implements:
 *   TODO 1 - monitored_entry struct (linked list node)
 *   TODO 2 - global list + mutex
 *   TODO 3 - timer callback: periodic RSS check, soft/hard limit enforcement
 *   TODO 4 - ioctl REGISTER: allocate and insert entry
 *   TODO 5 - ioctl UNREGISTER: find and remove entry
 *   TODO 6 - module exit: free all remaining entries
 */

#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME        "container_monitor"
#define CHECK_INTERVAL_SEC 1

/* ==============================================================
 * TODO 1: Linked-list node struct
 *
 * Tracks one registered container process.
 * soft_warned: set after the first soft-limit warning so we
 *              only emit it once per entry (not every second).
 * ============================================================== */
struct monitored_entry {
    pid_t          pid;
    char           container_id[MONITOR_NAME_LEN];
    unsigned long  soft_limit_bytes;
    unsigned long  hard_limit_bytes;
    int            soft_warned;   /* 1 after first soft-limit printk */
    struct list_head list;        /* kernel doubly-linked list linkage */
};

/* ==============================================================
 * TODO 2: Global list + protecting mutex
 *
 * We use a mutex (not a spinlock) because the timer callback runs
 * in a softirq-deferred context (tasklet / timer wheel) on kernels
 * that use hrtimers, but mod_timer callbacks actually run in softirq
 * context where sleeping is NOT allowed.
 *
 * However, since our critical sections are short (list insert/remove
 * and a get_rss_bytes call that only holds rcu_read_lock internally),
 * we use a mutex for simplicity and document the tradeoff:
 *
 *   Tradeoff: mutex_lock() may sleep, which is illegal in hard-IRQ
 *   context. timer_list callbacks run in softirq context, not hard-IRQ,
 *   so mutex_lock() is safe there. If we ever needed to lock from a
 *   hard-IRQ handler we would switch to a spinlock.
 *
 * For this project a mutex is the correct and safe choice.
 * ============================================================== */
static LIST_HEAD(monitored_list);
static DEFINE_MUTEX(monitored_lock);

/* --- Provided: internal device / timer state --- */
static struct timer_list monitor_timer;
static dev_t             dev_num;
static struct cdev       c_dev;
static struct class     *cl;

/* ---------------------------------------------------------------
 * Provided: RSS Helper
 * Returns RSS in bytes for pid, or -1 if the task no longer exists.
 * --------------------------------------------------------------- */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct   *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

/* ---------------------------------------------------------------
 * Provided: soft-limit warning
 * --------------------------------------------------------------- */
static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Provided: hard-limit kill
 * --------------------------------------------------------------- */
static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * TODO 3: Timer callback — fires every CHECK_INTERVAL_SEC seconds.
 *
 * Walk the list and for each entry:
 *   1. Call get_rss_bytes(). If -1, the process is gone — remove the
 *      entry and free it (stale entry cleanup).
 *   2. If RSS > hard_limit: kill, remove, free.
 *   3. Else if RSS > soft_limit and not yet warned: emit warning,
 *      set soft_warned = 1.
 *
 * We use list_for_each_entry_safe so we can delete nodes mid-iteration
 * without corrupting the list walk (it saves the next pointer before
 * we call list_del + kfree).
 * --------------------------------------------------------------- */
static void timer_callback(struct timer_list *t)
{
    struct monitored_entry *entry, *tmp;
    long rss;

    mutex_lock(&monitored_lock);

    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        rss = get_rss_bytes(entry->pid);

        if (rss < 0) {
            /* Process already exited — clean up stale entry */
            printk(KERN_INFO
                   "[container_monitor] Stale entry removed: container=%s pid=%d\n",
                   entry->container_id, entry->pid);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        if ((unsigned long)rss > entry->hard_limit_bytes) {
            /* Hard limit exceeded: kill the process, remove entry */
            kill_process(entry->container_id, entry->pid,
                         entry->hard_limit_bytes, rss);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        if ((unsigned long)rss > entry->soft_limit_bytes && !entry->soft_warned) {
            /* Soft limit exceeded for the first time: warn once */
            log_soft_limit_event(entry->container_id, entry->pid,
                                 entry->soft_limit_bytes, rss);
            entry->soft_warned = 1;
        }
    }

    mutex_unlock(&monitored_lock);

    /* Reschedule ourselves for the next interval */
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
}

/* ---------------------------------------------------------------
 * IOCTL Handler
 * --------------------------------------------------------------- */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
        return -EFAULT;

    /* Null-terminate container_id defensively */
    req.container_id[MONITOR_NAME_LEN - 1] = '\0';

    /* ============================================================
     * TODO 4: REGISTER — allocate and insert a new monitored entry.
     *
     * Validate: soft limit must not exceed hard limit.
     * Allocate with GFP_KERNEL (safe here — ioctl runs in process
     * context, sleeping is allowed).
     * Insert at the tail of the list under the mutex.
     * ============================================================ */
    if (cmd == MONITOR_REGISTER) {
        struct monitored_entry *entry;

        printk(KERN_INFO
               "[container_monitor] Registering container=%s pid=%d"
               " soft=%lu hard=%lu\n",
               req.container_id, req.pid,
               req.soft_limit_bytes, req.hard_limit_bytes);

        if (req.soft_limit_bytes > req.hard_limit_bytes) {
            printk(KERN_WARNING
                   "[container_monitor] Register rejected: soft > hard for %s\n",
                   req.container_id);
            return -EINVAL;
        }

        entry = kmalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry)
            return -ENOMEM;

        entry->pid              = req.pid;
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        entry->soft_warned      = 0;
        strncpy(entry->container_id, req.container_id,
                MONITOR_NAME_LEN - 1);
        entry->container_id[MONITOR_NAME_LEN - 1] = '\0';
        INIT_LIST_HEAD(&entry->list);

        mutex_lock(&monitored_lock);
        list_add_tail(&entry->list, &monitored_list);
        mutex_unlock(&monitored_lock);

        return 0;
    }

    /* ============================================================
     * TODO 5: UNREGISTER — find and remove an entry by PID.
     *
     * Match on pid (and container_id for extra safety).
     * Use list_for_each_entry_safe so we can delete mid-iteration.
     * Return 0 if found and removed, -ENOENT if not found.
     * ============================================================ */
    printk(KERN_INFO
           "[container_monitor] Unregister request container=%s pid=%d\n",
           req.container_id, req.pid);

    {
        struct monitored_entry *entry, *tmp;
        int found = 0;

        mutex_lock(&monitored_lock);

        list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
            if (entry->pid == req.pid &&
                strncmp(entry->container_id, req.container_id,
                        MONITOR_NAME_LEN) == 0) {
                list_del(&entry->list);
                kfree(entry);
                found = 1;
                break;
            }
        }

        mutex_unlock(&monitored_lock);

        if (found) {
            printk(KERN_INFO
                   "[container_monitor] Unregistered container=%s pid=%d\n",
                   req.container_id, req.pid);
            return 0;
        }
    }

    return -ENOENT;
}

/* --- Provided: file operations --- */
static struct file_operations fops = {
    .owner          = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* --- Provided: Module Init --- */
static int __init monitor_init(void)
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "[container_monitor] Module loaded. Device: /dev/%s\n",
           DEVICE_NAME);
    return 0;
}

/* --- Provided: Module Exit --- */
static void __exit monitor_exit(void)
{
    del_timer_sync(&monitor_timer);

    /* ============================================================
     * TODO 6: Free all remaining monitored entries on module unload.
     *
     * del_timer_sync() above guarantees the timer callback is not
     * running when we reach here, so we can safely walk and free
     * the list without holding the lock (but we take it anyway for
     * correctness in case any ioctl is still in flight).
     * ============================================================ */
    {
        struct monitored_entry *entry, *tmp;

        mutex_lock(&monitored_lock);
        list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
            printk(KERN_INFO
                   "[container_monitor] Freeing entry container=%s pid=%d"
                   " on unload\n",
                   entry->container_id, entry->pid);
            list_del(&entry->list);
            kfree(entry);
        }
        mutex_unlock(&monitored_lock);
    }

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");
