#ifndef _NVMEVIRT_TSU_FAIRNESS_H
#define _NVMEVIRT_TSU_FAIRNESS_H

#include <float.h>
#include <linux/sched/clock.h>
#include <linux/delay.h>
#include <linux/bitmap.h>
#include <linux/ktime.h>
#include "nvmev.h"
#include "ssd.h"
#include "zns_ftl.h"

#define HASH_TABLE_SIZE 11

struct nvme_tsu_tr_list_entry{
    unsigned int channel;
    unsigned int chip;
    unsigned int entry;
    int die;
    unsigned int sqid;

    uint64_t estimated_alone_waiting_time;
    bool is_copyed;
    struct list_head list;
};

struct stream_data {
    int stream_id;
    double slow_down;
    unsigned int transaction_count;
    // other data fields...
    struct list_head list;
};

struct stream {
    int stream_id;
    double global_slowdown; 
    double *chip_queue_slowdown;
    
    unsigned int nr_processed;
};

struct transaction_entry{
    unsigned int channel;
    unsigned int chip;
    unsigned int entry;
    int die;
    bool is_completed;

    unsigned int prev, next;
};

struct die_queue_entry {
    unsigned int channel;
    unsigned int chip;
    unsigned int entry;
    unsigned int idx;
    int die;
    bool is_copyed;

    struct list_head list;
};

struct transaction_package {
    DECLARE_BITMAP(used_die_bitmap, LUNS_PER_CHIP);
    unsigned int package_id;
    unsigned int nr_transactions;
    double avg_slowdown;

    struct transaction_entry* transactions;
    struct list_head list;
};

struct nvmev_process_queue {
    struct transaction_entry* queue;

    unsigned int nr_trs_in_fly;
    unsigned int free_seq; /* free io req head index */
	unsigned int free_seq_end; /* free io req tail index */
	unsigned int io_seq; /* io req head index */
	unsigned int io_seq_end; /* io req tail index */
};

// 查找链表中是否存在指定值
#define FIND_ELEMENT(list_head, key) \
    ({ \
        struct stream_data *node, *found = NULL; \
        list_for_each_entry(node, list_head, list) { \
            if (node->stream_id == key) { \
                found = node; \
                break; \
            } \
        } \
        found; \
    })


static inline unsigned long long __get_clock(void)
{
	return cpu_clock(nvmev_vdev->config.cpu_nr_dispatcher);
}

/**
 * Determine whether there is a zone conflict between two transactions.
*/
static bool check_zone_conflict(struct nvmev_tsu_tr* tr1, struct nvmev_tsu_tr* tr2){
	return tr1->ppa->g.lun == tr2->ppa->g.lun;
}

static void clear_die_used(struct transaction_package* package, int die_number)
{
    if (die_number < LUNS_PER_CHIP) {
        clear_bit(die_number, package->used_die_bitmap);
    }
}

static void set_die_used(struct transaction_package* package, int die_number){
    if (die_number < LUNS_PER_CHIP) {
        set_bit(die_number, package->used_die_bitmap);
    }
}

static bool is_die_used(struct transaction_package* package, int die_number) {
    if (die_number < LUNS_PER_CHIP) {
        return test_bit(die_number, package->used_die_bitmap);
    }
    return false; 
}

static void insert_to_die_queue (
    struct nvmev_die_queue *die_queue, 
    unsigned int channel, unsigned int chip, 
    unsigned int die, unsigned int tr_entry) {
    struct die_queue_entry *entry = kzalloc(sizeof(struct die_queue_entry), GFP_KERNEL);
    *entry = (struct die_queue_entry){
        .channel = channel,
        .chip = chip,
        .die = die,
        .entry = tr_entry,
        .is_copyed = false,
        .list = LIST_HEAD_INIT(entry->list),
    };

    list_add_tail(&entry->list, &die_queue->transactions_list);
} 

static struct transaction_package* generate_package_and_insert_to_chip_queue(struct nvmev_transaction_queue* chip_queue){
    struct transaction_package *package = kzalloc(sizeof(struct transaction_package), GFP_KERNEL);
	*package = (struct transaction_package){
		.avg_slowdown = 0.0,
		.nr_transactions = 0,
		.list = LIST_HEAD_INIT(package->list),
	};
    package->transactions = kzalloc(sizeof(struct transaction_entry) * chip_queue->nr_luns, GFP_KERNEL);

	list_add_tail(&package->list, &chip_queue->transaction_package_list);
    chip_queue->nr_packages++;
    return package;
}

static void insert_tr_to_package(struct transaction_package* package, unsigned int entry, int channel,  int chip, int die){
    package->transactions[die].channel = channel;
    package->transactions[die].chip = chip;
    package->transactions[die].entry = entry;
    package->transactions[die].die = die;
    package->nr_transactions++;
}

static void clear_entries_in_die_queue(struct nvmev_die_queue *die_queue) {
    struct die_queue_entry *pos, *next;
    list_for_each_entry_safe(pos, next, &die_queue->transactions_list, list) {
        list_del(&pos->list);
        kfree(pos);
    }
}

static void clear_copyed_entries_in_die_queue(struct nvmev_die_queue *die_queue) {
    struct die_queue_entry *pos, *next;
    list_for_each_entry_safe(pos, next, &die_queue->transactions_list, list) {
        if (pos->is_copyed == true) {
            list_del(&pos->list);
            kfree(pos);
        }
    }
}


void schedule_fairness(struct nvmev_tsu* tsu);
void __reclaim_transaction_in_process_queue(struct nvmev_process_queue* process_queue);
#endif