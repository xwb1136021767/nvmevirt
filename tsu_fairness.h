#ifndef _NVMEVIRT_TSU_FAIRNESS_H
#define _NVMEVIRT_TSU_FAIRNESS_H

#include <float.h>
#include <linux/sched/clock.h>
#include <linux/delay.h>
#include <linux/bitmap.h>
#include "nvmev.h"
#include "ssd.h"
#include "zns_ftl.h"

#define HASH_TABLE_SIZE 11

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
    unsigned int entry;
    struct list_head list;
};

struct transaction_package {
    DECLARE_BITMAP(used_die_bitmap, LUNS_PER_CHIP);
    unsigned int package_id;
    unsigned int nr_transactions;
    double avg_slowdown;


    struct list_head transactions_list;
    struct list_head list;
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

static struct transaction_package* generate_package_and_insert_to_chip_queue(struct nvmev_transaction_queue* chip_queue){
    struct transaction_package *package = kzalloc(sizeof(struct transaction_package), GFP_KERNEL);
	*package = (struct transaction_package){
		.avg_slowdown = 0.0,
		.nr_transactions = 0,
		.transactions_list = LIST_HEAD_INIT(package->transactions_list),
		.list = LIST_HEAD_INIT(package->list),
	};
	list_add(&chip_queue->transaction_package_list, &package->list);
    chip_queue->nr_packages++;
    return package;
}

static void insert_tr_to_package(struct transaction_package* package, unsigned int entry){
    struct transaction_entry *tr = kzalloc(sizeof(struct transaction_entry), GFP_KERNEL);
	*tr = (struct transaction_entry){
		.entry = entry,
		.list = LIST_HEAD_INIT(tr->list),
	};
	list_add_tail(&tr->list, &package->transactions_list);
    package->nr_transactions++;
}
static void clear_transactions_in_package(struct transaction_package *package) {
    struct transaction_entry *entry, *next_entry;
    struct list_head *pos, *temp;

    list_for_each_entry_safe(entry, next_entry, &package->transactions_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }
}

static void clear_packages(struct nvmev_transaction_queue* chip_queue){
    struct transaction_package *entry, *next_entry;
    struct list_head *pos, *temp;

    list_for_each_entry_safe(entry, next_entry, &chip_queue->transaction_package_list, list) {
        list_del(&entry->list);
        kfree(entry); 
    }
    chip_queue->nr_packages = 0;
}

void schedule_fairness(struct nvmev_tsu* tsu);

#endif