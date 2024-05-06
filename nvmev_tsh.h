#ifndef _NVMEVIRT_NVMEV_TSU_H
#define _NVMEVIRT_NVMEV_TSU_H
#include <linux/sched/clock.h>
#include <linux/delay.h>
#include <linux/ktime.h>
#include <linux/bitmap.h>
#include "nvmev.h"
#include "ssd.h"
#include "zns_ftl.h"


static LIST_HEAD(global_flow_infos);
struct nvmev_tsu_flow_info{
    unsigned int sqid;
    double global_slowdown;
    double sum_slowdown;
    double local_slowdown;
    unsigned int local_transactions;
    unsigned int nr_transactions;

    struct list_head list;
};

struct nvme_tsu_tr_list_entry{
    unsigned int channel;
    unsigned int chip;
    unsigned int entry;
    int die;
    unsigned int sqid;
    uint32_t zid;

    uint64_t estimated_alone_waiting_time;
    uint64_t transfer_time;
    uint64_t command_time;
    bool is_copyed;
    struct list_head list;
};

struct stream_data {
    int stream_id;
    double slow_down;
    unsigned int transaction_count;
	unsigned int channel;
	unsigned int chip;
	unsigned int die;
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
	unsigned int sqid;
	uint32_t zid;
    unsigned int channel;
    unsigned int chip;
    unsigned int entry;
    int die;
    bool is_copyed;
	uint64_t estimated_alone_waiting_time;
    uint64_t transfer_time;
    uint64_t command_time;

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


static bool is_lun_avail(struct nvmev_tsu_tr* tr){
	struct nand_lun *lun;
	struct nvmev_ns *ns = &nvmev_vdev->ns[tr->nsid];
    struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;
	struct ssd *ssd = zns_ftl->ssd;
	uint64_t now_time = __get_clock();
	struct ppa *ppa = tr->ppa;
	lun = get_lun(ssd, ppa);

	if(lun == NULL) return true;

	if(lun->next_lun_avail_time >= now_time)
		return false;
	else
		return true;
}

static unsigned int get_nr_used_dies(struct nvmev_transaction_queue* chip_queue)
{
	DECLARE_BITMAP(used_die_bitmap, LUNS_PER_CHIP);
	unsigned int curr = chip_queue->io_seq;
	struct nvmev_tsu_tr* tr;
	unsigned int nr_used_dies = 0;
	int lun;

	while(curr != -1){
		tr = &chip_queue->queue[curr];
		lun = tr->ppa->g.lun;
		if(tr->is_completed || tr->is_copyed) {
			curr = tr->next;
			continue;
		}

		if(!test_bit(lun, used_die_bitmap)){
			nr_used_dies++;
			set_bit(lun, used_die_bitmap);
		}
		curr = tr->next;
	}
	return nr_used_dies;
}

static void move_forward(
	struct nvmev_transaction_queue* chip_queue,
	struct list_head* tmp_queue,
	uint32_t zid 
){
	LIST_HEAD(group_with_same_zone);
	struct nvme_tsu_tr_list_entry *entry, *tmp;
	unsigned int tr_entry;
	struct nvmev_tsu_tr* tr;

	// 1. 将属于zid的请求都转移到链表group_with_same_zone
	list_for_each_entry_safe(entry, tmp, tmp_queue, list){
		tr_entry = entry->entry;
		tr = &chip_queue->queue[tr_entry];
		if(tr->zid == zid){
			list_move_tail(&entry->list, &group_with_same_zone);
		}
	}

	// 2. 将group_with_same_zone中的请求插入tmp_queue首部
	list_splice_init(&group_with_same_zone, tmp_queue);
}


// operations for struct nvme_tsu_tr_list_entry.
static struct die_queue_entry* find_die_entry(
	struct list_head* tr_list,
	unsigned int sqid
){
	struct die_queue_entry *entry, *result = NULL;
	list_for_each_entry(entry, tr_list, list){
		if(entry->sqid == sqid){
			result = entry;
			break;
		}
	}
	return result;
}

static struct die_queue_entry* find_die_entry_reverse(
	struct list_head* tr_list,
	unsigned int sqid
){
	struct die_queue_entry *entry, *result = NULL;
	list_for_each_entry_reverse(entry, tr_list, list){
		if(entry->sqid == sqid){
			result = entry;
			break;
		}
	}
	return result;
}

static void clear_die_entry(struct list_head* tr_list){
	struct die_queue_entry *entry, *tmp;
	list_for_each_entry_safe(entry, tmp, tr_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }
}


// Display info for debug.
static void traverse_tmp_queue(
	struct nvmev_transaction_queue* chip_queue,
	struct list_head* tmp_queue,
	bool before){
	struct nvme_tsu_tr_list_entry *entry, *tmp;
	unsigned int idx = 0;
	unsigned int tr_entry;
	struct nvmev_tsu_tr* tr;

	if(before)
		NVMEV_INFO("======================traverse-tmp_queue-before======================\n");
	else
		NVMEV_INFO("======================traverse-tmp_queue-after======================\n");

	list_for_each_entry_safe(entry, tmp, tmp_queue, list){
		tr_entry = entry->entry;
		tr = &chip_queue->queue[tr_entry];
		NVMEV_INFO("idx: %d, sqid: %d zone : %d\n", idx, tr->sqid, tr->zid);
	}

	if(before)
		NVMEV_INFO("======================traverse-tmp_queue-before======================\n");
	else
		NVMEV_INFO("======================traverse-tmp_queue-after======================\n");
}

static void display_chip_queue(struct nvmev_transaction_queue* chip_queue){
	unsigned int curr = chip_queue->io_seq;
	struct nvmev_tsu_tr* tr;
	NVMEV_INFO("------chip_queue begin-------------");
	while(curr != -1){
		tr = &chip_queue->queue[curr];
		if(tr->is_completed) {
			curr = tr->next;
			continue;
		}

		NVMEV_INFO("sqid: %d entry=%d ch=%d chip=%d die=%d is_completed=%d is_reclaim_by_ret=%d\n", tr->sqid, curr, tr->ppa->g.ch, tr->ppa->g.chip, tr->ppa->g.lun, tr->is_completed, tr->is_reclaim_by_ret);
		curr = tr->next;
	}
	NVMEV_INFO("------chip_queue end-------------");
}

static void print_process_queue(struct nvmev_process_queue* process_queue) {
	unsigned int curr = process_queue->io_seq;
	struct transaction_entry* tr;
	if(curr == -1) return;
	NVMEV_INFO("------process_queue begin-------------");
	while(curr != -1) {
		tr = &process_queue->queue[curr];
		NVMEV_INFO("entry: %d die: %d \n", tr->entry, tr->die);
		curr = tr->next;
	}
	NVMEV_INFO("------process_queue end-------------");
}

static void print_tmp_queue(struct list_head* tmp_queue){
	struct nvme_tsu_tr_list_entry *entry;
	unsigned int idx = 0;
    NVMEV_INFO("******************tmp_queue**************************");
	list_for_each_entry(entry, tmp_queue, list){
		NVMEV_INFO("sqid: %d estimated_alone_waiting_time: %lld \n", entry->sqid, entry->estimated_alone_waiting_time);
	}
	NVMEV_INFO("*******************tmp_queue*************************");
}

static void print_queue_statistics(struct nvmev_transaction_queue* chip_queue, int channel, int chip){
	NVMEV_INFO("**********chip-queue(channel: %d, chip: %d) statistics*************", channel, chip);

	NVMEV_INFO("num_of_scheduling: %d\n", chip_queue->queue_probe.num_of_scheduling);
	NVMEV_INFO("max_slowdown: %d\n", (unsigned int)chip_queue->queue_probe.max_slowdown*1000);
	// NVMEV_INFO("min_slowdown: %d\n", (unsigned int)chip_queue->queue_probe.min_slowdown*1000);
	NVMEV_INFO("max_fairness: %d\n", (unsigned int)chip_queue->queue_probe.max_fairness);
	NVMEV_INFO("min_fairness: %d\n", (unsigned int)chip_queue->queue_probe.min_fairness);
	NVMEV_INFO("avg_fairness: %d\n", (unsigned int)chip_queue->queue_probe.avg_fairness);
	NVMEV_INFO("avg_waiting_times: %lld\n", chip_queue->queue_probe.avg_waiting_times);
	NVMEV_INFO("max_waiting_times: %lld\n", chip_queue->queue_probe.max_waiting_times);
	NVMEV_INFO("max_queue_length: %d\n", chip_queue->queue_probe.max_queue_length);

	NVMEV_INFO("********************************************");
}


static void delete_node(struct nvmev_transaction_queue* chip_queue, unsigned int node) {
	unsigned int prev = chip_queue->queue[node].prev;
	unsigned int next = chip_queue->queue[node].next;

	if(prev != -1)
		chip_queue->queue[prev].next = next;
	if(next != -1)
		chip_queue->queue[next].prev = prev;
	chip_queue->queue[node].prev = -1;
	chip_queue->queue[node].next = -1;
	if(node == chip_queue->io_seq_end) 
		chip_queue->io_seq_end = prev;
}

static void add_node_after(struct nvmev_transaction_queue* chip_queue, unsigned int node, unsigned int curr) {
	unsigned int prev = chip_queue->queue[curr].prev;
	unsigned int next = chip_queue->queue[curr].next;

	chip_queue->queue[node].prev = curr;
	chip_queue->queue[node].next = next;
	chip_queue->queue[curr].next = node;
	if(next != -1)
		chip_queue->queue[next].prev = node;
	if(curr == chip_queue->io_seq_end)
		chip_queue->io_seq_end = node;
}

static void move_node_after(struct nvmev_transaction_queue* chip_queue, unsigned int node, unsigned int curr) {
	// Delete node from origin position.
	delete_node(chip_queue, node);
	// Add node to the back of curr.
	add_node_after(chip_queue, node, curr);
}



static uint64_t convert_clock_to_ms(uint64_t cycles){
	uint64_t delta_ns = 0, delta_ms = 0;
	delta_ns = cycles * 1000000000 / cpu_khz;
	delta_ms = delta_ns / 1000000;
	return delta_ms;
}

static struct nvmev_process_queue* __alloc_process_queue_entry(struct nvmev_process_queue* process_queue, unsigned int* entry) {
    unsigned int e = process_queue->free_seq;
    struct transaction_entry* tr = process_queue->queue + e;
    if(tr->next >= NR_MAX_PROCESS_IO){
        WARN_ON_ONCE("Process queue is almost full");
		return NULL;
    }

    process_queue->free_seq = tr->next;
    BUG_ON(process_queue->free_seq >= NR_MAX_PROCESS_IO);
    *entry = e;
    return process_queue;
}

 static void __insert_tr_to_process_queue(unsigned int entry, struct nvmev_process_queue *process_queue)
{

	if(process_queue->io_seq == -1){
		process_queue->io_seq = entry;
		process_queue->io_seq_end = entry;
	}else{
		unsigned int curr = process_queue->io_seq_end;
		if(curr == -1){
			return;
		}
		process_queue->queue[entry].prev = curr;
		process_queue->io_seq_end = entry;
		process_queue->queue[curr].next = entry;
	}
	process_queue->nr_trs_in_fly++;

}

 static void __enqueue_transaction_to_process_queue(
	struct nvmev_process_queue* process_queue, 
 	unsigned int tr_entry, unsigned int channel, 
	unsigned int chip, unsigned int die) 
{
    unsigned int entry;
    struct nvmev_process_queue* queue;
    struct transaction_entry* tr;
    queue = __alloc_process_queue_entry(process_queue, &entry);
    if(!queue)
			return;

    tr = process_queue->queue + entry;
	tr->channel = channel;
	tr->chip = chip;
    tr->die = die;
    tr->entry = tr_entry;
	tr->is_completed = false;
    tr->next = -1;
    tr->prev = -1;

	// NVMEV_INFO("insert entry: %d die: %d to process queue\n", tr_entry, die);
    __insert_tr_to_process_queue(entry, process_queue);
}

 static void __reclaim_transaction_in_process_queue(struct nvmev_process_queue* process_queue) {
    unsigned int first_entry = -1;
    unsigned int last_entry = -1;
    unsigned int curr;
    int nr_reclaimed = 0;
    struct transaction_entry* tr;

    first_entry = process_queue->io_seq;
    curr = first_entry;
    while (curr != -1) {
        tr = &process_queue->queue[curr];
        if (tr->is_completed == true) {
            last_entry = curr;
            curr = tr->next;
            nr_reclaimed++;
        } else {
            break;
        }
    }

    if (last_entry != -1) {
        // spin_lock(&worker->entry_lock);
        tr = &process_queue->queue[last_entry];
        process_queue->io_seq = tr->next;
        if (tr->next != -1) {
            process_queue->queue[tr->next].prev = -1;
        }
        tr->next = -1;

        tr = &process_queue->queue[first_entry];
        tr->prev = process_queue->free_seq_end;

        tr = &process_queue->queue[process_queue->free_seq_end];
        tr->next = first_entry;

        process_queue->free_seq_end = last_entry;
        // NVMEV_INFO("%s: %u -- %u, %d\n", __func__,
        //         first_entry, last_entry, nr_reclaimed);
    }
}

static void copy_to_process_queue(struct nvmev_transaction_queue* chip_queue, struct nvmev_process_queue* process_queue) {
	unsigned int curr = chip_queue->io_seq;
	struct nvmev_tsu_tr* tr;
	int channel, chip, die;
	int entry = 0;
	while(curr != -1){
		tr = &chip_queue->queue[curr];
		if(tr->is_completed || tr->is_copyed){
			curr = tr->next;
			continue;
		}

		if(!is_lun_avail(tr)){
			curr = tr->next;
			continue;
		}

		channel = tr->ppa->g.ch;
		chip = tr->ppa->g.chip;
		die = tr->ppa->g.lun;
		
		// estimate_alone_waiting_time(chip_queue, curr);
		// NVMEV_INFO("In die-%d queue, entry : %d, estimated_alone_waiting_time = %lld\n", die, entry, tr->estimated_alone_waiting_time);
		__enqueue_transaction_to_process_queue(process_queue, curr, channel, chip, die);
		// spin_lock(&chip_queue->tr_lock);
		tr->is_copyed = true;
		// spin_unlock(&chip_queue->tr_lock);
		curr = tr->next;
		entry++;
	}
}

#endif