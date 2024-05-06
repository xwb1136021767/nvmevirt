#ifndef _NVMEVIRT_TSU_FAIRNESS_H
#define _NVMEVIRT_TSU_FAIRNESS_H

#include "nvmev_compute_fairness.h"

#define HASH_TABLE_SIZE 11
#define F_THR 0.9

static bool check_zone_conflict(struct nvmev_tsu_tr* tr1, struct nvmev_tsu_tr* tr2){
	return tr1->ppa->g.lun == tr2->ppa->g.lun;
}

static struct nvmev_workload_global_info* create_global_info(
    struct list_head* workload_infos,
    unsigned int sqid
){
    struct nvmev_workload_global_info *pos;
    pos = kzalloc(sizeof(struct nvmev_workload_global_info), GFP_KERNEL);
    *pos = (struct nvmev_workload_global_info){
        .global_slowdown = 0.0,
        .local_infos = NULL,
        .nr_processed = 0,
        .sqid = sqid,
        .local_infos = LIST_HEAD_INIT(pos->local_infos),
        .list = LIST_HEAD_INIT(pos->list),
    };
    list_add_tail(&pos->list, workload_infos);
    return pos;
}

static struct nvmev_workload_global_info* find_global_workload_info(
    struct list_head* workload_infos,
    unsigned int sqid
){
    // NVMEV_INFO("find sqid = %d\n", sqid);
    struct nvmev_workload_global_info *pos, *next;
    list_for_each_entry_safe(pos, next, workload_infos, list) {
        // NVMEV_INFO("pos->sqid = %d, sqid = %d\n", pos->sqid, sqid);
        if(pos->sqid == sqid){
            return pos;
        }
    }

    return NULL;
}

static struct nvmev_workload_local_info* create_local_info(
    struct nvmev_workload_global_info* global_info,
    unsigned int channel,
    unsigned int chip,
    unsigned int die
){
    struct nvmev_workload_local_info *pos;
    pos = kzalloc(sizeof(struct nvmev_workload_local_info), GFP_KERNEL);
    *pos = (struct nvmev_workload_local_info){
        .channel = channel,
        .chip = chip,
        .die = die,
        .local_slowdown = 0.0,
        .nr_transactions_in_die_queue = 0,
        .nr_zones = 0,
        .completed_transactions_time = 0,
        .nr_enqueued_transactions = 0,
        .zone_infos = LIST_HEAD_INIT(pos->zone_infos),
        .list = LIST_HEAD_INIT(pos->list),
    };
    list_add_tail(&pos->list, &global_info->local_infos);
    return pos;
}



static struct nvmev_workload_local_info* find_local_workload_info(
    struct nvmev_workload_global_info* global_info,
    unsigned int channel,
    unsigned int chip,
    unsigned int die
){
    struct nvmev_workload_local_info *pos, *next;
    list_for_each_entry_safe(pos, next, &global_info->local_infos, list) {
        if(pos->channel == channel && pos->chip == chip && pos->die == die){
            return pos;
        }
    }

    return NULL;
}


static struct nvmev_zone_info* create_zone_info(
    struct nvmev_workload_local_info* local_info,
    uint32_t zid
){
    struct nvmev_zone_info *pos;
    pos = kzalloc(sizeof(struct nvmev_zone_info), GFP_KERNEL);
    *pos = (struct nvmev_zone_info){
        .zid = zid,
        .idx = local_info->nr_zones,
        .pos_head = NULL,
        .pos_tail = NULL,
        .list = LIST_HEAD_INIT(pos->list),
    };
    list_add_tail(&pos->list, &local_info->zone_infos);
    local_info->nr_zones++;
    return pos;
}

static struct nvmev_zone_info* find_zone_info(
    struct nvmev_workload_local_info* local_info,
    uint32_t zid
){
    struct nvmev_zone_info *pos, *next;
    list_for_each_entry_safe(pos, next, &local_info->zone_infos, list) {
        if(pos->zid == zid){
            return pos;
        }
    }
    return NULL;
}


static void insert_to_die_queue (
    struct nvmev_die_queue *die_queue, 
    struct list_head* workload_infos,
    struct nvmev_tsu_tr* tr,
    int curr
) {
    struct nvmev_workload_global_info* global_info;
    struct nvmev_workload_local_info* local_info;
    struct nvmev_zone_info* zone_info;
    struct die_queue_entry *entry = kzalloc(sizeof(struct die_queue_entry), GFP_KERNEL);
    *entry = (struct die_queue_entry){
        .sqid = tr->sqid,
        .channel = tr->ppa->g.ch,
        .chip = tr->ppa->g.chip,
        .die = tr->ppa->g.lun,
        .entry = curr,
        .zid = tr->zid,
        .transfer_time = tr->transfer_time,
        .command_time = tr->command_time,
        .is_copyed = false,
        .list = LIST_HEAD_INIT(entry->list),
    };

    global_info = find_global_workload_info(workload_infos, entry->sqid);
    if(global_info == NULL){
        global_info = create_global_info(workload_infos, entry->sqid);
    }
    
    local_info = find_local_workload_info(global_info, entry->channel, entry->chip, entry->die);
    if(local_info == NULL){
        local_info = create_local_info(global_info, entry->channel, entry->chip, entry->die);
    }

    zone_info = find_zone_info(local_info, entry->zid);
    if(zone_info == NULL){
        zone_info = create_zone_info(local_info, entry->zid);
    }

    // NVMEV_INFO("global_info = %p\n", &global_info);
    // NVMEV_INFO("local_info = %p\n", &local_info);
    // NVMEV_INFO("zone_info = %p\n", &zone_info);

    // NVMEV_INFO("entry = %p\n", &entry);
    // NVMEV_INFO("entry->list = %p\n", &entry->list);

    if(zone_info->pos_head == NULL && zone_info->pos_tail == NULL){
        zone_info->pos_head = &entry->list;
        list_add_tail(&entry->list, &die_queue->transactions_list);
    }else{
        list_add(&entry->list, zone_info->pos_tail);
    }

    zone_info->pos_tail = &entry->list;
    zone_info->nr_transactions++;
    if(local_info->nr_transactions_in_die_queue == 0){
        die_queue->nr_workloads++;
    }
    local_info->nr_transactions_in_die_queue++;
    local_info->nr_enqueued_transactions++;
    die_queue->nr_transactions++;
    // NVMEV_INFO("nr_transactions_in_die_queue = %d\n", local_info->nr_transactions_in_die_queue);
    // NVMEV_INFO("zid: %d zone_info->pos_head = %p\n", zone_info->zid, zone_info->pos_head);
    // NVMEV_INFO("zid: %d zone_info->pos_tail = %p\n", zone_info->zid, zone_info->pos_tail);
} 

static void clear_workload_infos(struct list_head* workload_infos){
    struct nvmev_workload_global_info* global_info, *next;
    struct nvmev_workload_local_info* local_info, *next_local;
    struct nvmev_zone_info* zone_info, *next_zone;

    list_for_each_entry_safe(global_info, next, workload_infos, list) {
        list_for_each_entry_safe(local_info, next_local, &global_info->local_infos, list){
            list_for_each_entry_safe(zone_info, next_zone, &local_info->zone_infos, list){
                list_del(&zone_info->list);
                kfree(zone_info);
            }
            list_del(&local_info->list);
            kfree(local_info);
        }
        list_del(&global_info->list);
        kfree(global_info);
    }
}

static void clear_entries_in_die_queue(struct nvmev_die_queue *die_queue) {
    struct die_queue_entry *pos, *next;
    list_for_each_entry_safe(pos, next, &die_queue->transactions_list, list) {
        list_del(&pos->list);
        kfree(pos);
    }
}

static void clear_zone_info(
    struct nvmev_die_queue *die_queue,
    struct nvmev_workload_local_info* local_info,
    struct nvmev_zone_info* zone_info
){
    zone_info->pos_head = NULL;
    zone_info->pos_tail = NULL;
    zone_info->nr_transactions = 0;
    local_info->nr_transactions_in_die_queue--;
    local_info->nr_zones--;
    if(local_info->nr_transactions_in_die_queue == 0){
        die_queue->nr_workloads--;
    }
    list_del(&zone_info->list);
    kfree(zone_info);
}

static void delete_dieentry_in_workload_info(
    struct list_head* workload_infos,
    struct nvmev_die_queue *die_queue,
    struct die_queue_entry *entry
){
    struct nvmev_workload_global_info* global_info;
    struct nvmev_workload_local_info* local_info;
    struct nvmev_zone_info* zone_info;
    struct die_queue_entry *next;
    bool has_next = (entry->list.next != &die_queue->transactions_list);



    global_info = find_global_workload_info(workload_infos, entry->sqid);
    if(global_info == NULL){
        NVMEV_INFO("Can't find global_info for delete_dieentry_in_workload_info\n");
        return;
    }
    local_info = find_local_workload_info(global_info, entry->channel, entry->chip, entry->die);
    if(local_info == NULL){
        NVMEV_INFO("Can't find local_info for delete_dieentry_in_workload_info\n");
        return;
    }
    zone_info = find_zone_info(local_info, entry->zid);
    if(zone_info == NULL){
        NVMEV_INFO("Can't find zone_info for delete_dieentry_in_workload_info\n");
        return;
    }

    if(zone_info->pos_head == zone_info->pos_tail){
        clear_zone_info(die_queue, local_info, zone_info);
    }else{
        zone_info->pos_head = zone_info->pos_head->next;
        zone_info->nr_transactions--;
        local_info->nr_transactions_in_die_queue--;
    }
    // NVMEV_INFO("nr_transactions_in_die_queue = %d\n", local_info->nr_transactions_in_die_queue);
    // next = container_of(entry->list.next, struct die_queue_entry, list);
    // if(next->zid == entry->zid){
    //     if(zone_info->pos_head == zone_info->pos_tail){
    //         zone_info->pos_tail = entry->list.next;
    //     }
    //     zone_info->pos_head = entry->list.next;
    //     zone_info->nr_transactions--;
    // }else{
    //     zone_info->pos_head = &die_queue->transactions_list;
    //     zone_info->pos_tail = &die_queue->transactions_list;
    //     zone_info->nr_transactions = 0;
    // }
}

static void clear_copyed_entries_in_die_queue(
    struct nvmev_die_queue *die_queue,
    struct list_head* workload_infos
) {
    struct die_queue_entry *pos, *next;
    list_for_each_entry_safe(pos, next, &die_queue->transactions_list, list) {
        if (pos->is_copyed == true) {
            // NVMEV_INFO("delete_dieentry_in_workload_info, ch: %d chip: %d die: %d sqid: %d entry: %d\n", pos->channel, pos->chip, pos->die, pos->sqid, pos->entry);
            delete_dieentry_in_workload_info(workload_infos, die_queue, pos);
            list_del(&pos->list);
            kfree(pos);
            die_queue->nr_transactions--;
        }
    }
}

static double add_double(double a, double b){
    double num1 = (unsigned int)(a * 1000);
    double num2 = (unsigned int)(b * 1000);
    double res = (double)((a+b)/1000);
    return res;
}

// 返回链表队列中某个节点的前n个位置的节点
static struct list_head *get_node_before_n(struct list_head *node, int n) {
    // 检查节点是否为空
    if (!node) {
        NVMEV_INFO("Error: Node is NULL\n");
        return NULL;
    }

    // 沿着链表的前向指针（prev）向前遍历n次
    while (n > 0 && node->prev != node) {
        node = node->prev;
        n--;
    }

    // 如果链表长度小于n，返回NULL
    if (n > 0) {
        NVMEV_INFO("Error: List length is less than n\n");
        return NULL;
    }

    // 将节点指针转换为 my_node 结构体指针，并返回
    return node;
}


void schedule_fairness(struct nvmev_tsu* tsu);
void __reclaim_transaction_in_process_queue(struct nvmev_process_queue* process_queue);
void print_queue_statistics(struct nvmev_transaction_queue* chip_queue, int channel, int chip);
#endif