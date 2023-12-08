#ifndef _NVMEVIRT_TSU_FAIRNESS_H
#define _NVMEVIRT_TSU_FAIRNESS_H

#include <float.h>
#include <linux/sched/clock.h>
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


void schedule_fairness(struct nvmev_tsu* tsu);

#endif