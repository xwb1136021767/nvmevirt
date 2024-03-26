#include "tsu_fairness.h"


double fairness_based_on_local_slowdown(
	struct nvmev_transaction_queue* chip_queue, 
	struct list_head* tmp_queue,
	unsigned int curr,
	unsigned int* flow_with_max_average_slowdown
)
{
	LIST_HEAD(sum_slowdown);
	uint64_t total_finish_time = 0;
	struct nvmev_tsu_tr* tr = &chip_queue->queue[curr];
    struct nvmev_ns *ns = &nvmev_vdev->ns[tr->nsid];
    struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;
	struct ssdparams *spp;
    struct ssd *ssd = zns_ftl->ssd;
	double slowdown_max = DBL_MIN, slowdown_min = DBL_MAX;
	int stream_count = 0;
	struct stream_data* data;
	uint64_t tr_execute_time;
	struct nvme_tsu_tr_list_entry *entry, *tmp;
	unsigned int tr_entry;
	uint64_t transaction_alone_time, transaction_shared_time;
	spp = &ssd->sp;
	*flow_with_max_average_slowdown = tr->sqid;

	list_for_each_entry_safe(entry, tmp, tmp_queue, list){
		tr_entry = entry->entry;
		tr = &chip_queue->queue[tr_entry];
		tr_execute_time =  entry->transfer_time + entry->command_time;
		total_finish_time += tr_execute_time;

		transaction_alone_time = entry->estimated_alone_waiting_time + tr_execute_time;
		transaction_shared_time = total_finish_time + (__get_clock() - tr->stime);
		double slow_down = (double)transaction_shared_time / transaction_alone_time;
		struct stream_data *data = FIND_ELEMENT(&sum_slowdown, tr->sqid);
		if(data){
			data->slow_down += slow_down;
			data->transaction_count++;
		}else{
			struct stream_data *data = kmalloc(sizeof(struct stream_data), GFP_KERNEL);
			*data = (struct stream_data){
				.stream_id = tr->sqid,
				.slow_down = slow_down,
				.transaction_count = 1,
			};
			list_add(&data->list, &sum_slowdown);
		}
	}

	// Find the stream with max slowdown 
	list_for_each_entry(data, &sum_slowdown, list){
		stream_count++;
		double average_slowdown = data->slow_down / data->transaction_count;
		if (average_slowdown > slowdown_max)
		{
			slowdown_max = average_slowdown;
			*flow_with_max_average_slowdown = data->stream_id;
		}
		if (average_slowdown < slowdown_min)
			slowdown_min = average_slowdown;
		
		// NVMEV_INFO("sqid: %d, slowdown = %d\n", data->stream_id, (unsigned int)average_slowdown*1000);
	};
	
	if (stream_count == 1)
	{
		*flow_with_max_average_slowdown = -1;
	}
	return (double)(slowdown_min / slowdown_max);
}

bool fine_ture_unfair_transactions_fairzns(
	struct list_head* tmp_queue,
	unsigned int flow_with_max_average_slowdown
){
	struct nvme_tsu_tr_list_entry *entry, *last_entry = NULL;
    struct list_head *next_pos;

    list_for_each_entry(entry, tmp_queue, list) {
        if (entry->sqid == flow_with_max_average_slowdown) {
            last_entry = entry;
        }
    }

    if (last_entry) {
        next_pos = last_entry->list.next;

        if (next_pos != tmp_queue) {
			list_del(&last_entry->list);
            list_add(&last_entry->list, next_pos);
			return false;
        } else {
            return true;
        }
    }
	return true;
}

void reorder_for_fairness_fairzns(
		struct nvmev_transaction_queue* chip_queue,
		struct list_head* tmp_queue, 
		unsigned int flow_with_max_average_slowdown,
		unsigned int curr
	){
	struct nvmev_tsu_tr* tr = &chip_queue->queue[curr];
	struct nvme_tsu_tr_list_entry *entry;
	unsigned int flow_with_max_average_slowdown_after_fineture;
	bool is_tail = false;

	traverse_tmp_queue(chip_queue, tmp_queue, true);

	if(tr->sqid == flow_with_max_average_slowdown){
		// 将与curr目的zone相同的请求都移动到队列首部。
		move_forward(chip_queue, tmp_queue, tr->zid);
	}else{
		// 将与flow_with_max_average_slowdown中最后一个请求目的zone相同的请求都转移到首部。
		list_for_each_entry_reverse(entry, tmp_queue, list){
			if(entry->sqid == flow_with_max_average_slowdown){
				move_forward(chip_queue, tmp_queue, entry->zid);
				break;
			}
		}
	}

	// Fine-ture
	update_estimated_alone_waiting_time(chip_queue, tmp_queue);
	fairness_based_on_local_slowdown(chip_queue, tmp_queue, curr, &flow_with_max_average_slowdown_after_fineture);
	while(!is_tail && flow_with_max_average_slowdown_after_fineture != flow_with_max_average_slowdown){
		is_tail = fine_ture_unfair_transactions_fairzns(tmp_queue, flow_with_max_average_slowdown);
		update_estimated_alone_waiting_time(chip_queue, tmp_queue);
		fairness_based_on_local_slowdown(chip_queue, tmp_queue, curr, &flow_with_max_average_slowdown_after_fineture);
	}
	

	traverse_tmp_queue(chip_queue, tmp_queue, false);
}


void fairzns(
	struct nvmev_transaction_queue* chip_queue,
	struct nvmev_process_queue* process_queue
) {
	volatile unsigned int curr = chip_queue->io_seq;
	struct nvmev_tsu_tr* tr;
	int channel, chip, die, tr_entry;
	unsigned int flow_with_max_average_slowdown = 0;
	double fairness = 0.0;
	unsigned int fairness_show = 0;
	unsigned int idx = 0;
	struct nvme_tsu_tr_list_entry *entry, *tmp;
	LIST_HEAD(tmp_queue);

	if(curr == -1) return;

	while (curr != -1){
		tr = &chip_queue->queue[curr];
		if(tr->is_completed) {
			curr = tr->next;
			continue;
		}

		channel = tr->ppa->g.ch;
		chip = tr->ppa->g.chip;
		die = tr->ppa->g.lun;

		// 1. Estimate alone_waiting_time for each TR.
		estimate_alone_waiting_time(chip_queue, curr, &tmp_queue);

		// 1. 插入到tmp-queue
		struct nvme_tsu_tr_list_entry *tr_tmp = kzalloc(sizeof(struct nvme_tsu_tr_list_entry), GFP_KERNEL);
		*tr_tmp = (struct nvme_tsu_tr_list_entry){
			.channel = channel,
			.chip = chip,
			.die = die,
			.entry = curr,
			.sqid = tr->sqid,
			.estimated_alone_waiting_time = tr->estimated_alone_waiting_time,
			.list = LIST_HEAD_INIT(tr_tmp->list),
		};
		list_add_tail(&tr_tmp->list, &tmp_queue);

		// 3. Compute fairness and reorder temporary queue for max fairness.
		fairness = fairness_based_on_local_slowdown(chip_queue, &tmp_queue, curr, &flow_with_max_average_slowdown);
		if(fairness <= 0.8) {
			// Adjust the order of TRs in the queue to maximize fairness.
			fairness_show = fairness * 1000;
			NVMEV_INFO("before reorder, fairness = %d  flow_with_max_average_slowdown = %d\n", fairness_show, flow_with_max_average_slowdown);
			reorder_for_fairness_fairzns(chip_queue, &tmp_queue, flow_with_max_average_slowdown, curr);

			fairness = fairness_based_on_local_slowdown(chip_queue, &tmp_queue, curr, &flow_with_max_average_slowdown);
			fairness_show = fairness * 1000;
			NVMEV_INFO("after reorder, fairness = %d  flow_with_max_average_slowdown = %d\n", fairness_show, flow_with_max_average_slowdown);
		}

		curr = tr->next;
	}

	// 4. Insert temporary queue to process queue.
	list_for_each_entry_safe(entry, tmp, &tmp_queue, list){
		channel = entry->channel;
		chip = entry->chip;
		die = entry->die;
		tr_entry = entry->entry;
		tr = &chip_queue->queue[tr_entry];

		if(is_lun_avail(tr)){
			__enqueue_transaction_to_process_queue(process_queue, tr_entry, channel, chip, die);
		}else{
			break;
		}
	}

	// 5. CLear tmp-queue.
	list_for_each_entry_safe(entry, tmp, &tmp_queue, list) {
        list_del(&entry->list);
        kfree(entry);
    }
}

void schedule_fairzns(struct nvmev_tsu* tsu){
    unsigned int i,j;
	double fairness = 0.0;
	unsigned int flow_with_max_average_slowdown = 0;
	uint64_t delta = 0;
	struct nvmev_transaction_queue* chip_queue;
	struct nvmev_process_queue* process_queue = tsu->process_queue;
	struct nvmev_tsu_tr* tr;
	struct transaction_entry* tr_process;
	struct nvmev_ns *ns;
	unsigned int curr, nr_processed;
	unsigned int nr_used_dies;
	unsigned int ch, chip;

    for(i=0;i<tsu->nchs;i++){
        for(j=0;j<tsu->nchips;j++){
            chip_queue = &tsu->chip_queue[i][j];
			curr = chip_queue->io_seq;
			nr_processed = 0;
			if(curr == -1) continue;

			nr_used_dies = get_nr_used_dies(chip_queue);


			fairzns(chip_queue, process_queue);

			curr = process_queue->io_seq;
            while(curr != -1){
				tr_process = &process_queue->queue[curr];
                tr = &chip_queue->queue[tr_process->entry];
                ns = &nvmev_vdev->ns[tr->nsid];

                if(tr_process->is_completed){
                    curr = tr_process->next;
                    continue;
                }

                if (!ns->proc_tr_cmd(ns, tr)){
					NVMEV_DEBUG("tr: %lld sqid: %lld sq_entry: %lld failed\n", curr, tr->sqid, tr->sqid);
                    return;
                }
                
                mb();
				spin_lock(&chip_queue->tr_lock);
                tr->is_completed = true;
				spin_unlock(&chip_queue->tr_lock);
				tr_process->is_completed = true;

				nr_processed++;
                curr = tr_process->next;
            }
			
			__reclaim_transaction_in_process_queue(process_queue);
			if(nr_used_dies > 1){
				chip_queue->nr_exist_conflict_trs += nr_processed;
				chip_queue->nr_processed_trs += nr_processed;
				NVMEV_INFO("(channel: %d chip: %d) nr_exist_conflict_trs= %lld, nr_processed_trs= %lld \n", i, j, chip_queue->nr_exist_conflict_trs, chip_queue->nr_processed_trs);
			}else{
				chip_queue->nr_processed_trs += nr_processed;
			}
        }
    }
}
