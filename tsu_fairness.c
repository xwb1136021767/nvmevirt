#include "tsu_fairness.h"

double compute_global_slowdown(int stream_id, double local_slowdown, unsigned int transaction_count){
	struct nvmev_tsu_flow_info *node, *flow_entry = NULL;
	double global_slowdown, sum_slowdown;
	unsigned int num1, num2;
	// 1. Find flow_info.
	list_for_each_entry(node, &global_flow_infos, list){
		if(node->sqid == stream_id){
			flow_entry = node;
			break;
		}
	}

	if(flow_entry){
		flow_entry->local_slowdown = local_slowdown;
		flow_entry->local_transactions = transaction_count;
		num1 = (unsigned int)(local_slowdown * transaction_count * 1000);
		num2 = (unsigned int)(flow_entry->sum_slowdown * 1000);
		sum_slowdown = (double)((num1 + num2)/1000);
		transaction_count += flow_entry->nr_transactions; 
		global_slowdown =  sum_slowdown / transaction_count;
	}else{
		sum_slowdown = local_slowdown * transaction_count;
		global_slowdown = sum_slowdown / transaction_count;

		flow_entry = kmalloc(sizeof(struct nvmev_tsu_flow_info), GFP_KERNEL);
		*flow_entry = (struct nvmev_tsu_flow_info){
			.sqid = stream_id,
			.global_slowdown = 0.0,
			.nr_transactions = 0,
			.sum_slowdown = 0.0,
			.local_slowdown = local_slowdown,
			.local_transactions = transaction_count,
		};
		list_add(&flow_entry->list, &global_flow_infos);
	}
	return global_slowdown;
}

double fairness_based_on_average_slowdown(
	struct nvmev_transaction_queue* chip_queue, 
	struct list_head* tmp_queue,
	unsigned int curr,
	unsigned int* flow_with_max_average_slowdown,
	double* max_slowdown
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
	double slow_down, average_slowdown;

	spp = &ssd->sp;
	*flow_with_max_average_slowdown = tr->sqid;

	print_tmp_queue(tmp_queue);

	list_for_each_entry_safe(entry, tmp, tmp_queue, list){
		tr_entry = entry->entry;
		tr = &chip_queue->queue[tr_entry];
		tr_execute_time = entry->transfer_time + entry->command_time;
		// tr_execute_time = estimate_transaction_transfer_time(tr, spp, ssd) + estimate_transaction_command_time(tr, spp, ssd);
		total_finish_time += tr_execute_time;

		transaction_alone_time = entry->estimated_alone_waiting_time + tr_execute_time;
		// transaction_shared_time = total_finish_time + (__get_clock() - tr->stime);
		transaction_shared_time = total_finish_time;
		slow_down = (double)transaction_shared_time / (double)transaction_alone_time;

		data = FIND_ELEMENT(&sum_slowdown, tr->sqid);
		if(data){
			data->slow_down += slow_down;
			data->transaction_count++;
		}else{
			data = kmalloc(sizeof(struct stream_data), GFP_KERNEL);
			*data = (struct stream_data){
				.stream_id = tr->sqid,
				.slow_down = slow_down,
				.transaction_count = 1,
			};
			list_add(&data->list, &sum_slowdown);
		}

		NVMEV_INFO("tr_execute_time = %lld\n", tr_execute_time);
		NVMEV_INFO("transaction_shared_time = %lld\n", transaction_shared_time);
		NVMEV_INFO("transaction_alone_time = %lld\n", transaction_alone_time);
		NVMEV_INFO("slow_down = %d\n", (unsigned int)(slow_down * 1000));
		NVMEV_INFO("data->slow_down = %d\n", (unsigned int)(data->slow_down*1000));
		NVMEV_INFO("data->transaction_count = %d\n", data->transaction_count);
	}

	// Find the stream with max slowdown 
	list_for_each_entry(data, &sum_slowdown, list){
		stream_count++;
		// 1. 计算local slowdown.
		average_slowdown = data->slow_down / data->transaction_count;
		NVMEV_INFO("local slowdown = %d\n", (unsigned int)(average_slowdown*1000));
		// 2. 计算global slowdown.
		average_slowdown = compute_global_slowdown(data->stream_id, average_slowdown, data->transaction_count);
		NVMEV_INFO("global slowdown = %d\n", (unsigned int)(average_slowdown*1000));

		// 3. 使用global slowdown参与后续计算。
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

	*max_slowdown = slowdown_max;
	return (double)(slowdown_min / slowdown_max);
}


bool fine_ture_unfair_transactions(
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

void reorder_for_fairness(
		struct nvmev_transaction_queue* chip_queue,
		struct list_head* tmp_queue, 
		unsigned int flow_with_max_average_slowdown,
		unsigned int curr
	){
	struct nvmev_tsu_tr* tr = &chip_queue->queue[curr];
	struct nvme_tsu_tr_list_entry *entry;
	unsigned int flow_with_max_average_slowdown_after_fineture;
	bool stop = false;
	double max_slowdown = 0.0;

	// traverse_tmp_queue(chip_queue, tmp_queue, true);
	if(tr->sqid == flow_with_max_average_slowdown){
		// 将与curr目的zone相同的请求都移动到队列首部。
		move_forward(chip_queue, tmp_queue, tr->zid);
	}else{
		// 将与flow_with_max_average_slowdown中最后一个请求目的zone相同的请求都转移到首部。
		list_for_each_entry_reverse(entry, tmp_queue, list){
			tr = &chip_queue->queue[entry->entry];
			if(tr->sqid == flow_with_max_average_slowdown){
				move_forward(chip_queue, tmp_queue, tr->zid);
				break;
			}
		}
	}


	update_estimated_alone_waiting_time(chip_queue, tmp_queue);
	// fine-ture last transaction's position.
	fairness_based_on_average_slowdown(chip_queue, tmp_queue, curr, &flow_with_max_average_slowdown_after_fineture, &max_slowdown);
	while(!stop && flow_with_max_average_slowdown_after_fineture != flow_with_max_average_slowdown){
		stop = fine_ture_unfair_transactions(tmp_queue, flow_with_max_average_slowdown);
		update_estimated_alone_waiting_time(chip_queue, tmp_queue);
		fairness_based_on_average_slowdown(chip_queue, tmp_queue, curr, &flow_with_max_average_slowdown_after_fineture, &max_slowdown);
	}
	

	// traverse_tmp_queue(chip_queue, tmp_queue, false);
}


void update_global_flow_info(void){
	struct nvmev_tsu_flow_info *flow_entry = NULL;
	list_for_each_entry(flow_entry, &global_flow_infos, list){
		unsigned int num1 = (unsigned int)(flow_entry->local_slowdown * flow_entry->local_transactions * 1000);
		unsigned int num2 = (unsigned int)(flow_entry->sum_slowdown * 1000);
		
		// flow_entry->sum_slowdown += ( flow_entry->local_slowdown * flow_entry->local_transactions);
		flow_entry->sum_slowdown = (double)((num1+num2)/1000);
		flow_entry->nr_transactions += flow_entry->local_transactions;
		flow_entry->global_slowdown = (flow_entry->sum_slowdown)/(flow_entry->nr_transactions);
		flow_entry->local_slowdown = 0.0;
		flow_entry->local_transactions = 0;

		// NVMEV_INFO("========update_global_flow_info==============\n");
		// NVMEV_INFO("sqid = %d\n", flow_entry->sqid);
		// NVMEV_INFO("sum_slowdown = %d\n", (unsigned int)(flow_entry->sum_slowdown * 1000));
		// NVMEV_INFO("nr_transactions = %lld\n", flow_entry->nr_transactions);
		// NVMEV_INFO("global_slowdown = %lld\n", (unsigned int)(flow_entry->global_slowdown * 1000));
		// NVMEV_INFO("========update_global_flow_info==============\n");
	}
}
/**
 * Aggregate transactions that have no zone conflict and can be executed in parallel to form a package.
*/
void get_transaction_packages_without_zone_conflict(
	struct nvmev_transaction_queue* chip_queue
) {
	volatile unsigned int curr = chip_queue->io_seq;
	struct nvmev_tsu_tr* tr;
	int channel, chip, die, tr_entry;
	unsigned int flow_with_max_average_slowdown = 0;
	double fairness = 0.0, max_slowdown=0.0;
	unsigned int fairness_show = 0;
	unsigned int idx = 0, nr_transactions = 0;
	struct nvme_tsu_tr_list_entry *tr_tmp;
	struct nvme_tsu_tr_list_entry *entry, *tmp;
	LIST_HEAD(tmp_queue);

	if(curr == -1) return;

	while (curr != -1){
		tr = &chip_queue->queue[curr];
		if(tr->is_completed || tr->is_copyed) {
			curr = tr->next;
			continue;
		}

		channel = tr->ppa->g.ch;
		chip = tr->ppa->g.chip;
		die = tr->ppa->g.lun;
		nr_transactions++;

		// 1. Estimate alone_waiting_time for each TR.
		estimate_alone_waiting_time(chip_queue, curr, &tmp_queue);

		// 1. 插入到tmp-queue
		tr_tmp = kzalloc(sizeof(struct nvme_tsu_tr_list_entry), GFP_KERNEL);
		*tr_tmp = (struct nvme_tsu_tr_list_entry){
			.channel = channel,
			.chip = chip,
			.die = die,
			.entry = curr,
			.sqid = tr->sqid,
			.is_copyed = false,
			.estimated_alone_waiting_time = tr->estimated_alone_waiting_time,
			.transfer_time = tr->transfer_time,
			.command_time = tr->command_time,
			.list = LIST_HEAD_INIT(tr_tmp->list),
		};
		list_add_tail(&tr_tmp->list, &tmp_queue);
		
		// 3. Compute fairness and reorder temporary queue for max fairness.
		fairness = fairness_based_on_average_slowdown(chip_queue, &tmp_queue, curr, &flow_with_max_average_slowdown, &max_slowdown);
		if(fairness <= F_THR) {
			chip_queue->queue_probe.num_of_scheduling++;
			chip_queue->queue_probe.total_before_reorder_fairness += fairness;
			chip_queue->queue_probe.before_reorder_max_slowdown = max_slowdown;
			chip_queue->queue_probe.avg_before_reorder_fairness = (double)((unsigned int)chip_queue->queue_probe.total_before_reorder_fairness*1000 / chip_queue->queue_probe.num_of_scheduling);
			
			// Adjust the order of TRs in the queue to maximize fairness.
			reorder_for_fairness(chip_queue, &tmp_queue, flow_with_max_average_slowdown, curr);

			fairness = fairness_based_on_average_slowdown(chip_queue, &tmp_queue, curr, &flow_with_max_average_slowdown, &max_slowdown);
			chip_queue->queue_probe.after_reorder_max_slowdown = max_slowdown;
			chip_queue->queue_probe.total_after_reorder_fairness += fairness;
			chip_queue->queue_probe.avg_after_reorder_fairness = (double)((unsigned int)chip_queue->queue_probe.total_after_reorder_fairness*1000 / chip_queue->queue_probe.num_of_scheduling);
		}

		// insert_to_die_queue(&chip_queue->die_queues[die], channel, chip, die, curr);
		tr->is_copyed = true;
		curr = tr->next;
	}

	update_global_flow_info();

	// 4. Insert temporary queue to die queue.
	list_for_each_entry_safe(entry, tmp, &tmp_queue, list){
		channel = entry->channel;
		chip = entry->chip;
		die = entry->die;
		tr_entry = entry->entry;
		insert_to_die_queue(&chip_queue->die_queues[die], NULL, channel, chip, die, tr_entry, 0, 0);
	}

	// 5. CLear tmp-queue.
	clear_nvme_tsu_tr_list_entry(&tmp_queue);

	if(nr_transactions > chip_queue->queue_probe.max_queue_length)
		chip_queue->queue_probe.max_queue_length = nr_transactions;
}

void reorder_for_zone_conflict(struct nvmev_transaction_queue* chip_queue, struct nvmev_process_queue* process_queue) {
	unsigned int curr = chip_queue->io_seq;
	unsigned int curr_process = process_queue->free_seq;
	struct nvmev_tsu_tr *tr;
	struct die_queue_entry *die_entry;
	unsigned int nr_used_dies;
	int channel, chip, die, tr_entry;
	if(curr == -1) return;

	for( die = 0; die < chip_queue->nr_luns; die++){
		list_for_each_entry(die_entry, &chip_queue->die_queues[die].transactions_list, list){
			channel = die_entry->channel;
			chip = die_entry->chip;
			tr_entry = die_entry->entry;
			tr = &chip_queue->queue[tr_entry];

			if(is_lun_avail(tr)){
				__enqueue_transaction_to_process_queue(process_queue, tr_entry, channel, chip, die);
				die_entry->is_copyed = true;
			}
			break;
		}
		// clear_copyed_entries_in_die_queue(&chip_queue->die_queues[die]);
		// clear_entries_in_die_queue(&chip_queue->die_queues[die]);
	}
}

void dispatch_transactions_to_chipqueue(
	struct nvmev_transaction_queue* chip_queue, 
	struct nvmev_process_queue* process_queue,
	struct list_head* workload_infos
){
	unsigned int curr = chip_queue->io_seq;
	unsigned int curr_process = process_queue->free_seq;
	struct nvmev_tsu_tr *tr;
	struct die_queue_entry *die_entry;
	unsigned int nr_dispatched;
	int channel, chip, die, tr_entry;
	if(curr == -1) return;

	for( die = 0; die < chip_queue->nr_luns; die++){
		nr_dispatched = 0;
		list_for_each_entry(die_entry, &chip_queue->die_queues[die].transactions_list, list){
			channel = die_entry->channel;
			chip = die_entry->chip;
			tr_entry = die_entry->entry;
			tr = &chip_queue->queue[tr_entry];

			if(is_lun_avail(tr)){
				NVMEV_INFO("__enqueue_transaction_to_process_queue, ch: %d chip: %d die: %d sqid: %d entry: %d\n", channel, chip, die, tr->sqid, tr_entry);
				__enqueue_transaction_to_process_queue(process_queue, tr_entry, channel, chip, die);
				die_entry->is_copyed = true;
				nr_dispatched++;
				if(nr_dispatched >= chip_queue->nr_planes){
					break;
				}
			}else{
				break;
			}
		}
		// NVMEV_INFO("clear_copyed_entries_in_die_queue\n");
		clear_copyed_entries_in_die_queue(&chip_queue->die_queues[die], workload_infos);
	}
}

void traverse_diequeue(struct nvmev_die_queue *die_queue){
	struct die_queue_entry *die_entry;
	NVMEV_INFO("===========traverse_diequeue==================\n");
	list_for_each_entry(die_entry, &die_queue->transactions_list, list){
		NVMEV_INFO("ch: %d chip: %d die: %d entry: %d\n", die_entry->channel, die_entry->chip, die_entry->die, die_entry->entry);
	}
	NVMEV_INFO("===========traverse_diequeue==================\n");
}

void zone_conflict_transaction_aggregation_and_compute_slowdown(
	struct nvmev_transaction_queue* chip_queue,
	struct nvmev_process_queue* process_queue,
	struct list_head* workload_infos
){
	volatile unsigned int curr = chip_queue->io_seq;
	struct nvmev_tsu_tr* tr;
	unsigned int channel, chip, die;
	unsigned int flow_with_max_average_slowdown = 0;
	double fairness = 0.0, max_slowdown=0.0;
	unsigned int fairness_show = 0;

	if(curr == -1) return;

	while (curr != -1){
		tr = &chip_queue->queue[curr];
		if(tr->is_completed || tr->is_copyed) {
			curr = tr->next;
			continue;
		}

		// Insert to die-queue.
		channel = tr->ppa->g.ch;
		chip = tr->ppa->g.chip;
		die = tr->ppa->g.lun;
		NVMEV_INFO("insert_to_die_queue, channel: %d chip: %d die: %d tr->sqid = %d entry: %d\n", channel, chip, die, tr->sqid, curr);
		insert_to_die_queue(&chip_queue->die_queues[die], workload_infos, channel, chip, die, curr, tr->sqid, tr->zid);
		tr->is_copyed = true;
		curr = tr->next;
		traverse_diequeue(&chip_queue->die_queues[die]);
	}

	// compute slowdown and fairness.

	// reorder transactions for better fairness.
}

void schedule_fairness(struct nvmev_tsu* tsu){
    unsigned int i,j;
	double fairness = 0.0;
	unsigned int flow_with_max_average_slowdown = 0;
	uint64_t delta = 0, transaction_waiting_time = 0;
	struct nvmev_transaction_queue* chip_queue;
	struct nvmev_process_queue* process_queue = tsu->process_queue;
	struct nvmev_tsu_tr* tr;
	struct transaction_entry* tr_process;
	struct nvmev_ns *ns;
	unsigned int curr, nr_processed;
	unsigned int nr_used_dies;
	unsigned int ch, chip;

	/**
	 * 1. 将所有chip-queue中的事务添加到die_queues中，并且以分区为单位进行聚合。
	 * 2. 对每个die_queues中的事务计算local slowdown，结合这些结果计算global slowdown。
	*/
	for(i=0;i<tsu->nchs;i++){
        for(j=0;j<tsu->nchips;j++){
			chip_queue = &tsu->chip_queue[i][j];
			// NVMEV_INFO("zone_conflict_transaction_aggregation_and_compute_slowdown\n");
			zone_conflict_transaction_aggregation_and_compute_slowdown(chip_queue, process_queue, &tsu->workload_infos);
		}
	}

    for(i=0;i<tsu->nchs;i++){
        for(j=0;j<tsu->nchips;j++){
            chip_queue = &tsu->chip_queue[i][j];
			curr = chip_queue->io_seq;
			nr_processed = 0;
			if(curr == -1) continue;

			nr_used_dies = get_nr_used_dies(chip_queue);
			// dispatch transactions to chip queue.
			// NVMEV_INFO("dispatch_transactions_to_chipqueue\n");
			dispatch_transactions_to_chipqueue(chip_queue, process_queue, &tsu->workload_infos);
			// 1. global fairness scheduling
			// get_transaction_packages_without_zone_conflict(chip_queue);
			// // 2. zone-conflict-aware 
			// reorder_for_zone_conflict(chip_queue, process_queue);
			curr = process_queue->io_seq;
            while(curr != -1){
				tr_process = &process_queue->queue[curr];
                tr = &chip_queue->queue[tr_process->entry];
                ns = &nvmev_vdev->ns[tr->nsid];

                if(tr_process->is_completed){
                    curr = tr_process->next;
                    continue;
                }

				NVMEV_INFO("process tr : %d\n", tr_process->entry);
                if (!ns->proc_tr_cmd(ns, tr)){
					NVMEV_DEBUG("tr: %lld sqid: %lld sq_entry: %lld failed\n", curr, tr->sqid, tr->sqid);
                    return;
                }
                
				// record chip-queue statistics
				transaction_waiting_time = (__get_clock() > tr->stime)?(__get_clock() - tr->stime):0;
				transaction_waiting_time = convert_clock_to_ms(transaction_waiting_time);
				chip_queue->queue_probe.total_waiting_times += transaction_waiting_time;
				chip_queue->queue_probe.nr_processed_trs++;
				chip_queue->queue_probe.avg_waiting_times = chip_queue->queue_probe.total_waiting_times / chip_queue->queue_probe.nr_processed_trs;
				if(transaction_waiting_time > chip_queue->queue_probe.max_waiting_times)
				 	chip_queue->queue_probe.max_waiting_times = transaction_waiting_time;

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
				// NVMEV_INFO("(channel: %d chip: %d) nr_exist_conflict_trs= %lld, nr_processed_trs= %lld \n", i, j, chip_queue->nr_exist_conflict_trs, chip_queue->nr_processed_trs);
			}else{
				chip_queue->nr_processed_trs += nr_processed;
			}
        }
    }
}
