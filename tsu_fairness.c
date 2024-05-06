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

		// NVMEV_INFO("tr_execute_time = %lld\n", tr_execute_time);
		// NVMEV_INFO("transaction_shared_time = %lld\n", transaction_shared_time);
		// NVMEV_INFO("transaction_alone_time = %lld\n", transaction_alone_time);
		// NVMEV_INFO("slow_down = %d\n", (unsigned int)(slow_down * 1000));
		// NVMEV_INFO("data->slow_down = %d\n", (unsigned int)(data->slow_down*1000));
		// NVMEV_INFO("data->transaction_count = %d\n", data->transaction_count);
	}

	// Find the stream with max slowdown 
	list_for_each_entry(data, &sum_slowdown, list){
		stream_count++;
		// 1. 计算local slowdown.
		average_slowdown = data->slow_down / data->transaction_count;
		// NVMEV_INFO("local slowdown = %d\n", (unsigned int)(average_slowdown*1000));
		// 2. 计算global slowdown.
		average_slowdown = compute_global_slowdown(data->stream_id, average_slowdown, data->transaction_count);
		// NVMEV_INFO("global slowdown = %d\n", (unsigned int)(average_slowdown*1000));

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
// void get_transaction_packages_without_zone_conflict(
// 	struct nvmev_transaction_queue* chip_queue
// ) {
// 	volatile unsigned int curr = chip_queue->io_seq;
// 	struct nvmev_tsu_tr* tr;
// 	int channel, chip, die, tr_entry;
// 	unsigned int flow_with_max_average_slowdown = 0;
// 	double fairness = 0.0, max_slowdown=0.0;
// 	unsigned int fairness_show = 0;
// 	unsigned int idx = 0, nr_transactions = 0;
// 	struct nvme_tsu_tr_list_entry *tr_tmp;
// 	struct nvme_tsu_tr_list_entry *entry, *tmp;
// 	LIST_HEAD(tmp_queue);

// 	if(curr == -1) return;

// 	while (curr != -1){
// 		tr = &chip_queue->queue[curr];
// 		if(tr->is_completed || tr->is_copyed) {
// 			curr = tr->next;
// 			continue;
// 		}

// 		channel = tr->ppa->g.ch;
// 		chip = tr->ppa->g.chip;
// 		die = tr->ppa->g.lun;
// 		nr_transactions++;

// 		// 1. Estimate alone_waiting_time for each TR.
// 		estimate_alone_waiting_time(chip_queue, curr, &tmp_queue);

// 		// 1. 插入到tmp-queue
// 		tr_tmp = kzalloc(sizeof(struct nvme_tsu_tr_list_entry), GFP_KERNEL);
// 		*tr_tmp = (struct nvme_tsu_tr_list_entry){
// 			.channel = channel,
// 			.chip = chip,
// 			.die = die,
// 			.entry = curr,
// 			.sqid = tr->sqid,
// 			.is_copyed = false,
// 			.estimated_alone_waiting_time = tr->estimated_alone_waiting_time,
// 			.transfer_time = tr->transfer_time,
// 			.command_time = tr->command_time,
// 			.list = LIST_HEAD_INIT(tr_tmp->list),
// 		};
// 		list_add_tail(&tr_tmp->list, &tmp_queue);
		
// 		// 3. Compute fairness and reorder temporary queue for max fairness.
// 		fairness = fairness_based_on_average_slowdown(chip_queue, &tmp_queue, curr, &flow_with_max_average_slowdown, &max_slowdown);
// 		if(fairness <= F_THR) {
// 			chip_queue->queue_probe.num_of_scheduling++;
// 			chip_queue->queue_probe.total_before_reorder_fairness += fairness;
// 			chip_queue->queue_probe.before_reorder_max_slowdown = max_slowdown;
// 			chip_queue->queue_probe.avg_before_reorder_fairness = (double)((unsigned int)chip_queue->queue_probe.total_before_reorder_fairness*1000 / chip_queue->queue_probe.num_of_scheduling);
			
// 			// Adjust the order of TRs in the queue to maximize fairness.
// 			reorder_for_fairness(chip_queue, &tmp_queue, flow_with_max_average_slowdown, curr);

// 			fairness = fairness_based_on_average_slowdown(chip_queue, &tmp_queue, curr, &flow_with_max_average_slowdown, &max_slowdown);
// 			chip_queue->queue_probe.after_reorder_max_slowdown = max_slowdown;
// 			chip_queue->queue_probe.total_after_reorder_fairness += fairness;
// 			chip_queue->queue_probe.avg_after_reorder_fairness = (double)((unsigned int)chip_queue->queue_probe.total_after_reorder_fairness*1000 / chip_queue->queue_probe.num_of_scheduling);
// 		}

// 		// insert_to_die_queue(&chip_queue->die_queues[die], channel, chip, die, curr);
// 		tr->is_copyed = true;
// 		curr = tr->next;
// 	}

// 	update_global_flow_info();

// 	// 4. Insert temporary queue to die queue.
// 	list_for_each_entry_safe(entry, tmp, &tmp_queue, list){
// 		channel = entry->channel;
// 		chip = entry->chip;
// 		die = entry->die;
// 		tr_entry = entry->entry;
// 		insert_to_die_queue(&chip_queue->die_queues[die], NULL, channel, chip, die, tr_entry, 0, 0);
// 	}

// 	// 5. CLear tmp-queue.
// 	clear_die_entry(&tmp_queue);

// 	if(nr_transactions > chip_queue->queue_probe.max_queue_length)
// 		chip_queue->queue_probe.max_queue_length = nr_transactions;
// }

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

void dispatch_transactions_to_process_queue(
	struct nvmev_transaction_queue* chip_queue, 
	struct nvmev_process_queue* process_queue_read,
	struct nvmev_process_queue* process_queue_write,
	struct list_head* workload_infos
){
	unsigned int curr = chip_queue->io_seq;
	unsigned int curr_process_read = process_queue_read->free_seq;
	unsigned int curr_process_write = process_queue_write->free_seq;
	struct nvmev_tsu_tr *tr;
	struct die_queue_entry *die_entry;
	unsigned int nr_dispatched;
	int channel, chip, die, tr_entry;
	struct nvme_command *cmd;
	if(curr == -1) return;

	for( die = 0; die < chip_queue->nr_luns; die++){
		nr_dispatched = 0;
		list_for_each_entry(die_entry, &chip_queue->die_queues[die].transactions_list, list){
			channel = die_entry->channel;
			chip = die_entry->chip;
			tr_entry = die_entry->entry;
			tr = &chip_queue->queue[tr_entry];

			if(is_lun_avail(tr) && die_entry->is_copyed==false){
				// NVMEV_INFO("__enqueue_transaction_to_process_queue, ch: %d chip: %d die: %d sqid: %d entry: %d type: %d\n", channel, chip, die, tr->sqid, tr_entry, tr->cmd);
				switch (tr->cmd) {
					case NAND_WRITE:
						// NVMEV_INFO("__enqueue_transaction_to_process_queue, process_queue_write, ch: %d chip: %d die: %d sqid: %d entry: %d type: %d\n", channel, chip, die, tr->sqid, tr_entry, tr->cmd);
						__enqueue_transaction_to_process_queue(process_queue_write, tr_entry, channel, chip, die);
						break;
					case NAND_READ:
						// NVMEV_INFO("__enqueue_transaction_to_process_queue, process_queue_read, ch: %d chip: %d die: %d sqid: %d entry: %d type: %d\n", channel, chip, die, tr->sqid, tr_entry, tr->cmd);
						__enqueue_transaction_to_process_queue(process_queue_read, tr_entry, channel, chip, die);
						break;
				}
				
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
	NVMEV_INFO("nr_transactions: %d\n", die_queue->nr_transactions);
	list_for_each_entry(die_entry, &die_queue->transactions_list, list){
		NVMEV_INFO("addr: %p ch: %d chip: %d die: %d sqid: %d entry: %d zid: %d\n", &die_entry->list , die_entry->channel, die_entry->chip, die_entry->die, die_entry->sqid, die_entry->entry, die_entry->zid);
	}
	NVMEV_INFO("===========traverse_diequeue==================\n");
}

// 更新local slowdown，并判断flow_with_max_average_slowdown是否需要继续微调
bool update_local_slowdown(
	struct nvmev_transaction_queue* chip_queue, 
	struct list_head* tmp_queue,
	unsigned int flow_with_max_average_slowdown,
	struct list_head* workload_infos
)
{
	LIST_HEAD(sum_slowdown);
	uint64_t total_finish_time = 0;
	double slowdown_max = DBL_MIN, slowdown_min = DBL_MAX;
	double average_slowdown=0.0;
	double local_fairness = 0.0;
	struct stream_data* data;
	uint64_t tr_execute_time;
	struct nvmev_tsu_tr* tr;
	struct die_queue_entry *entry, *tmp;
	unsigned int tr_entry, sqid;
	uint64_t transaction_alone_time, transaction_shared_time;
	struct nvmev_workload_global_info* global_info;
    struct nvmev_workload_local_info* local_info;
	unsigned int channel, chip, die;

	list_for_each_entry_safe(entry, tmp, tmp_queue, list){
		tr_entry = entry->entry;
		tr = &chip_queue->queue[tr_entry];
		tr_execute_time =  entry->transfer_time + entry->command_time;
		total_finish_time += tr_execute_time;
		channel = tr->ppa->g.ch;
		chip = tr->ppa->g.chip;
		die = tr->ppa->g.lun;
		global_info = find_global_workload_info(workload_infos, tr->sqid);
    	local_info = find_local_workload_info(global_info, channel, chip, die);


		transaction_alone_time = entry->estimated_alone_waiting_time + tr_execute_time;
		transaction_shared_time = (total_finish_time + (__get_clock() - tr->stime));
		// transaction_shared_time = (local_info->completed_transactions_time + total_finish_time + (__get_clock() - tr->stime)) / local_info->nr_enqueued_transactions;
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
				.channel = tr->ppa->g.ch,
				.chip = tr->ppa->g.chip,
				.die = tr->ppa->g.lun,
			};
			list_add(&data->list, &sum_slowdown);
		}
	}

	list_for_each_entry(data, &sum_slowdown, list){
		sqid = data->stream_id;
		average_slowdown = data->slow_down / data->transaction_count;
		global_info = find_global_workload_info(workload_infos, sqid);
    	local_info = find_local_workload_info(global_info, data->channel, data->chip, data->die);
		local_info->local_slowdown = average_slowdown;

		if (average_slowdown > slowdown_max)
			slowdown_max = average_slowdown;
		if (average_slowdown < slowdown_min)
			slowdown_min = average_slowdown;
		
		if(sqid == flow_with_max_average_slowdown && average_slowdown >= slowdown_min)
			return true;
	};

	if(slowdown_max > chip_queue->queue_probe.max_slowdown)
		chip_queue->queue_probe.max_slowdown = slowdown_max;
	if(slowdown_min < chip_queue->queue_probe.min_slowdown)
		chip_queue->queue_probe.min_slowdown = slowdown_min;
	if(slowdown_max != DBL_MIN && slowdown_min != DBL_MAX)
		local_fairness = slowdown_min / slowdown_max;
	
	if(local_fairness < chip_queue->queue_probe.min_fairness && local_fairness != 0.0)
		chip_queue->queue_probe.min_fairness = local_fairness;
	if(local_fairness > chip_queue->queue_probe.max_fairness)
		chip_queue->queue_probe.max_fairness = local_fairness;

	chip_queue->queue_probe.sum_fairness += local_fairness; 
	chip_queue->queue_probe.num_of_scheduling++;
	chip_queue->queue_probe.avg_fairness = ((unsigned int)chip_queue->queue_probe.sum_fairness*1000 / chip_queue->queue_probe.num_of_scheduling);
	return false;
}

void update_global_slowdown(
	struct list_head* workload_infos,
	unsigned int* flow_with_max_average_slowdown
){
	struct nvmev_workload_global_info* global_info, *next;
    struct nvmev_workload_local_info* local_info, *next_local;
	unsigned int channel, chip, die;
	unsigned int nr_dies = 0, nr_workloads = 0;
	double global_slowdown = 0.0;
	double slowdown_max = DBL_MIN, slowdown_min = DBL_MAX;

	list_for_each_entry_safe(global_info, next, workload_infos, list) {
		nr_workloads++;
		nr_dies = 0;
		global_slowdown = 0.0;
        list_for_each_entry_safe(local_info, next_local, &global_info->local_infos, list){
			if(local_info->nr_transactions_in_die_queue == 0) continue;

            nr_dies++;
			global_slowdown += local_info->local_slowdown;
        }
		global_slowdown = global_slowdown / nr_dies;
		global_info->global_slowdown = global_slowdown;

		if (global_slowdown > slowdown_max)
		{
			slowdown_max = global_slowdown;
			*flow_with_max_average_slowdown = global_info->sqid;
		}
		if (global_slowdown < slowdown_min)
			slowdown_min = global_slowdown;
		
		// NVMEV_INFO("sqid: %d global slowdown: %d\n", global_info->sqid, (unsigned int)(global_info->global_slowdown * 1000));
		// NVMEV_INFO("slowdown_max: %d\n", (unsigned int)(slowdown_max * 1000));
		// NVMEV_INFO("slowdown_min: %d\n", (unsigned int)(slowdown_min * 1000));
		// NVMEV_INFO("flow_with_max_average_slowdown: %d\n", *flow_with_max_average_slowdown);
    }
	
	if(nr_workloads <= 1){
		*flow_with_max_average_slowdown = -1;
	}
}

void compute_slowdown(
	struct nvmev_transaction_queue* chip_queue,
	struct nvmev_die_queue *die_queue, 
	struct list_head* workload_infos
){
	double fairness = 0.0;
	unsigned int fairness_show = 0;
	unsigned int flow_with_max_average_slowdown = 0;
	// 计算alone waiting time。
	update_estimated_alone_waiting_time(chip_queue, &die_queue->transactions_list);

	// 计算local slowdown
	update_local_slowdown(chip_queue, &die_queue->transactions_list, -1, workload_infos);
}

void zone_conflict_transaction_aggregation_and_compute_slowdown(
	struct nvmev_transaction_queue* chip_queue,
	struct list_head* workload_infos
){
	volatile unsigned int curr = chip_queue->io_seq;
	struct nvmev_tsu_tr* tr;
	unsigned int channel, chip, die;
	unsigned int flow_with_max_average_slowdown = 0;
	double fairness = 0.0, max_slowdown=0.0;
	unsigned int fairness_show = 0;
	unsigned int nr_transactions = 0;

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

		// NVMEV_INFO("insert_to_die_queue, channel: %d chip: %d die: %d tr->sqid = %d entry: %d\n", channel, chip, die, tr->sqid, curr);
		compute_transfer_and_command_time(chip_queue, tr);
		insert_to_die_queue(&chip_queue->die_queues[die], workload_infos, tr, curr);
		tr->is_copyed = true;
		curr = tr->next;
		nr_transactions++;
		// traverse_diequeue(&chip_queue->die_queues[die]);
	}

	if(nr_transactions > chip_queue->queue_probe.max_queue_length)
		chip_queue->queue_probe.max_queue_length = nr_transactions;
	// compute slowdown and fairness.
	// compute_local_slowdown();
	for( die = 0; die < chip_queue->nr_luns; die++ ){
		compute_slowdown(chip_queue, &chip_queue->die_queues[die], workload_infos);
	}
}

void swap_segments(struct list_head *head1, struct list_head *tail1,
                   struct list_head *head2, struct list_head *tail2) {
    // 交换头尾指针
    struct list_head *temp = head1->prev;
    head1->prev = head2->prev;
    head2->prev = temp;

    temp = tail1->next;
    tail1->next = tail2->next;
    tail2->next = temp;

    // 更新指针
    head1->prev->next = head1;
    tail1->next->prev = tail1;
    head2->prev->next = head2;
    tail2->next->prev = tail2;
}

void move_elements_to_head(struct list_head *head, struct list_head *tail,
                           struct list_head *queue_head) {
	if(queue_head->next == head){
		// NVMEV_INFO( "head == queue_head, don't need move\n");
		return;
	}

    // 如果链表为空，不执行任何操作
    if (list_empty(head)) {
        return;
    }

	// 创建一个临时链表来存放被移动的元素
    struct list_head temp_list;
    INIT_LIST_HEAD(&temp_list);

	// NVMEV_INFO("head = %p\n", head);
	// NVMEV_INFO("tail = %p\n", tail);
	// NVMEV_INFO("queue_head = %p\n", queue_head);
	// NVMEV_INFO("queue_head->next = %p\n", queue_head->next);
    // 从链表中删除指定的元素段，并添加到临时链表中
    list_cut_position(&temp_list, head->prev, tail);

    // 将临时链表中的元素按顺序添加到链表的首部
	list_splice(&temp_list, queue_head);
}



void move_transactions_forward(
	struct nvmev_die_queue *die_queue, 
	unsigned int flow_with_max_average_slowdown,
	unsigned int channel,
	unsigned int chip,
	unsigned int die,
	struct list_head* workload_infos
){
	struct nvmev_workload_global_info* global_info, *next;
    struct nvmev_workload_local_info* local_info, *next_local;
    struct nvmev_zone_info* zone_info, *next_zone, *new_zone_info;

	if(list_empty(&die_queue->transactions_list) || die_queue->nr_workloads <= 1) return;

	list_for_each_entry_safe(global_info, next, workload_infos, list) {
		if(global_info->sqid != flow_with_max_average_slowdown) continue;

        list_for_each_entry_safe(local_info, next_local, &global_info->local_infos, list){
			if(local_info->channel != channel || local_info->chip != chip || local_info->die != die) continue;
			// 反向遍历，保持原来的顺序。
            list_for_each_entry_reverse(zone_info, &local_info->zone_infos, list){
				if(zone_info->pos_head == NULL) continue;

				// NVMEV_INFO("move sqid: %d zid: %d to head of die queue\n", flow_with_max_average_slowdown, zone_info->zid);
				// NVMEV_INFO("===========before move_transactions_forward==================\n");
				// traverse_diequeue(die_queue);
				// NVMEV_INFO("===========before move_transactions_forward==================\n");
				
				// 把每个zone都移动到链表首部。
                move_elements_to_head(zone_info->pos_head, zone_info->pos_tail, &die_queue->transactions_list );

				// NVMEV_INFO("===========after move_transactions_forward==================\n");
				// traverse_diequeue(die_queue);
				// NVMEV_INFO("===========after move_transactions_forward==================\n");
            }
        }
    }
}

void move_elements_backward(
	struct nvmev_die_queue *die_queue,
	struct list_head *head, 
	struct list_head *tail, 
	int n
){
	if (!head || !tail) {
        // NVMEV_INFO("Error: Head or tail is NULL\n");
        return;
    }

    // 使用 list_cut_position 将需要移动的元素从链表中剪切出来
    struct list_head temp_list;
	struct list_head *pos = head->prev;
    INIT_LIST_HEAD(&temp_list);
    list_cut_position(&temp_list, head->prev, tail);

    // 找到移动后的位置
    while (n-- && pos->next != &die_queue->transactions_list)
        pos = pos->next;

    // 将剪切出来的元素添加到原来的位置后面的第 n 个位置
    list_splice(&temp_list, pos);
}



void fineture_tail_transactions(
	struct nvmev_transaction_queue* chip_queue,
	struct nvmev_die_queue *die_queue, 
	unsigned int flow_with_max_average_slowdown,
	unsigned int channel,
	unsigned int chip,
	unsigned int die,
	struct list_head* workload_infos
){
	struct nvmev_workload_global_info* global_info;
	struct nvmev_workload_local_info* local_info;
	struct nvmev_zone_info* zone_info, *new_zone_info;
	bool should_stop = false;
	struct list_head* pos;

	if(die_queue->nr_workloads <= 1) return;

	should_stop = update_local_slowdown(chip_queue, &die_queue->transactions_list, flow_with_max_average_slowdown, workload_infos);
	if(should_stop) return;

	global_info = find_global_workload_info(workload_infos, flow_with_max_average_slowdown);
    if(global_info == NULL){
        // NVMEV_INFO("Can't find global_info for fineture_tail_transactions\n");
        return;
    }
    local_info = find_local_workload_info(global_info, channel, chip, die);
    if(local_info == NULL){
        // NVMEV_INFO("Can't find local_info for fineture_tail_transactions\n");
        return;
    }

	if(local_info->nr_zones <= 1 || list_empty(&local_info->zone_infos)) return;

	// 1. 找到最后一个zone
	pos = local_info->zone_infos.prev;
	while(pos != &local_info->zone_infos){
		zone_info = list_entry(pos, struct nvmev_zone_info, list);
		if(zone_info->nr_transactions > 0) break;
		pos = pos->prev;
	}
	if(zone_info->nr_transactions == 0) return;
	
	// NVMEV_INFO("zone_info = %p\n", &zone_info);
	// NVMEV_INFO("zid: %d zone_info->pos_head = %p\n", zone_info->zid, zone_info->pos_head);
    // NVMEV_INFO("zid: %d zone_info->pos_tail = %p\n", zone_info->zid, zone_info->pos_tail);
	// 2. 判断事务数量是否大于plane的数量，大于的话将其分割为两个部分，否则直接移动事务即可。
	if(zone_info->nr_transactions > chip_queue->nr_planes){
		new_zone_info = create_zone_info(local_info, zone_info->zid);
		new_zone_info->pos_head = get_node_before_n(zone_info->pos_tail, chip_queue->nr_planes - 1);
		new_zone_info->pos_tail = zone_info->pos_tail;

		zone_info->pos_tail = new_zone_info->pos_head->prev;
		zone_info = new_zone_info;

		// NVMEV_INFO("new_zone_info = %p\n", &new_zone_info);
		// NVMEV_INFO("zid: %d zone_info->pos_head = %p\n", zone_info->zid, zone_info->pos_head);
    	// NVMEV_INFO("zid: %d zone_info->pos_tail = %p\n", zone_info->zid, zone_info->pos_tail);
		// NVMEV_INFO("zid: %d new_zone_info->pos_head = %p\n", new_zone_info->zid, new_zone_info->pos_head);
    	// NVMEV_INFO("zid: %d new_zone_info->pos_tail = %p\n", new_zone_info->zid, new_zone_info->pos_tail);
	}


	// 3. 将zone的事务以plane的数量为单位向后移动
	// NVMEV_INFO("should_stop : %d \n", should_stop);
	while(!should_stop && zone_info->pos_tail->next != &die_queue->transactions_list){
		// NVMEV_INFO("move_elements_backward\n");
		move_elements_backward(die_queue, zone_info->pos_head, zone_info->pos_tail, chip_queue->nr_planes);
		should_stop = update_local_slowdown(chip_queue, &die_queue->transactions_list, flow_with_max_average_slowdown, workload_infos);
	}

}

void reorder_for_global_fairness(
	struct nvmev_transaction_queue* chip_queue,
	unsigned int flow_with_max_average_slowdown,
	struct list_head* workload_infos
){
	unsigned int die;
	for( die = 0; die < chip_queue->nr_luns; die++ ){
		if(list_empty(&chip_queue->die_queues[die].transactions_list)) continue;
		if(chip_queue->die_queues[die].nr_workloads <= 1) continue;
		
		// 1. 将flow_with_max_average_slowdown的事务都转移到die queue的首部。
		move_transactions_forward(&chip_queue->die_queues[die], flow_with_max_average_slowdown, chip_queue->channel , chip_queue->chip, die, workload_infos);
		// compute_slowdown(chip_queue, &chip_queue->die_queues[die], workload_infos);
		// 2. 微调尾部事务。

		// NVMEV_INFO("fineture sqid: %d\n", flow_with_max_average_slowdown);
		// NVMEV_INFO("===========before fineture_tail_transactions==================\n");
		// traverse_diequeue(&chip_queue->die_queues[die]);
		// NVMEV_INFO("===========before fineture_tail_transactions==================\n");

		fineture_tail_transactions(chip_queue, &chip_queue->die_queues[die], flow_with_max_average_slowdown, chip_queue->channel , chip_queue->chip, die, workload_infos);
		
		// NVMEV_INFO("===========after fineture_tail_transactions==================\n");
		// traverse_diequeue(&chip_queue->die_queues[die]);
		// NVMEV_INFO("===========after fineture_tail_transactions==================\n");
	}	
}

bool RWirp(
	struct nvmev_transaction_queue* chip_queue,
	struct nvmev_process_queue* process_queue_read,
	struct nvmev_process_queue* process_queue_write
){
	unsigned int curr = chip_queue->io_seq;
	unsigned int curr_process_read = process_queue_read->io_seq;
	unsigned int curr_process_write = process_queue_write->io_seq;
	struct nvmev_tsu_tr* tr;
	struct transaction_entry* tr_process;
	uint64_t pw_read = 0.0, pw_write=0.0;
	unsigned int cnt = chip_queue->nr_planes;
	uint64_t transaction_waiting_time = 0, transaction_execute_time = 0;

	while(curr_process_read != -1 && cnt){
		tr_process = &process_queue_read->queue[curr_process_read];
		
		if(tr_process->is_completed){
			curr_process_read = tr_process->next;
			continue;
		}

        tr = &chip_queue->queue[tr_process->entry];
		transaction_waiting_time = (__get_clock() > tr->stime)?(__get_clock() - tr->stime):0;
		transaction_execute_time = tr->transfer_time + tr->command_time;
		// NVMEV_INFO("process_queue_read tr: %lld sqid: %lld sq_entry: %lld type: %d\n", curr, tr->sqid, tr->sq_entry, tr->cmd);
		// NVMEV_INFO("process_queue_read transaction_waiting_time: %lld  transaction_execute_time: %lld\n", transaction_waiting_time, transaction_execute_time);
		if(transaction_execute_time != 0) 
			pw_read += div64_u64(transaction_waiting_time, transaction_execute_time);

		cnt--;
		curr_process_read = tr_process->next;
	}

	// if(process_queue_read->io_seq != -1)
	// 	// NVMEV_INFO("process_queue_read end\n");

	cnt = chip_queue->nr_planes;
	pw_read = pw_read / cnt;

	while(curr_process_write != -1 && cnt){
		tr_process = &process_queue_write->queue[curr_process_write];
		
		if(tr_process->is_completed){
			curr_process_write = tr_process->next;
			continue;
		}

        tr = &chip_queue->queue[tr_process->entry];
		transaction_waiting_time = (__get_clock() > tr->stime)?(__get_clock() - tr->stime):0;
		transaction_execute_time = tr->transfer_time + tr->command_time;
		// NVMEV_INFO("process_queue_write, tr: %lld sqid: %lld sq_entry: %lld type: %d\n", curr, tr->sqid, tr->sq_entry, tr->cmd);
		// NVMEV_INFO("process_queue_write, transaction_waiting_time: %lld  transaction_execute_time: %lld\n", transaction_waiting_time, transaction_execute_time);
		if(transaction_execute_time != 0) 
			pw_write += div64_u64(transaction_waiting_time, transaction_execute_time);

		cnt--;
		curr_process_write = tr_process->next;
	}

	// if(process_queue_write->io_seq != -1)
	// 	NVMEV_INFO("process_queue_write end\n");

	cnt = chip_queue->nr_planes;
	pw_write = pw_write / cnt;
	
	if(pw_read >= pw_write){
		return true;
	}
	else{
		return false;
	}		
}

void execute_transactions_in_process_queue(
	struct nvmev_transaction_queue* chip_queue,
	struct nvmev_process_queue* process_queue,
	unsigned int nr_used_dies,
	struct list_head* workload_infos

){
	unsigned int curr, nr_processed;
	struct nvmev_tsu_tr* tr;
	struct transaction_entry* tr_process;
	struct nvmev_ns *ns;
	struct nvmev_workload_global_info* global_info;
    struct nvmev_workload_local_info* local_info;
	uint64_t delta = 0, transaction_waiting_time = 0;

	curr = process_queue->io_seq;
	while(curr != -1){
		tr_process = &process_queue->queue[curr];
		tr = &chip_queue->queue[tr_process->entry];
		ns = &nvmev_vdev->ns[tr->nsid];

		if(tr_process->is_completed){
			curr = tr_process->next;
			continue;
		}

		// NVMEV_INFO("tr: %lld sqid: %lld sq_entry: %lld type: %d\n", curr, tr->sqid, tr->sqid, tr->cmd);

		if (!ns->proc_tr_cmd(ns, tr)){
			NVMEV_INFO("tr: %lld sqid: %lld sq_entry: %lld failed\n", curr, tr->sqid, tr->sqid);
			return;
		}
		
		// record chip-queue statistics
		global_info = find_global_workload_info(workload_infos, tr->sqid);
		local_info = find_local_workload_info(global_info, tr->ppa->g.ch, tr->ppa->g.chip, tr->ppa->g.lun);

		transaction_waiting_time = (__get_clock() > tr->stime)?(__get_clock() - tr->stime):0;
		local_info->completed_transactions_time += transaction_waiting_time;
		chip_queue->queue_probe.total_waiting_times += convert_clock_to_ms(transaction_waiting_time);
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

void schedule_fairness(struct nvmev_tsu* tsu){
    unsigned int i,j;
	double fairness = 0.0;
	unsigned int flow_with_max_average_slowdown = 0;
	uint64_t delta = 0, transaction_waiting_time = 0;
	struct nvmev_transaction_queue* chip_queue;
	struct nvmev_process_queue* process_queue_read = tsu->process_queue_read;
	struct nvmev_process_queue* process_queue_write = tsu->process_queue_write;
	struct nvmev_process_queue* process_queue_first, *process_queue_second;
	struct nvmev_tsu_tr* tr;
	struct transaction_entry* tr_process;
	struct nvmev_ns *ns;
	unsigned int curr, nr_processed;
	unsigned int nr_used_dies;
	unsigned int ch, chip;
	struct nvmev_workload_global_info* global_info;
    struct nvmev_workload_local_info* local_info;
	bool read_first = false;
	/**
	 * 1. 将所有chip-queue中的事务添加到die_queues中，并且以分区为单位进行聚合。
	 * 2. 对每个die_queues中的事务计算local slowdown，结合这些结果计算global slowdown。
	*/
	for(i=0;i<tsu->nchs;i++){
        for(j=0;j<tsu->nchips;j++){
			chip_queue = &tsu->chip_queue[i][j];
			// NVMEV_INFO("zone_conflict_transaction_aggregation_and_compute_slowdown\n");
			zone_conflict_transaction_aggregation_and_compute_slowdown(chip_queue, &tsu->workload_infos);
		}
	}

	update_global_slowdown(&tsu->workload_infos, &flow_with_max_average_slowdown);
	if(flow_with_max_average_slowdown != -1){
		// 调整工作队列中事务顺序。
		for(i=0;i<tsu->nchs;i++){
			for(j=0;j<tsu->nchips;j++){
				chip_queue = &tsu->chip_queue[i][j];
				reorder_for_global_fairness(chip_queue, flow_with_max_average_slowdown, &tsu->workload_infos);
			}
		}
	}

    for(i=0;i<tsu->nchs;i++){
        for(j=0;j<tsu->nchips;j++){
            chip_queue = &tsu->chip_queue[i][j];
			curr = chip_queue->io_seq;
			nr_processed = 0;
			if(curr == -1) continue;

			nr_used_dies = get_nr_used_dies(chip_queue);
			dispatch_transactions_to_process_queue(chip_queue, process_queue_read, process_queue_write, &tsu->workload_infos);
			read_first = RWirp(chip_queue, process_queue_read, process_queue_write);
			if(read_first){
				process_queue_first = process_queue_read;
				process_queue_second = process_queue_write;
			}
			else{
				process_queue_first = process_queue_write;
				process_queue_second = process_queue_read;
			}

			execute_transactions_in_process_queue(chip_queue, process_queue_first, nr_used_dies, &tsu->workload_infos);
			execute_transactions_in_process_queue(chip_queue, process_queue_second, nr_used_dies, &tsu->workload_infos);
        }
    }
}
