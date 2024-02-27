#include "tsu_fairness.h"

uint64_t estimate_transaction_transfer_time(struct nvmev_tsu_tr* tr, struct ssdparams *spp, struct ssd *ssd)
{
	int c = tr->cmd;
	uint64_t cmd_stime = (tr->stime == 0) ? __get_clock() : tr->stime;
	uint64_t nand_stime, nand_etime;
	uint64_t chnl_stime, chnl_etime;
	uint64_t remaining, xfer_size, completed_time, transfer_time;
	struct nand_lun *lun;
	struct ssd_channel *ch;
	struct ppa *ppa = tr->ppa;
	uint32_t cell;

	lun = get_lun(ssd, ppa);
	ch = get_ch(ssd, ppa);
	cell = get_cell(ssd, ppa);
	remaining = tr->xfer_size;
	transfer_time = 0;

	switch (c) {
	case NAND_READ:
		/* read: perform NAND cmd first */
		nand_stime = max(lun->next_lun_avail_time, cmd_stime);
		if (tr->xfer_size == 4096) {
			nand_etime = nand_stime + spp->pg_4kb_rd_lat[cell];
		} else {
			nand_etime = nand_stime + spp->pg_rd_lat[cell];
		}

		/* read: then data transfer through channel */
		chnl_stime = nand_etime;

		while (remaining) {
			xfer_size = min(remaining, (uint64_t)spp->max_ch_xfer_size);
			chnl_etime = chmodel_request(ch->perf_model, chnl_stime, xfer_size);

			if (tr->interleave_pci_dma) { /* overlap pci transfer with nand ch transfer*/
				completed_time = ssd_advance_pcie(ssd, chnl_etime, xfer_size);
			} else {
				completed_time = chnl_etime;
			}

			transfer_time += completed_time - chnl_stime;
			remaining -= xfer_size;
			chnl_stime = chnl_etime;
		}

		// lun->next_lun_avail_time = chnl_etime;
		break;

	case NAND_WRITE:
		/* write: transfer data through channel first */
		chnl_etime = chmodel_request(ch->perf_model, chnl_stime, tr->xfer_size);
		transfer_time = chnl_etime - chnl_stime;
		break;

	case NAND_ERASE:
		/* erase: only need to advance NAND status */
		break;

	case NAND_NOP:
		/* no operation: just return last completed time of lun */
		break;

	default:
		NVMEV_ERROR("Unsupported NAND command: 0x%x\n", c);
		return 0;
	}

	return transfer_time;
}

uint64_t estimate_transaction_command_time(struct nvmev_tsu_tr* tr, struct ssdparams *spp, struct ssd *ssd)
{
	int c = tr->cmd;
	uint64_t cmd_stime = (tr->stime == 0) ? __get_clock() : tr->stime;
	uint64_t nand_stime, nand_etime;
	uint64_t chnl_stime, chnl_etime;
	uint64_t remaining, xfer_size, completed_time, command_time;
	struct nand_lun *lun;
	struct ssd_channel *ch;
	struct ppa *ppa = tr->ppa;
	uint32_t cell;

	lun = get_lun(ssd, ppa);
	ch = get_ch(ssd, ppa);
	cell = get_cell(ssd, ppa);
	remaining = tr->xfer_size;
	command_time = 0;

	switch (c) {
	case NAND_READ:
		/* read: perform NAND cmd first */
		nand_stime = max(lun->next_lun_avail_time, cmd_stime);
		if (tr->xfer_size == 4096) {
			nand_etime = nand_stime + spp->pg_4kb_rd_lat[cell];
		} else {
			nand_etime = nand_stime + spp->pg_rd_lat[cell];
		}
		command_time = nand_etime - nand_stime;
		break;

	case NAND_WRITE:
		command_time = spp->pg_wr_lat;
		break;

	case NAND_ERASE:
		/* erase: only need to advance NAND status */
		command_time = spp->blk_er_lat;
		break;

	case NAND_NOP:
		/* no operation: just return last completed time of lun */
		break;

	default:
		NVMEV_ERROR("Unsupported NAND command: 0x%x\n", c);
		return 0;
	}

	return command_time;
}

bool is_lun_avail(struct nvmev_tsu_tr* tr){
	// struct nvmev_tsu_tr* tr_in_chipqueue = &chip_queue->queue[curr];
	// NVMEV_INFO("ch: %d chip: %d tr_addr: %p curr: %d\n", ch, chip, tr, curr);
	// NVMEV_INFO("ch: %d chip: %d tr_in_chipqueue: %p curr: %d\n", ch, chip, tr_in_chipqueue, curr);
	struct nand_lun *lun;
	struct nvmev_ns *ns = &nvmev_vdev->ns[tr->nsid];
    struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;
	struct ssd *ssd = zns_ftl->ssd;
	uint64_t now_time = __get_clock();
	struct ppa *ppa = tr->ppa;
	lun = get_lun(ssd, ppa);

	if(lun == NULL) return true;
	// NVMEV_INFO("lun_address: %p\n", lun);
	// NVMEV_INFO("sqid: %d entry: %d curr: %d ch: %d chip: %d lun: %d stime: %lld  now_time: %lld lun->next_lun_avail_time: %lld\n", 
	// 	tr->sqid, tr->sq_entry, curr, tr->ppa->g.ch, tr->ppa->g.chip, tr->ppa->g.lun, tr->stime, now_time, lun->next_lun_avail_time);
	if(lun->next_lun_avail_time >= now_time)
		return false;
	else
		return true;
	// struct ppa *ppa = tr->ppa;
	// NVMEV_INFO("sqid: %d entry: %d stime: %lld lun: %d \n", tr->sqid, tr->sq_entry, tr->stime, tr->ppa->g.lun);
	// lun = get_lun(ssd, ppa);
	// NVMEV_INFO("sqid: %d entry: %d stime: %lld lun: %d lun->next_lun_avail_time: %lld\n", tr->sqid, tr->sq_entry, tr->stime, tr->ppa->g.lun, lun->next_lun_avail_time);
	// if(lun->next_lun_avail_time >= now_time)
    //     return false;
	// else
	// 	return true;
}

void estimate_alone_waiting_time(
	struct nvmev_transaction_queue* chip_queue,
	int curr,
	struct list_head* tmp_queue
)
{
	uint64_t chip_busy_time=0, expected_last_time=0;
    unsigned int start = chip_queue->io_seq, end = chip_queue->io_seq_end;

    struct nvmev_tsu_tr* tr = &chip_queue->queue[curr];
    struct nvmev_ns *ns = &nvmev_vdev->ns[tr->nsid];
    struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;
    struct nand_lun *lun;
	struct ssd_channel *ch;
    struct ppa *ppa = tr->ppa;
    struct ssdparams *spp;
    struct ssd *ssd = zns_ftl->ssd;
	struct nvmev_tsu_tr* tmp;
	
    uint32_t cell;
    uint64_t remaining, xfer_size, transfer_time, command_time;
    int c = tr->cmd;
	unsigned int die;
	struct die_queue_entry *die_entry;
	struct list_head *die_lists[chip_queue->nr_luns];
	struct nvme_tsu_tr_list_entry *tr_tmp, *entry;
    spp = &ssd->sp;
    lun = get_lun(ssd, ppa);
	ch = get_ch(ssd, ppa);
    cell = get_cell(ssd, ppa);
    remaining = tr->xfer_size;

	// 计算方法有问题？
    // if(lun->next_lun_avail_time >= tr->stime){
    //     chip_busy_time = lun->next_lun_avail_time - tr->stime;
    // }

	// 遍历tmp-queue，计算curr的estimated_alone_waiting_time
	list_for_each_entry_reverse(entry, tmp_queue, list){
		tmp = &chip_queue->queue[entry->entry];
		if(tmp->sqid == tr->sqid){
			transfer_time = estimate_transaction_transfer_time(tmp, spp, ssd);
			command_time = estimate_transaction_command_time(tmp, spp, ssd);
			tmp->transfer_time = transfer_time;
			tmp->command_time = command_time;
			expected_last_time = tmp->estimated_alone_waiting_time + transfer_time + command_time;
			break;
		}
	}
	tr->estimated_alone_waiting_time = chip_busy_time + expected_last_time;
}

void estimate_alone_waiting_time2(struct nvmev_transaction_queue* chip_queue, int curr){
    uint64_t chip_busy_time=0, expected_last_time=0;
    unsigned int start = chip_queue->io_seq, end = chip_queue->io_seq_end;

    struct nvmev_tsu_tr* tr = &chip_queue->queue[curr];
    struct nvmev_ns *ns = &nvmev_vdev->ns[tr->nsid];
    struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;
    struct nand_lun *lun;
	struct ssd_channel *ch;
    struct ppa *ppa = tr->ppa;
    struct ssdparams *spp;
    struct ssd *ssd = zns_ftl->ssd;
	struct nvmev_tsu_tr* tmp;

    uint32_t cell;
    uint64_t remaining, xfer_size, transfer_time, command_time;
    int c = tr->cmd;

    spp = &ssd->sp;
    lun = get_lun(ssd, ppa);
	ch = get_ch(ssd, ppa);
    cell = get_cell(ssd, ppa);
    remaining = tr->xfer_size;

    if(lun->next_lun_avail_time >= tr->stime){
        chip_busy_time = lun->next_lun_avail_time - tr->stime;
    }

    if(curr != start){
		curr = tr->prev;

        while(curr != -1){
			tmp = &chip_queue->queue[curr];
			if(tmp->is_completed){
				curr = tmp->prev;
				continue;
			}

            if(tmp->sqid == tr->sqid){
				transfer_time = estimate_transaction_transfer_time(tmp, spp, ssd);
				command_time = estimate_transaction_command_time(tmp, spp, ssd);
                expected_last_time = tmp->estimated_alone_waiting_time + transfer_time + command_time;
                break;
            }
            curr = tmp->prev;
        }
    }

    tr->estimated_alone_waiting_time = chip_busy_time + expected_last_time;
}

double fairness_based_on_average_slowdown(
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
	spp = &ssd->sp;
	*flow_with_max_average_slowdown = tr->sqid;

	list_for_each_entry_safe(entry, tmp, tmp_queue, list){
		tr_entry = entry->entry;
		tr = &chip_queue->queue[tr_entry];
		tr_execute_time = estimate_transaction_transfer_time(tr, spp, ssd) + estimate_transaction_command_time(tr, spp, ssd);
		total_finish_time += tr_execute_time;

		uint64_t transaction_alone_time = tr->estimated_alone_waiting_time + tr_execute_time;
		uint64_t transaction_shared_time = total_finish_time + (__get_clock() - tr->stime);
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

double fairness_based_on_average_slowdown2(
	struct nvmev_transaction_queue* chip_queue, 
	unsigned int* flow_with_max_average_slowdown,
	unsigned int start,
	unsigned int end
){
	if(start == end) return 1.0;
	LIST_HEAD(sum_slowdown);
	uint64_t total_finish_time = 0;
	unsigned int itr = chip_queue->io_seq;
	struct nvmev_tsu_tr* tr = &chip_queue->queue[itr];
    struct nvmev_ns *ns = &nvmev_vdev->ns[tr->nsid];
    struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;
	struct ssdparams *spp;
    struct ssd *ssd = zns_ftl->ssd;
	double slowdown_max = DBL_MIN, slowdown_min = DBL_MAX;
	int stream_count = 0;
	struct stream_data* data;
	uint64_t tr_execute_time;
	spp = &ssd->sp;
	*flow_with_max_average_slowdown = tr->sqid;

	if(chip_queue->nr_trs_in_fly <= 1) return 1.0;

	while(itr != -1 && itr != end){
		tr = &chip_queue->queue[itr];
		tr_execute_time = estimate_transaction_transfer_time(tr, spp, ssd) + estimate_transaction_command_time(tr, spp, ssd);
		total_finish_time += tr_execute_time;

		uint64_t transaction_alone_time = tr->estimated_alone_waiting_time + tr_execute_time;
		uint64_t transaction_shared_time = total_finish_time + (__get_clock() - tr->stime);
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
		itr = tr->next;
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

void update_waiting_time(
	struct nvmev_transaction_queue* chip_queue,
	struct list_head* tmp_queue
){
	struct nvme_tsu_tr_list_entry *entry, *tmp;
	unsigned int tr_entry;
	struct nvmev_tsu_tr* tr;
	uint64_t prev_estimated_alone_waiting_time = 0;

	list_for_each_entry_safe(entry, tmp, tmp_queue, list){
		tr_entry = entry->entry;
		tr = &chip_queue->queue[tr_entry];

		entry->estimated_alone_waiting_time = prev_estimated_alone_waiting_time + tr->transfer_time + tr->command_time;
		prev_estimated_alone_waiting_time = entry->estimated_alone_waiting_time;
	}
}

void move_forward(
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

void traverse_tmp_queue(
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

void reorder_for_fairness(
		struct nvmev_transaction_queue* chip_queue,
		struct list_head* tmp_queue, 
		unsigned int flow_with_max_average_slowdown,
		unsigned int curr
	){
		/**
		 * 1. 把所有flow_with_max_average_slowdown的请求放到队列首部。
		 * 2. 保证针对同一zone的请求的相对顺序不变。
		 * */ 
	struct nvmev_tsu_tr* tr = &chip_queue->queue[curr];
	struct nvme_tsu_tr_list_entry *entry;

	traverse_tmp_queue(chip_queue, tmp_queue, true);
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

	update_waiting_time(chip_queue, tmp_queue);
	traverse_tmp_queue(chip_queue, tmp_queue, false);
}




/**
 * Aggregate transactions that have no zone conflict and can be executed in parallel to form a package.
*/
void get_transaction_packages_without_zone_conflict(struct nvmev_transaction_queue* chip_queue) {
	volatile unsigned int curr = chip_queue->io_seq;
	struct nvmev_tsu_tr* tr;
	int channel, chip, die, tr_entry;
	unsigned int flow_with_max_average_slowdown = 0;
	double fairness = 0.0;
	unsigned int fairness_show = 0;
	unsigned int idx = 0;
	LIST_HEAD(tmp_queue);

	if(curr == -1) return;

	while (curr != -1){
		tr = &chip_queue->queue[curr];
		if(tr->is_completed || tr->is_copyed) {
			curr = tr->next;
			continue;
		}

		// if(tr->is_completed) {
		// 	curr = tr->next;
		// 	continue;
		// }

		channel = tr->ppa->g.ch;
		chip = tr->ppa->g.chip;
		die = tr->ppa->g.lun;


		// 1. Estimate alone_waiting_time for each TR.
		// estimate_alone_waiting_time2(chip_queue, curr);
		estimate_alone_waiting_time(chip_queue, curr, &tmp_queue);

		// 1. 插入到tmp-queue
		struct nvme_tsu_tr_list_entry *tr_tmp = kzalloc(sizeof(struct nvme_tsu_tr_list_entry), GFP_KERNEL);
		*tr_tmp = (struct nvme_tsu_tr_list_entry){
			.channel = channel,
			.chip = chip,
			.die = die,
			.entry = curr,
			.is_copyed = false,
			.estimated_alone_waiting_time = tr->estimated_alone_waiting_time,
			.list = LIST_HEAD_INIT(tr_tmp->list),
		};
		list_add_tail(&tr_tmp->list, &tmp_queue);

		// 3. Compute fairness and reorder temporary queue for max fairness.
		fairness = fairness_based_on_average_slowdown(chip_queue, &tmp_queue, curr, &flow_with_max_average_slowdown);
		if(fairness <= 0.8) {
			// Adjust the order of TRs in the queue to maximize fairness.
			fairness_show = fairness * 1000;
			NVMEV_INFO("before reorder, fairness = %d  flow_with_max_average_slowdown = %d\n", fairness_show, flow_with_max_average_slowdown);
			reorder_for_fairness(chip_queue, &tmp_queue, flow_with_max_average_slowdown, curr);

			fairness = fairness_based_on_average_slowdown(chip_queue, &tmp_queue, curr, &flow_with_max_average_slowdown);
			fairness_show = fairness * 1000;
			NVMEV_INFO("after reorder, fairness = %d  flow_with_max_average_slowdown = %d\n", fairness_show, flow_with_max_average_slowdown);
		}

		// insert_to_die_queue(&chip_queue->die_queues[die], channel, chip, die, curr);
		tr->is_copyed = true;
		curr = tr->next;
	}

	// 4. Insert temporary queue to die queue.
	struct nvme_tsu_tr_list_entry *entry, *tmp;
	list_for_each_entry_safe(entry, tmp, &tmp_queue, list){
		channel = entry->channel;
		chip = entry->chip;
		die = entry->die;
		tr_entry = entry->entry;
		insert_to_die_queue(&chip_queue->die_queues[die], channel, chip, die, tr_entry);
	}
	
}

void delete_node(struct nvmev_transaction_queue* chip_queue, unsigned int node) {
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

void add_node_after(struct nvmev_transaction_queue* chip_queue, unsigned int node, unsigned int curr) {
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

void display_chip_queue(struct nvmev_transaction_queue* chip_queue){
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

void move_node_after(struct nvmev_transaction_queue* chip_queue, unsigned int node, unsigned int curr) {
	// Delete node from origin position.
	delete_node(chip_queue, node);
	// Add node to the back of curr.
	add_node_after(chip_queue, node, curr);
}

unsigned int get_nr_used_dies(struct nvmev_transaction_queue* chip_queue)
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

struct nvmev_process_queue* __alloc_process_queue_entry(struct nvmev_process_queue* process_queue, unsigned int* entry) {
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

 void __insert_tr_to_process_queue(unsigned int entry, struct nvmev_process_queue *process_queue)
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

 void __enqueue_transaction_to_process_queue(
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

 void __reclaim_transaction_in_process_queue(struct nvmev_process_queue* process_queue) {
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

void print_process_queue(struct nvmev_process_queue* process_queue) {
	unsigned int curr = process_queue->io_seq;
	struct transaction_entry* tr;
	NVMEV_INFO("------process_queue begin-------------");
	while(curr != -1) {
		tr = &process_queue->queue[curr];
		NVMEV_INFO("entry: %d die: %d \n", tr->entry, tr->die);
		curr = tr->next;
	}
	NVMEV_INFO("------process_queue end-------------");
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
		clear_copyed_entries_in_die_queue(&chip_queue->die_queues[die]);
		// clear_entries_in_die_queue(&chip_queue->die_queues[die]);
	}
}

void print_transaction_packages(struct nvmev_transaction_queue* chip_queue)
{
	unsigned int curr = chip_queue->io_seq;
	struct transaction_package *package;
	struct transaction_entry *tr;
	int die;

	list_for_each_entry(package, &chip_queue->transaction_package_list, list){
		NVMEV_INFO("====package====");
		for (die = 0; die < chip_queue->nr_luns; die++) {
			tr = &package->transactions[die];
			if(tr->is_completed || !is_die_used(package, die)){
				continue;
			}
			NVMEV_INFO("transaction entry: %d die: %d\n", tr->entry, tr->die);
		}
		NVMEV_INFO("====package====");
	}
}

void copy_to_process_queue(struct nvmev_transaction_queue* chip_queue, struct nvmev_process_queue* process_queue) {
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
		NVMEV_INFO("In die-%d queue, entry : %d, estimated_alone_waiting_time = %lld\n", die, entry, tr->estimated_alone_waiting_time);
		__enqueue_transaction_to_process_queue(process_queue, curr, channel, chip, die);
		// spin_lock(&chip_queue->tr_lock);
		tr->is_copyed = true;
		// spin_unlock(&chip_queue->tr_lock);
		curr = tr->next;
		entry++;
	}
}

void schedule_fairness(struct nvmev_tsu* tsu){
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
			get_transaction_packages_without_zone_conflict(chip_queue);
			
			reorder_for_zone_conflict(chip_queue, process_queue);
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
