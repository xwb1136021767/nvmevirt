#include "tsu_fairness.h"

uint64_t caculate_tr_completed_time(struct nvmev_tsu_tr* tr, struct ssdparams *spp, struct ssd *ssd){
    int c = tr->cmd;
	uint64_t cmd_stime = (tr->stime == 0) ? __get_clock() : tr->stime;
	uint64_t nand_stime, nand_etime;
	uint64_t chnl_stime, chnl_etime;
	uint64_t remaining, xfer_size, completed_time;
	struct nand_lun *lun;
	struct ssd_channel *ch;
	struct ppa *ppa = tr->ppa;
	uint32_t cell;

	lun = get_lun(ssd, ppa);
	ch = get_ch(ssd, ppa);
	cell = get_cell(ssd, ppa);
	remaining = tr->xfer_size;

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

			remaining -= xfer_size;
			chnl_stime = chnl_etime;
		}

		// lun->next_lun_avail_time = chnl_etime;
		break;

	case NAND_WRITE:
		/* write: transfer data through channel first */
		chnl_etime = chmodel_request(ch->perf_model, chnl_stime, tr->xfer_size);

		/* write: then do NAND program */
		nand_stime = chnl_etime;
		nand_etime = nand_stime + spp->pg_wr_lat;
		// lun->next_lun_avail_time = nand_etime;
		completed_time = nand_etime;
		break;

	case NAND_ERASE:
		/* erase: only need to advance NAND status */
		nand_etime = nand_stime + spp->blk_er_lat;
		// lun->next_lun_avail_time = nand_etime;
		completed_time = nand_etime;
		break;

	case NAND_NOP:
		/* no operation: just return last completed time of lun */
		// lun->next_lun_avail_time = nand_stime;
		completed_time = nand_stime;
		break;

	default:
		NVMEV_ERROR("Unsupported NAND command: 0x%x\n", c);
		return 0;
	}

	return completed_time;
}

void estimate_alone_waiting_time(struct nvmev_transaction_queue* chip_queue, int curr){
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

    uint32_t cell;
    uint64_t remaining, xfer_size, completed_time;
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

        while(curr!=-1){
            struct nvmev_tsu_tr* tmp = &chip_queue->queue[curr];
            if(tmp->sqid == tr->sqid){
                completed_time = caculate_tr_completed_time(tmp, spp, ssd);
                expected_last_time = tmp->estimated_alone_waiting_time + completed_time;
                break;
            }

            curr = tmp->prev;
        }
    }

    tr->estimated_alone_waiting_time = chip_busy_time + expected_last_time;
}

double fairness_based_on_average_slowdown(struct nvmev_transaction_queue* chip_queue, unsigned int* flow_with_max_average_slowdown){
	LIST_HEAD(sum_slowdown);
	uint64_t total_finish_time = 0;
	unsigned int start = chip_queue->io_seq;
	unsigned int itr = start;
	struct nvmev_tsu_tr* tr = &chip_queue->queue[itr];
    struct nvmev_ns *ns = &nvmev_vdev->ns[tr->nsid];
    struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;
	struct ssdparams *spp;
    struct ssd *ssd = zns_ftl->ssd;
	double slowdown_max = DBL_MIN, slowdown_min = DBL_MAX;
	int stream_count = 0;
	struct stream_data* data;
	spp = &ssd->sp;
	*flow_with_max_average_slowdown = tr->sqid;

	if(chip_queue->nr_trs_in_fly <= 1) return 1.0;

	while(itr != -1){
		tr = &chip_queue->queue[itr];
		uint64_t tr_execute_time = caculate_tr_completed_time(tr, spp, ssd);
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
	};
	
	if (stream_count == 1)
	{
		*flow_with_max_average_slowdown = -1;
	}
	return (double)slowdown_min / slowdown_max;
}

/**
 * Aggregate transactions that have no zone conflict and can be executed in parallel to form a package.
*/
void get_transaction_packages_without_zone_conflict(struct nvmev_transaction_queue* chip_queue) {
	volatile unsigned int curr = chip_queue->io_seq;

	while (curr != -1){
		struct nvmev_tsu_tr* tr = &chip_queue->queue[curr];
		if(tr->is_completed) {
			curr = tr->next;
			continue;
		}

		struct transaction_package* package;
		bool find_package = false;
		list_for_each_entry(package, &chip_queue->transaction_package_list, list) {
			if(!is_die_used(package, tr->ppa->g.lun)){
				set_die_used(package, tr->ppa->g.lun);
				insert_tr_to_package(package, curr);
				find_package = true;
				break;
			}
    	}

		if(!find_package) {
			package = generate_package_and_insert_to_chip_queue(chip_queue);
			set_die_used(package, tr->ppa->g.lun);
			insert_tr_to_package(package, curr);
		}
		curr = tr->next;
	}
}

void delete_node(struct nvmev_transaction_queue* chip_queue, unsigned int node) {
	unsigned int prev = chip_queue->queue[node].prev;
	unsigned int next = chip_queue->queue[node].next;

	NVMEV_DEBUG("node= %d, prev= %d next= %d \n", node, prev, next);

	if(prev != -1)
		chip_queue->queue[prev].next = next;
	if(next != -1)
		chip_queue->queue[next].prev = prev;
	chip_queue->queue[node].prev = -1;
	chip_queue->queue[node].next = -1;
}

void add_node_after(struct nvmev_transaction_queue* chip_queue, unsigned int node, unsigned int curr) {
	unsigned int prev = chip_queue->queue[curr].prev;
	unsigned int next = chip_queue->queue[curr].next;

	chip_queue->queue[node].prev = curr;
	chip_queue->queue[node].next = next;
	chip_queue->queue[curr].next = node;
	if(next != -1)
		chip_queue->queue[next].prev = node;
}

void move_node_after(struct nvmev_transaction_queue* chip_queue, unsigned int node, unsigned int curr) {
	// Delete node from origin position.
	delete_node(chip_queue, node);
	// Add node to the back of curr.
	add_node_after(chip_queue, node, curr);
}

void reorder_for_zone_conflict(struct nvmev_transaction_queue* chip_queue) {
	unsigned int curr = chip_queue->io_seq;
	if(curr == -1) return;
	
	struct transaction_package *package;
	list_for_each_entry(package, &chip_queue->transaction_package_list, list){
		struct transaction_entry *entry;
		list_for_each_entry(entry, &package->transactions_list, list){
			if(curr != entry->entry){
				// Move entry to the back of curr.
				spin_lock(&chip_queue->tr_lock);
				move_node_after(chip_queue, entry->entry, curr);
				spin_unlock(&chip_queue->tr_lock);
				curr = entry->entry;
			}
		}
		clear_transactions_in_package(package);
	}
	clear_packages(chip_queue);
}

void schedule_fairness(struct nvmev_tsu* tsu){
    unsigned int i,j;
	double fairness = 0.0;
	unsigned int flow_with_max_average_slowdown = 0;

    for(i=0;i<tsu->nchs;i++){
        for(j=0;j<tsu->nchips;j++){
            struct nvmev_transaction_queue* chip_queue = &tsu->chip_queue[i][j];
			volatile unsigned int curr = chip_queue->io_seq;
			unsigned int nr_processed = 0;
			if(curr == -1) continue;

			get_transaction_packages_without_zone_conflict(chip_queue);
			// reorder_for_zone_conflict(chip_queue);
            while(curr != -1){
                struct nvmev_tsu_tr* tr = &chip_queue->queue[curr];
                struct nvmev_ns *ns = &nvmev_vdev->ns[tr->nsid];
                if(tr->is_completed){
                    curr = tr->next;
                    continue;
                }

				spin_lock(&chip_queue->tr_lock);
				tr->stime = __get_clock();
				// compute alone waiting time for 
                estimate_alone_waiting_time(chip_queue, curr);
				spin_unlock(&chip_queue->tr_lock);

                NVMEV_DEBUG("process tr from ch: %d lun: %d zid: %d, curr = %d ,estimated_alone_waiting_time=%lld\n", 
                        i, j, tr->zid, curr, tr->estimated_alone_waiting_time);
                if (!ns->proc_tr_cmd(ns, tr)){
                    return;
                }
                
                mb();
				spin_lock(&chip_queue->tr_lock);
                tr->is_completed = true;
				spin_unlock(&chip_queue->tr_lock);
				nr_processed++;
                curr = tr->next;
            }
			NVMEV_DEBUG("tsu processed %d transactions in ch: %d chip: %d\n", nr_processed, i, j);
			// kernel_fpu_begin();
			// fairness = fairness_based_on_average_slowdown(chip_queue, &flow_with_max_average_slowdown);
			// kernel_fpu_end();
			// NVMEV_DEBUG("(ch: %d lun: %d) fairness: %d  flow_with_max_average_slowdown: %d\n", i, j, (int)fairness, flow_with_max_average_slowdown);
        }
    }
}
