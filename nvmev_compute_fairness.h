#ifndef _NVMEVIRT_NVMEV_COMPUTE_FAIRNESS_H
#define _NVMEVIRT_NVMEV_COMPUTE_FAIRNESS_H
#include <float.h>
#include "nvmev_tsh.h"

static uint64_t estimate_transaction_transfer_time(struct nvmev_tsu_tr* tr, struct ssdparams *spp, struct ssd *ssd)
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

static uint64_t estimate_transaction_command_time(struct nvmev_tsu_tr* tr, struct ssdparams *spp, struct ssd *ssd)
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

static void estimate_alone_waiting_time(
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

	
    if(lun->next_lun_avail_time >= tr->stime){
        chip_busy_time = lun->next_lun_avail_time - tr->stime;
    }

	// 遍历tmp-queue，计算curr的estimated_alone_waiting_time
    // 反向遍历tmp-queue，找到最后一个具有相同sqid的事务即可
	list_for_each_entry_reverse(entry, tmp_queue, list){
		tmp = &chip_queue->queue[entry->entry];
		if(tmp->sqid == tr->sqid){
			transfer_time = tmp->transfer_time;
			command_time = tmp->command_time;
			expected_last_time = tmp->estimated_alone_waiting_time + transfer_time + command_time;
			break;
		}
	}
	tr->transfer_time = estimate_transaction_transfer_time(tr, spp, ssd);
	tr->command_time = estimate_transaction_command_time(tr, spp, ssd);
	tr->estimated_alone_waiting_time = chip_busy_time + expected_last_time;
}

/**
 * 重新计算排序后的estimated_alone_waiting_time
*/
static void update_estimated_alone_waiting_time(
	struct nvmev_transaction_queue* chip_queue,
	struct list_head* tmp_queue
){
	struct nvme_tsu_tr_list_entry *entry, *tmp, *data;
	unsigned int tr_entry;
	struct nvmev_tsu_tr* tr;
	LIST_HEAD(prev_estimated_alone_waiting_time);

	list_for_each_entry_safe(entry, tmp, tmp_queue, list){
		tr_entry = entry->entry;
		tr = &chip_queue->queue[tr_entry];

		data = find_nvme_tsu_tr_list_entry(&prev_estimated_alone_waiting_time, entry->sqid);
		if(data){
			entry->estimated_alone_waiting_time = data->estimated_alone_waiting_time + data->transfer_time + data->command_time;
			data->estimated_alone_waiting_time = entry->estimated_alone_waiting_time;
			data->transfer_time = entry->transfer_time;
			data->command_time = entry->command_time;
		}else{
			entry->estimated_alone_waiting_time = 0;
			data = kmalloc(sizeof(struct nvme_tsu_tr_list_entry), GFP_KERNEL);
			*data = (struct nvme_tsu_tr_list_entry){
				.sqid = entry->sqid,
				.estimated_alone_waiting_time = 0,
				.transfer_time = entry->transfer_time,
				.command_time = entry->command_time,
			};
			list_add(&data->list, &prev_estimated_alone_waiting_time);
		}
	}

	clear_nvme_tsu_tr_list_entry(&prev_estimated_alone_waiting_time);
}


#endif
