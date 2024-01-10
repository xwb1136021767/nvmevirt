// SPDX-License-Identifier: GPL-2.0-only

#include <linux/kthread.h>
#include <linux/ktime.h>
#include <linux/highmem.h>
#include <linux/sched/clock.h>

#include "nvmev.h"
#include "dma.h"
#include "ssd_config.h"
#include "tsu_fifo.h"
#include "tsu_fairness.h"
#if (SUPPORTED_SSD_TYPE(CONV) || SUPPORTED_SSD_TYPE(ZNS))
#include "ssd.h"
#else
struct buffer;
#endif

#undef PERF_DEBUG

#define sq_entry(entry_id) sq->sq[SQ_ENTRY_TO_PAGE_NUM(entry_id)][SQ_ENTRY_TO_PAGE_OFFSET(entry_id)]
#define cq_entry(entry_id) cq->cq[CQ_ENTRY_TO_PAGE_NUM(entry_id)][CQ_ENTRY_TO_PAGE_OFFSET(entry_id)]

extern bool io_using_dma;

static inline unsigned int __get_io_worker(int sqid)
{
#ifdef CONFIG_NVMEV_IO_WORKER_BY_SQ
	return (sqid - 1) % nvmev_vdev->config.nr_io_workers;
#else
	return nvmev_vdev->io_worker_turn;
#endif
}

static inline unsigned long long __get_wallclock(void)
{
	return cpu_clock(nvmev_vdev->config.cpu_nr_dispatcher);
}

static unsigned int __do_perform_io(int sqid, int sq_entry)
{
	struct nvmev_submission_queue *sq = nvmev_vdev->sqes[sqid];
	struct nvme_rw_command *cmd = &sq_entry(sq_entry).rw;
	size_t offset;
	size_t length, remaining;
	int prp_offs = 0;
	int prp2_offs = 0;
	u64 paddr;
	u64 *paddr_list = NULL;
	size_t nsid = cmd->nsid - 1; // 0-based

	offset = cmd->slba << 9;
	length = (cmd->length + 1) << 9;
	remaining = length;

	while (remaining) {
		size_t io_size;
		void *vaddr;
		size_t mem_offs = 0;

		prp_offs++;
		if (prp_offs == 1) {
			paddr = cmd->prp1;
		} else if (prp_offs == 2) {
			paddr = cmd->prp2;
			if (remaining > PAGE_SIZE) {
				paddr_list = kmap_atomic_pfn(PRP_PFN(paddr)) +
					     (paddr & PAGE_OFFSET_MASK);
				paddr = paddr_list[prp2_offs++];
			}
		} else {
			paddr = paddr_list[prp2_offs++];
		}

		vaddr = kmap_atomic_pfn(PRP_PFN(paddr));

		io_size = min_t(size_t, remaining, PAGE_SIZE);

		if (paddr & PAGE_OFFSET_MASK) {
			mem_offs = paddr & PAGE_OFFSET_MASK;
			if (io_size + mem_offs > PAGE_SIZE)
				io_size = PAGE_SIZE - mem_offs;
		}

		if (cmd->opcode == nvme_cmd_write ||
		    cmd->opcode == nvme_cmd_zone_append) {
			memcpy(nvmev_vdev->ns[nsid].mapped + offset, vaddr + mem_offs, io_size);
		} else if (cmd->opcode == nvme_cmd_read) {
			memcpy(vaddr + mem_offs, nvmev_vdev->ns[nsid].mapped + offset, io_size);
		}

		kunmap_atomic(vaddr);

		remaining -= io_size;
		offset += io_size;
	}

	if (paddr_list != NULL)
		kunmap_atomic(paddr_list);

	return length;
}

static u64 paddr_list[513] = {
	0,
}; // Not using index 0 to make max index == num_prp
static unsigned int __do_perform_io_using_dma(int sqid, int sq_entry)
{
	struct nvmev_submission_queue *sq = nvmev_vdev->sqes[sqid];
	struct nvme_rw_command *cmd = &sq_entry(sq_entry).rw;
	size_t offset;
	size_t length, remaining;
	int prp_offs = 0;
	int prp2_offs = 0;
	int num_prps = 0;
	u64 paddr;
	u64 *tmp_paddr_list = NULL;
	size_t io_size;
	size_t mem_offs = 0;

	offset = cmd->slba << 9;
	length = (cmd->length + 1) << 9;
	remaining = length;

	memset(paddr_list, 0, sizeof(paddr_list));
	/* Loop to get the PRP list */
	while (remaining) {
		io_size = 0;

		prp_offs++;
		if (prp_offs == 1) {
			paddr_list[prp_offs] = cmd->prp1;
		} else if (prp_offs == 2) {
			paddr_list[prp_offs] = cmd->prp2;
			if (remaining > PAGE_SIZE) {
				tmp_paddr_list = kmap_atomic_pfn(PRP_PFN(paddr_list[prp_offs])) +
						 (paddr_list[prp_offs] & PAGE_OFFSET_MASK);
				paddr_list[prp_offs] = tmp_paddr_list[prp2_offs++];
			}
		} else {
			paddr_list[prp_offs] = tmp_paddr_list[prp2_offs++];
		}

		io_size = min_t(size_t, remaining, PAGE_SIZE);

		if (paddr_list[prp_offs] & PAGE_OFFSET_MASK) {
			mem_offs = paddr_list[prp_offs] & PAGE_OFFSET_MASK;
			if (io_size + mem_offs > PAGE_SIZE)
				io_size = PAGE_SIZE - mem_offs;
		}

		remaining -= io_size;
	}
	num_prps = prp_offs;

	if (tmp_paddr_list != NULL)
		kunmap_atomic(tmp_paddr_list);

	remaining = length;
	prp_offs = 1;

	/* Loop for data transfer */
	while (remaining) {
		size_t page_size;
		mem_offs = 0;
		io_size = 0;
		page_size = 0;

		paddr = paddr_list[prp_offs];
		page_size = min_t(size_t, remaining, PAGE_SIZE);

		/* For non-page aligned paddr, it will never be between continuous PRP list (Always first paddr)  */
		if (paddr & PAGE_OFFSET_MASK) {
			mem_offs = paddr & PAGE_OFFSET_MASK;
			if (page_size + mem_offs > PAGE_SIZE) {
				page_size = PAGE_SIZE - mem_offs;
			}
		}

		for (prp_offs++; prp_offs <= num_prps; prp_offs++) {
			if (paddr_list[prp_offs] == paddr_list[prp_offs - 1] + PAGE_SIZE)
				page_size += PAGE_SIZE;
			else
				break;
		}

		io_size = min_t(size_t, remaining, page_size);

		if (cmd->opcode == nvme_cmd_write ||
		    cmd->opcode == nvme_cmd_zone_append) {
			ioat_dma_submit(paddr, nvmev_vdev->config.storage_start + offset, io_size);
		} else if (cmd->opcode == nvme_cmd_read) {
			ioat_dma_submit(nvmev_vdev->config.storage_start + offset, paddr, io_size);
		}

		remaining -= io_size;
		offset += io_size;
	}

	return length;
}

static void __insert_req_sorted(unsigned int entry, struct nvmev_io_worker *worker,
				unsigned long nsecs_target)
{
	/**
	 * Requests are placed in @work_queue sorted by their target time.
	 * @work_queue is statically allocated and the ordered list is
	 * implemented by chaining the indexes of entries with @prev and @next.
	 * This implementation is nasty but we do this way over dynamically
	 * allocated linked list to minimize the influence of dynamic memory allocation.
	 * Also, this O(n) implementation can be improved to O(logn) scheme with
	 * e.g., red-black tree but....
	 */
	// spin_lock(&worker->entry_lock);
	if (worker->io_seq == -1) {
		worker->io_seq = entry;
		worker->io_seq_end = entry;
	} else {
		unsigned int curr = worker->io_seq_end;

		while (curr != -1) {
			if (worker->work_queue[curr].nsecs_target <= worker->latest_nsecs)
				break;

			if (worker->work_queue[curr].nsecs_target <= nsecs_target)
				break;

			curr = worker->work_queue[curr].prev;
		}

		if (curr == -1) { /* Head inserted */
			worker->work_queue[worker->io_seq].prev = entry;
			worker->work_queue[entry].next = worker->io_seq;
			worker->io_seq = entry;
		} else if (worker->work_queue[curr].next == -1) { /* Tail */
			worker->work_queue[entry].prev = curr;
			worker->io_seq_end = entry;
			worker->work_queue[curr].next = entry;
		} else { /* In between */
			worker->work_queue[entry].prev = curr;
			worker->work_queue[entry].next = worker->work_queue[curr].next;

			worker->work_queue[worker->work_queue[entry].next].prev = entry;
			worker->work_queue[curr].next = entry;
		}
	}
	// spin_unlock(&worker->entry_lock);
}

static struct nvmev_io_worker *__allocate_work_queue_entry(int sqid, unsigned int *entry)
{
	unsigned int io_worker_turn = __get_io_worker(sqid);
	struct nvmev_io_worker *worker = &nvmev_vdev->io_workers[io_worker_turn];
	unsigned int e = worker->free_seq;
	struct nvmev_io_work *w = worker->work_queue + e;

	if (w->next >= NR_MAX_PARALLEL_IO) {
		WARN_ON_ONCE("IO queue is almost full");
		return NULL;
	}

	if (++io_worker_turn == nvmev_vdev->config.nr_io_workers)
		io_worker_turn = 0;
	nvmev_vdev->io_worker_turn = io_worker_turn;
	
	// spin_lock(&worker->entry_lock);
	worker->free_seq = w->next;
	// spin_unlock(&worker->entry_lock);
	BUG_ON(worker->free_seq >= NR_MAX_PARALLEL_IO);
	*entry = e;

	return worker;
}

void traverse_chip_queue(struct nvmev_transaction_queue *chip_queue){
	NVMEV_DEBUG("traverse io_seq -- io_end\n");
	unsigned int curr = chip_queue->io_seq;
	while(curr != -1){
		struct nvmev_tsu_tr *tr = &chip_queue->queue[curr];
		NVMEV_DEBUG("curr=%d  ", curr);
		curr = tr->next;
	}
	NVMEV_DEBUG("\n");

	NVMEV_DEBUG("traverse free_seq -- free_end\n");
	curr = chip_queue->free_seq;
	while(curr != -1){
		struct nvmev_tsu_tr *tr = &chip_queue->queue[curr];
		NVMEV_DEBUG("curr=%d  ", curr);
		curr = tr->next;
	}
	NVMEV_DEBUG("\n");
}

struct nvmev_transaction_queue* __allocate_tr_queue_entry(uint64_t channel, uint64_t chip, unsigned int *entry){
	struct nvmev_transaction_queue *chip_queue = &nvmev_vdev->nvmev_tsu->chip_queue[channel][chip];
	unsigned int e = chip_queue->free_seq;
	struct nvmev_tsu_tr *tr = chip_queue->queue + e;

	if (tr->next >= NR_MAX_CHIP_IO) {
		WARN_ON_ONCE("Chip queue is almost full");
		return NULL;
	}

	spin_lock(&chip_queue->tr_lock);
	chip_queue->free_seq = tr->next;
	spin_unlock(&chip_queue->tr_lock);
	BUG_ON(chip_queue->free_seq >= NR_MAX_CHIP_IO);
	*entry = e;
	return chip_queue;
}

static void __insert_tr_to_chip_queue(unsigned int entry, struct nvmev_transaction_queue *chip_queue)
{
	spin_lock(&chip_queue->tr_lock);
	if(chip_queue->io_seq == -1){
		chip_queue->io_seq = entry;
		chip_queue->io_seq_end = entry;
	}else{
		unsigned int curr = chip_queue->io_seq_end;
		if(curr == -1){
			spin_unlock(&chip_queue->tr_lock);
			return;
		}
		chip_queue->queue[entry].prev = curr;
		chip_queue->io_seq_end = entry;
		chip_queue->queue[curr].next = entry;
	}
	chip_queue->nr_trs_in_fly++;
	spin_unlock(&chip_queue->tr_lock);
}

static void __insert_ret_to_tsu(struct nvmev_result_tsu* tsu_ret)
{
	spin_lock(&nvmev_vdev->nvmev_tsu->ret_lock);
	INIT_LIST_HEAD(&tsu_ret->list);
	list_add_tail(&tsu_ret->list, &nvmev_vdev->nvmev_tsu->ret_queue);
	spin_unlock(&nvmev_vdev->nvmev_tsu->ret_lock);
}

static void __enqueue_trs_to_tsu(uint32_t nsid, int sqid, int cqid, int sq_entry, unsigned long long nsecs_start,
			     struct nvmev_result *ret)
{
	unsigned int e;
	bool has_transactions = false;
	struct nvmev_transaction_queue* chip_queue;
	struct nvmev_tsu_tr* tr;
	struct nvmev_transaction *entry;
	struct nvmev_result_tsu *tsu_ret = kmalloc(sizeof(struct nvmev_result_tsu), GFP_KERNEL);
	uint64_t channel, chip;
	*(tsu_ret) = (struct nvmev_result_tsu){
		.sqid = sqid,
		.cqid = cqid,
		.sq_entry = sq_entry,
		.nsecs_start = nsecs_start,
		.nsecs_target = ret->nsecs_target,
		.status = ret->status,
		.transactions = LIST_HEAD_INIT(tsu_ret->transactions),
	};

	list_for_each_entry(entry, &ret->transactions, list) {
		if(!has_transactions) has_transactions = true;
		
		channel = entry->swr->ppa->g.ch;
		chip = entry->swr->ppa->g.chip;
		chip_queue = __allocate_tr_queue_entry(channel, chip, &e);
		if(!chip_queue)
			return;

		NVMEV_DEBUG("allocate entry(%u) from chip(ch: %lld, lun: %lld) for req(sqid: %d sq_entry: %d)\n", 
			e, channel, chip, sqid, sq_entry);

		/* dispatch transactions to chip queue*/
		spin_lock(&chip_queue->tr_lock);
		tr = chip_queue->queue + e;
		tr->nsid = entry->nsid;
		tr->zid = entry->zid;
		tr->sqid = sqid;
		tr->sq_entry = sq_entry;
		tr->lpn =  entry->lpn;
		tr->nr_lba = entry->nr_lba;
		tr->pgs = entry->pgs;
		tr->zone_elpn = entry->zone_elpn;
		tr->write_buffer = entry->write_buffer;

		tr->type = entry->swr->type;
		tr->cmd = entry->swr->cmd;
		tr->xfer_size = entry->swr->xfer_size;
		tr->stime = entry->swr->stime;
		tr->interleave_pci_dma = entry->swr->interleave_pci_dma;
		tr->is_completed = false;
		tr->is_reclaim_by_ret = false;
		tr->nsecs_target = entry->swr->stime;
		tr->ppa = kmalloc(sizeof(struct ppa), GFP_KERNEL);
		*(tr->ppa) = *(entry->swr->ppa);
		tr->prev = -1;
		tr->next = -1;
		

		/* Add tr to tsu_ret's transaction list, so that TSU can calculate resquest's response time. */
		INIT_LIST_HEAD(&tr->list);
		list_add_tail(&tr->list, &tsu_ret->transactions);
		spin_unlock(&chip_queue->tr_lock);

		mb(); /* TSU worker shall see the updated tr at once */
		__insert_tr_to_chip_queue(e, chip_queue);
	}

	tsu_ret->has_transactions = has_transactions;
	__insert_ret_to_tsu(tsu_ret);
}

static void __enqueue_io_req(int sqid, int cqid, int sq_entry, unsigned long long nsecs_start,
			     struct nvmev_result *ret)
{
	struct nvmev_submission_queue *sq = nvmev_vdev->sqes[sqid];
	struct nvmev_io_worker *worker;
	struct nvmev_io_work *w;
	unsigned int entry;

	worker = __allocate_work_queue_entry(sqid, &entry);
	if (!worker)
		return;

	w = worker->work_queue + entry;

	NVMEV_DEBUG("%s/%u[%d], sq %d cq %d, entry %d, %llu + %llu\n", worker->thread_name, entry,
		    sq_entry(sq_entry).rw.opcode, sqid, cqid, sq_entry, nsecs_start,
		    ret->nsecs_target - nsecs_start);
	
	/////////////////////////////////
	// spin_lock_irq(&worker->entry_lock);
	w->sqid = sqid;
	w->cqid = cqid;
	w->sq_entry = sq_entry;
	w->command_id = sq_entry(sq_entry).common.command_id;
	w->nsecs_start = nsecs_start;
	w->nsecs_enqueue = local_clock();
	w->nsecs_target = ret->nsecs_target;
	w->status = ret->status;
	w->is_completed = false;
	w->is_copied = false;
	w->prev = -1;
	w->next = -1;

	w->is_internal = false;
	// spin_unlock_irq(&worker->entry_lock);
	mb(); /* IO worker shall see the updated w at once */

	__insert_req_sorted(entry, worker, ret->nsecs_target);
}

static void dispatch_req(uint32_t nsid, int sqid, int cqid, int sq_entry, unsigned long long nsecs_start,
			     struct nvmev_result *ret, struct nvme_command *cmd) 
{
	switch(cmd->common.opcode){
		case nvme_cmd_read:
		case nvme_cmd_write:
		case nvme_cmd_zone_append:
			__enqueue_trs_to_tsu(nsid, sqid, cqid, sq_entry, nsecs_start, ret);
			break;
		case nvme_cmd_flush:
		case nvme_cmd_zone_mgmt_send:
		case nvme_cmd_zone_mgmt_recv:
			__enqueue_io_req(sqid, cqid, sq_entry, nsecs_start, ret);
			break;
		default:
			NVMEV_ERROR("%s: unimplemented command: %s(%d)\n", __func__,
					nvme_opcode_string(cmd->common.opcode), cmd->common.opcode);
			break;
	}

}



void schedule_internal_operation(int sqid, unsigned long long nsecs_target,
				 struct buffer *write_buffer, size_t buffs_to_release)
{
	struct nvmev_io_worker *worker;
	struct nvmev_io_work *w;
	unsigned int entry;

	worker = __allocate_work_queue_entry(sqid, &entry);
	if (!worker)
		return;

	w = worker->work_queue + entry;
	NVMEV_DEBUG_VERBOSE("%s/%u, internal sq %d, %llu + %llu\n", worker->thread_name, entry, sqid,
		    local_clock(), nsecs_target - local_clock());

	/////////////////////////////////
	w->sqid = sqid;
	w->nsecs_start = w->nsecs_enqueue = local_clock();
	w->nsecs_target = nsecs_target;
	w->is_completed = false;
	w->is_copied = true;
	w->prev = -1;
	w->next = -1;

	w->is_internal = true;
	w->write_buffer = write_buffer;
	w->buffs_to_release = buffs_to_release;
	mb(); /* IO worker shall see the updated w at once */

	__insert_req_sorted(entry, worker, nsecs_target);
}


/*
	Reclaim completed result from tsu
*/
static void __reclaim_completed_ret_in_tsu(void)
{
	struct nvmev_result_tsu *ret_entry, *tmp;
	list_for_each_entry_safe(ret_entry, tmp, &nvmev_vdev->nvmev_tsu->ret_queue, list) {
		struct nvmev_tsu_tr *tr_entry;
		bool all_tr_completed = true;

		list_for_each_entry(tr_entry, &ret_entry->transactions, list) {
			if(tr_entry->is_completed == true && tr_entry->is_reclaim_by_ret == false){
				ret_entry->nsecs_target = max(ret_entry->nsecs_target, tr_entry->nsecs_target);
				tr_entry->is_reclaim_by_ret = true;
			}else{
				if((tr_entry->is_completed == true && tr_entry->is_reclaim_by_ret == false)
					|| (tr_entry->is_completed == false && tr_entry->is_reclaim_by_ret == false))
				{
					all_tr_completed = false;
				}
			}
		}

		/*If all transactions completed, remove ret from ret_queue*/
		if(all_tr_completed  || !ret_entry->has_transactions){
			struct nvmev_result ret = {
				.nsecs_target = ret_entry->nsecs_target,
				.status = ret_entry->status,
				.transactions = LIST_HEAD_INIT(ret.transactions),
			};
			__enqueue_io_req(ret_entry->sqid, ret_entry->cqid, ret_entry->sq_entry, ret_entry->nsecs_start, &ret);

			// Delete from ret_queue
			spin_lock(&nvmev_vdev->nvmev_tsu->ret_lock);
			list_del(&ret_entry->list);
			spin_unlock(&nvmev_vdev->nvmev_tsu->ret_lock);

			NVMEV_DEBUG("BIN:  delete ret from ret_queue, sqid: %d cqid: %d sq_entry: %d\n", 
					ret_entry->sqid, ret_entry->cqid, ret_entry->sq_entry);
		}
	}
}

/*
	Reclaim completed transactions from chip queue
*/
static void __reclaim_completed_transactions(void)
{
	struct nvmev_transaction_queue* chip_queue;
	struct nvmev_tsu_tr* tr;
	int nchs = nvmev_vdev->nvmev_tsu->nchs;
	int nchips = nvmev_vdev->nvmev_tsu->nchips;
	unsigned int i, j;
	for(i=0;i<nchs;i++){
		for(j=0;j<nchips;j++){
			unsigned int first_entry = -1;
			unsigned int last_entry = -1;
			unsigned int curr;
			int nr_reclaimed = 0;
			chip_queue = &nvmev_vdev->nvmev_tsu->chip_queue[i][j];
			
			first_entry = chip_queue->io_seq;
			curr = first_entry;
			while( curr != -1 ){
				tr = &chip_queue->queue[curr];
				if(tr->is_completed == true && tr->is_reclaim_by_ret == true){
					last_entry = curr;
					curr = tr->next;
					nr_reclaimed++;
				} else{
					break;
				}
			}

			if (last_entry != -1) {
				spin_lock(&chip_queue->tr_lock);
				tr = &chip_queue->queue[last_entry];
				chip_queue->io_seq = tr->next;
				if (tr->next != -1) {
					chip_queue->queue[tr->next].prev = -1;
				}
				tr->next = -1;

				tr = &chip_queue->queue[first_entry];
				tr->prev = chip_queue->free_seq_end;

				tr = &chip_queue->queue[chip_queue->free_seq_end];
				tr->next = first_entry;
				chip_queue->free_seq_end = last_entry;

				chip_queue->nr_trs_in_fly -= nr_reclaimed;

				spin_unlock(&chip_queue->tr_lock);
				NVMEV_DEBUG("%s: recliam from chip_queue in channel-%d die-%d %u -- %u, reclaimed %d remain %u\n", __func__,
						i, j, first_entry, last_entry, nr_reclaimed, chip_queue->nr_trs_in_fly);
			}
		}
	}
}

static void __reclaim_completed_reqs(void)
{
	unsigned int turn;

	for (turn = 0; turn < nvmev_vdev->config.nr_io_workers; turn++) {
		struct nvmev_io_worker *worker;
		struct nvmev_io_work *w;

		unsigned int first_entry = -1;
		unsigned int last_entry = -1;
		unsigned int curr;
		int nr_reclaimed = 0;

		worker = &nvmev_vdev->io_workers[turn];

		first_entry = worker->io_seq;
		curr = first_entry;

		while (curr != -1) {
			w = &worker->work_queue[curr];
			if (w->is_completed == true && w->is_copied == true &&
			    w->nsecs_target <= worker->latest_nsecs) {
				last_entry = curr;
				curr = w->next;
				nr_reclaimed++;
			} else {
				break;
			}
		}

		if (last_entry != -1) {
			// spin_lock(&worker->entry_lock);
			w = &worker->work_queue[last_entry];
			worker->io_seq = w->next;
			if (w->next != -1) {
				worker->work_queue[w->next].prev = -1;
			}
			w->next = -1;

			w = &worker->work_queue[first_entry];
			w->prev = worker->free_seq_end;

			w = &worker->work_queue[worker->free_seq_end];
			w->next = first_entry;

			worker->free_seq_end = last_entry;
			// spin_unlock(&worker->entry_lock);
			NVMEV_DEBUG_VERBOSE("%s: %u -- %u, %d\n", __func__,
					first_entry, last_entry, nr_reclaimed);
		}
	}
}

static size_t __nvmev_proc_io(int sqid, int sq_entry, size_t *io_size)
{
	struct nvmev_submission_queue *sq = nvmev_vdev->sqes[sqid];
	unsigned long long nsecs_start = __get_wallclock();
	struct nvme_command *cmd = &sq_entry(sq_entry);
#if (BASE_SSD == KV_PROTOTYPE)
	uint32_t nsid = 0; // Some KVSSD programs give 0 as nsid for KV IO
#else
	uint32_t nsid = cmd->common.nsid - 1;
#endif
	struct nvmev_ns *ns = &nvmev_vdev->ns[nsid];

	struct nvmev_request req = {
		.cmd = cmd,
		.sq_id = sqid,
		.sq_entry = sq_entry,
		.nsecs_start = nsecs_start,
	};
	struct nvmev_result ret = {
		.nsecs_target = nsecs_start,
		.status = NVME_SC_SUCCESS,
		.transactions = LIST_HEAD_INIT(ret.transactions),
	};

#ifdef PERF_DEBUG
	unsigned long long prev_clock = local_clock();
	unsigned long long prev_clock2 = 0;
	unsigned long long prev_clock3 = 0;
	unsigned long long prev_clock4 = 0;
	static unsigned long long clock1 = 0;
	static unsigned long long clock2 = 0;
	static unsigned long long clock3 = 0;
	static unsigned long long counter = 0;
#endif

	if (!ns->proc_io_cmd(ns, &req, &ret))
		return false;
	*io_size = (cmd->rw.length + 1) << 9;

	
#ifdef PERF_DEBUG
	prev_clock2 = local_clock();
#endif
	NVMEV_DEBUG("sq_entry %d, nsecs_start %lld nsecs_target %lld\n", sq_entry, nsecs_start, ret.nsecs_target);
	// dispatch_req(nsid, sqid, sq->cqid, sq_entry, nsecs_start, &ret, cmd);
	__enqueue_trs_to_tsu(nsid, sqid, sq->cqid, sq_entry, nsecs_start, &ret);
#ifdef PERF_DEBUG
	prev_clock3 = local_clock();
#endif

	// __reclaim_completed_reqs();

#ifdef PERF_DEBUG
	prev_clock4 = local_clock();

	clock1 += (prev_clock2 - prev_clock);
	clock2 += (prev_clock3 - prev_clock2);
	clock3 += (prev_clock4 - prev_clock3);
	counter++;

	if (counter > 1000) {
		NVMEV_DEBUG("LAT: %llu, ENQ: %llu, CLN: %llu\n", clock1 / counter, clock2 / counter,
			    clock3 / counter);
		clock1 = 0;
		clock2 = 0;
		clock3 = 0;
		counter = 0;
	}
#endif
	return true;
}

int nvmev_proc_io_sq(int sqid, int new_db, int old_db)
{
	struct nvmev_submission_queue *sq = nvmev_vdev->sqes[sqid];
	int num_proc = new_db - old_db;
	int seq;
	int sq_entry = old_db;
	int latest_db;
	// int turn = sq->loop_turn;
	// sq->loop_turn = (turn + 1) % sq->queue_size;

	if (unlikely(!sq))
		return old_db;
	if (unlikely(num_proc < 0))
		num_proc += sq->queue_size;

	// if(num_proc < 20 && turn < 200) return old_db;
	// sq->loop_turn = 0;
	// NVMEV_INFO("sqid: %d, received %d requests\n", sqid, num_proc);
	for (seq = 0; seq < num_proc; seq++) {
		size_t io_size;
		if (!__nvmev_proc_io(sqid, sq_entry, &io_size))
			break;

		if (++sq_entry == sq->queue_size) {
			sq_entry = 0;
		}
		sq->stat.nr_dispatched++;
		sq->stat.nr_in_flight++;
		sq->stat.total_io += io_size;
	}
	sq->stat.nr_dispatch++;
	sq->stat.max_nr_in_flight = max_t(int, sq->stat.max_nr_in_flight, sq->stat.nr_in_flight);

	latest_db = (old_db + seq) % sq->queue_size;
	return latest_db;
}

void nvmev_proc_io_cq(int cqid, int new_db, int old_db)
{
	struct nvmev_completion_queue *cq = nvmev_vdev->cqes[cqid];
	int i;
	for (i = old_db; i != new_db; i++) {
		int sqid = cq_entry(i).sq_id;
		if (i >= cq->queue_size) {
			i = -1;
			continue;
		}

		/* Should check the validity here since SPDK deletes SQ immediately
		 * before processing associated CQes */
		if (!nvmev_vdev->sqes[sqid]) continue;

		nvmev_vdev->sqes[sqid]->stat.nr_in_flight--;
	}

	cq->cq_tail = new_db - 1;
	if (new_db == -1)
		cq->cq_tail = cq->queue_size - 1;
}

static void __fill_cq_result(struct nvmev_io_work *w)
{
	int sqid = w->sqid;
	int cqid = w->cqid;
	int sq_entry = w->sq_entry;
	unsigned int command_id = w->command_id;
	unsigned int status = w->status;
	unsigned int result0 = w->result0;
	unsigned int result1 = w->result1;

	struct nvmev_completion_queue *cq = nvmev_vdev->cqes[cqid];
	int cq_head = cq->cq_head;
	struct nvme_completion *cqe = &cq_entry(cq_head);

	spin_lock(&cq->entry_lock);
	cqe->command_id = command_id;
	cqe->sq_id = sqid;
	cqe->sq_head = sq_entry;
	cqe->status = cq->phase | (status << 1);
	cqe->result0 = result0;
	cqe->result1 = result1;

	if (++cq_head == cq->queue_size) {
		cq_head = 0;
		cq->phase = !cq->phase;
	}

	cq->cq_head = cq_head;
	cq->interrupt_ready = true;
	spin_unlock(&cq->entry_lock);
}

static int nvmev_io_worker(void *data)
{
	struct nvmev_io_worker *worker = (struct nvmev_io_worker *)data;
	struct nvmev_ns *ns;

#ifdef PERF_DEBUG
	static unsigned long long intr_clock[NR_MAX_IO_QUEUE + 1];
	static unsigned long long intr_counter[NR_MAX_IO_QUEUE + 1];

	unsigned long long prev_clock;
#endif

	NVMEV_INFO("%s started on cpu %d (node %d)\n", worker->thread_name, smp_processor_id(),
		   cpu_to_node(smp_processor_id()));

	while (!kthread_should_stop()) {
		unsigned long long curr_nsecs_wall = __get_wallclock();
		unsigned long long curr_nsecs_local = local_clock();
		long long delta = curr_nsecs_wall - curr_nsecs_local;

		volatile unsigned int curr = worker->io_seq;
		int qidx;

		while (curr != -1) {
			struct nvmev_io_work *w = &worker->work_queue[curr];
			unsigned long long curr_nsecs = local_clock() + delta;
			// unsigned long long curr_nsecs =  __get_wallclock();
			
			// spin_lock_irq(&worker->entry_lock);
			worker->latest_nsecs = curr_nsecs;
			// spin_unlock_irq(&worker->entry_lock);

			if (w->is_completed == true) {
				curr = w->next;
				continue;
			}

			if (w->is_copied == false) {
#ifdef PERF_DEBUG
				w->nsecs_copy_start = local_clock() + delta;
#endif
				if (w->is_internal) {
					;
				} else if (io_using_dma) {
					__do_perform_io_using_dma(w->sqid, w->sq_entry);
				} else {
#if (BASE_SSD == KV_PROTOTYPE)
					struct nvmev_submission_queue *sq =
						nvmev_vdev->sqes[w->sqid];
					ns = &nvmev_vdev->ns[0];
					if (ns->identify_io_cmd(ns, sq_entry(w->sq_entry))) {
						w->result0 = ns->perform_io_cmd(
							ns, &sq_entry(w->sq_entry), &(w->status));
					} else {
						__do_perform_io(w->sqid, w->sq_entry);
					}
#endif
					__do_perform_io(w->sqid, w->sq_entry);
				}

#ifdef PERF_DEBUG
				w->nsecs_copy_done = local_clock() + delta;
#endif
				// spin_lock_irq(&worker->entry_lock);
				w->is_copied = true;
				// spin_unlock_irq(&worker->entry_lock);

				NVMEV_DEBUG_VERBOSE("%s: copied %u, %d %d %d\n", worker->thread_name, curr,
					    w->sqid, w->cqid, w->sq_entry);
			}

			if (w->nsecs_target <= curr_nsecs) {
				if (w->is_internal) {
#if (SUPPORTED_SSD_TYPE(CONV) || SUPPORTED_SSD_TYPE(ZNS))
					NVMEV_DEBUG("inernal buffer_release, %d %d %d\n",  w->sqid, w->cqid, w->sq_entry);
					buffer_release((struct buffer *)w->write_buffer,
						       w->buffs_to_release);
#endif
				} else {
					NVMEV_DEBUG("__fill_cq_result %d %d %d\n", w->sqid, w->cqid, w->sq_entry);
					__fill_cq_result(w);
				}

				NVMEV_DEBUG_VERBOSE("%s: completed %u, %d %d %d\n", worker->thread_name, curr,
					    w->sqid, w->cqid, w->sq_entry);

#ifdef PERF_DEBUG
				w->nsecs_cq_filled = local_clock() + delta;
				trace_printk("%llu %llu %llu %llu %llu %llu\n", w->nsecs_start,
					     w->nsecs_enqueue - w->nsecs_start,
					     w->nsecs_copy_start - w->nsecs_start,
					     w->nsecs_copy_done - w->nsecs_start,
					     w->nsecs_cq_filled - w->nsecs_start,
					     w->nsecs_target - w->nsecs_start);
#endif
				mb(); /* Reclaimer shall see after here */
				// spin_lock_irq(&worker->entry_lock);
				w->is_completed = true;
				// spin_unlock_irq(&worker->entry_lock);
			}

			curr = w->next;
		}

		for (qidx = 1; qidx <= nvmev_vdev->nr_cq; qidx++) {
			struct nvmev_completion_queue *cq = nvmev_vdev->cqes[qidx];

#ifdef CONFIG_NVMEV_IO_WORKER_BY_SQ
			if ((worker->id) != __get_io_worker(qidx))
				continue;
#endif
			if (cq == NULL || !cq->irq_enabled)
				continue;

			if (spin_trylock(&cq->irq_lock)) {
				if (cq->interrupt_ready == true) {
#ifdef PERF_DEBUG
					prev_clock = local_clock();
#endif
					cq->interrupt_ready = false;
					nvmev_signal_irq(cq->irq_vector);

#ifdef PERF_DEBUG
					intr_clock[qidx] += (local_clock() - prev_clock);
					intr_counter[qidx]++;

					if (intr_counter[qidx] > 1000) {
						NVMEV_DEBUG("Intr %d: %llu\n", qidx,
							    intr_clock[qidx] / intr_counter[qidx]);
						intr_clock[qidx] = 0;
						intr_counter[qidx] = 0;
					}
#endif
				}
				spin_unlock(&cq->irq_lock);
			}
		}
		cond_resched();
	}

	return 0;
}

void NVMEV_IO_WORKER_INIT(struct nvmev_dev *nvmev_vdev)
{
	unsigned int i, worker_id;

	nvmev_vdev->io_workers =
		kcalloc(sizeof(struct nvmev_io_worker), nvmev_vdev->config.nr_io_workers, GFP_KERNEL);
	nvmev_vdev->io_worker_turn = 0;

	for (worker_id = 0; worker_id < nvmev_vdev->config.nr_io_workers; worker_id++) {
		struct nvmev_io_worker *worker = &nvmev_vdev->io_workers[worker_id];

		worker->work_queue =
			kzalloc(sizeof(struct nvmev_io_work) * NR_MAX_PARALLEL_IO, GFP_KERNEL);
		for (i = 0; i < NR_MAX_PARALLEL_IO; i++) {
			worker->work_queue[i].next = i + 1;
			worker->work_queue[i].prev = i - 1;
		}
		spin_lock_init(&worker->entry_lock);
		worker->work_queue[NR_MAX_PARALLEL_IO - 1].next = -1;
		worker->id = worker_id;
		worker->free_seq = 0;
		worker->free_seq_end = NR_MAX_PARALLEL_IO - 1;
		worker->io_seq = -1;
		worker->io_seq_end = -1;

		snprintf(worker->thread_name, sizeof(worker->thread_name), "nvmev_io_worker_%d", worker_id);

		worker->task_struct = kthread_create(nvmev_io_worker, worker, "%s", worker->thread_name);

		kthread_bind(worker->task_struct, nvmev_vdev->config.cpu_nr_io_workers[worker_id]);
		wake_up_process(worker->task_struct);
	}
}

void NVMEV_IO_WORKER_FINAL(struct nvmev_dev *nvmev_vdev)
{
	unsigned int i;

	for (i = 0; i < nvmev_vdev->config.nr_io_workers; i++) {
		struct nvmev_io_worker *worker = &nvmev_vdev->io_workers[i];

		if (!IS_ERR_OR_NULL(worker->task_struct)) {
			kthread_stop(worker->task_struct);
		}

		kfree(worker->work_queue);
	}

	kfree(nvmev_vdev->io_workers);
}

static int nvmev_tsu(void *data){
	struct nvmev_tsu *tsu=(struct nvmev_tsu *)data;

	NVMEV_INFO("nvmev_tsu started on cpu %d (node %d)\n",
		   nvmev_vdev->config.cpu_nr_tsu,
		   cpu_to_node(nvmev_vdev->config.cpu_nr_tsu));
	
	while (!kthread_should_stop()) {
		tsu->schedule(tsu);

		__reclaim_completed_transactions();
		__reclaim_completed_ret_in_tsu();
		__reclaim_completed_reqs();
		cond_resched();
	}

	return 0;
};

void NVMEV_TSU_INIT(struct nvmev_dev *nvmev_vdev) {
    int nchs = NAND_CHANNELS;
    int nchips = CHIPS_PER_NAND_CH;
	unsigned int i, j, k;

	nvmev_vdev->nvmev_tsu = kcalloc(sizeof(struct nvmev_tsu), 1, GFP_KERNEL);
	*(nvmev_vdev->nvmev_tsu) = (struct nvmev_tsu){
		.nchs = nchs,
		.nchips = nchips,
		.ret_queue = LIST_HEAD_INIT(nvmev_vdev->nvmev_tsu->ret_queue),
	};
	spin_lock_init(&nvmev_vdev->nvmev_tsu->ret_lock);
	
	// Initialize chip queues for tsu.
	nvmev_vdev->nvmev_tsu->chip_queue = kzalloc(sizeof(struct nvmev_transaction_queue*) * nchs, GFP_KERNEL);
	for( i = 0; i < nchs; i++){
		nvmev_vdev->nvmev_tsu->chip_queue[i] = 
			kzalloc(sizeof(struct nvmev_transaction_queue) * nchips, GFP_KERNEL);	
		for( j = 0; j < nchips; j++){
			nvmev_vdev->nvmev_tsu->chip_queue[i][j].queue = kzalloc(sizeof(struct nvmev_tsu_tr) * NR_MAX_CHIP_IO, GFP_KERNEL);
			for( k = 0; k < NR_MAX_CHIP_IO; k++){
				nvmev_vdev->nvmev_tsu->chip_queue[i][j].queue[k].next = k+1;
				nvmev_vdev->nvmev_tsu->chip_queue[i][j].queue[k].prev = k-1;
			}
			nvmev_vdev->nvmev_tsu->chip_queue[i][j].queue[NR_MAX_CHIP_IO-1].next = -1;
			
			spin_lock_init(&nvmev_vdev->nvmev_tsu->chip_queue[i][j].tr_lock);
			nvmev_vdev->nvmev_tsu->chip_queue[i][j].nr_trs_in_fly = 0;
			nvmev_vdev->nvmev_tsu->chip_queue[i][j].nr_luns = LUNS_PER_CHIP;
			nvmev_vdev->nvmev_tsu->chip_queue[i][j].nr_packages = 0;
			nvmev_vdev->nvmev_tsu->chip_queue[i][j].free_seq = 0;
			nvmev_vdev->nvmev_tsu->chip_queue[i][j].free_seq_end = NR_MAX_CHIP_IO-1;
			nvmev_vdev->nvmev_tsu->chip_queue[i][j].io_seq = -1;
			nvmev_vdev->nvmev_tsu->chip_queue[i][j].io_seq_end = -1;
			INIT_LIST_HEAD(&nvmev_vdev->nvmev_tsu->chip_queue[i][j].transaction_package_list);
		}
	}

	// Initilize schedule method for tsu.
	nvmev_vdev->nvmev_tsu->schedule = schedule_fairness;
	// nvmev_vdev->nvmev_tsu->schedule = schedule_fifo;
	nvmev_vdev->nvmev_tsu->task_struct = kthread_create(nvmev_tsu, nvmev_vdev->nvmev_tsu, "nvmev_tsu");
	if (nvmev_vdev->config.cpu_nr_dispatcher != -1)
		kthread_bind(nvmev_vdev->nvmev_tsu->task_struct, nvmev_vdev->config.cpu_nr_tsu);
	wake_up_process(nvmev_vdev->nvmev_tsu->task_struct);
}

void NVMEV_TSU_FINAL(struct nvmev_dev *nvmev_vdev){
	unsigned int i,j;
	int nchs = NAND_CHANNELS;
    int nchips = CHIPS_PER_NAND_CH;

	if (!IS_ERR_OR_NULL(nvmev_vdev->nvmev_tsu)) {
		kthread_stop(nvmev_vdev->nvmev_tsu->task_struct);
	}

	for(i=0;i<nchs;i++){
		for(j=0;j<nchips;j++){
			kfree(nvmev_vdev->nvmev_tsu->chip_queue[i][j].queue);
		}
		kfree(nvmev_vdev->nvmev_tsu->chip_queue[i]);
	}
	kfree(nvmev_vdev->nvmev_tsu->chip_queue);
	kfree(nvmev_vdev->nvmev_tsu);
}