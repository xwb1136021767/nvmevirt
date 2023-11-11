#include "tsu_fifo.h"

void schedule_fifo(struct nvmev_tsu* tsu){
    unsigned int i,j;
    for(i=0;i<tsu->nchs;i++){
        for(j=0;j<tsu->dies_per_ch;j++){
            struct nvmev_transaction_queue* chip_queue = &tsu->chip_queue[i][j];
            volatile unsigned int curr = chip_queue->io_seq;
            volatile unsigned int end = chip_queue->io_seq_end;
    
            while(curr != -1){
                struct nvmev_tsu_tr* tr = &chip_queue->queue[curr];
                struct nvmev_ns *ns = &nvmev_vdev->ns[tr->nsid];
                if(tr->is_completed){
                    curr = tr->next;
                    continue;
                }

                NVMEV_DEBUG("BIN:  process tr from ch: %d lun: %d zid: %lld, curr = %lld ,io_seq=%lld, io_seq_end=%lld\n", 
                        i, j, tr->zid, curr, chip_queue->io_seq, chip_queue->io_seq_end);
                if (!ns->proc_tr_cmd(ns, tr)){
                    return;
                }
                
                mb();
                tr->is_completed = true;

                curr = tr->next;
            }
        }
    }
}
