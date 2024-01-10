// SPDX-License-Identifier: GPL-2.0-only

#include <linux/ktime.h>
#include <linux/sched/clock.h>

#include "nvmev.h"
#include "ssd.h"
#include "zns_ftl.h"

static void __init_descriptor(struct zns_ftl *zns_ftl)
{
	struct zone_descriptor *zone_descs;
	uint32_t zone_size = zns_ftl->zp.zone_size;
	uint32_t nr_zones = zns_ftl->zp.nr_zones;
	uint64_t zslba = 0;
	uint32_t i = 0;
	const uint32_t zrwa_buffer_size = zns_ftl->zp.zrwa_buffer_size;
	const uint32_t zone_wb_size = zns_ftl->zp.zone_wb_size;

	zns_ftl->zone_descs = kmalloc(sizeof(struct zone_descriptor) * nr_zones, GFP_KERNEL);
	zns_ftl->report_buffer = kmalloc(
		sizeof(struct zone_report) + sizeof(struct zone_descriptor) * nr_zones, GFP_KERNEL);

	if (zrwa_buffer_size)
		zns_ftl->zwra_buffer = kmalloc(sizeof(struct buffer) * nr_zones, GFP_KERNEL);

	if (zone_wb_size)
		zns_ftl->zone_write_buffer = kmalloc(sizeof(struct buffer) * nr_zones, GFP_KERNEL);

	zone_descs = zns_ftl->zone_descs;
	memset(zone_descs, 0, sizeof(struct zone_descriptor) * zns_ftl->zp.nr_zones);

	for (i = 0; i < nr_zones; i++) {
		zone_descs[i].state = ZONE_STATE_EMPTY;
		zone_descs[i].type = ZONE_TYPE_SEQ_WRITE_REQUIRED;

		zone_descs[i].zslba = zslba;
		zone_descs[i].wp = zslba;
		zslba += BYTE_TO_LBA(zone_size);
		zone_descs[i].zone_capacity = BYTE_TO_LBA(zone_size);

		if (zrwa_buffer_size)
			buffer_init(&(zns_ftl->zwra_buffer[i]), zrwa_buffer_size);

		if (zone_wb_size)
			buffer_init(&(zns_ftl->zone_write_buffer[i]), zone_wb_size);

		NVMEV_ZNS_DEBUG("[i] zslba 0x%llx zone capacity 0x%llx\n", zone_descs[i].zslba,
				zone_descs[i].zone_capacity);
	}
}

static void __remove_descriptor(struct zns_ftl *zns_ftl)
{
	if (zns_ftl->zp.zrwa_buffer_size)
		kfree(zns_ftl->zwra_buffer);

	if (zns_ftl->zp.zone_wb_size)
		kfree(zns_ftl->zone_write_buffer);

	kfree(zns_ftl->report_buffer);
	kfree(zns_ftl->zone_descs);
}

static void __init_resource(struct zns_ftl *zns_ftl)
{
	struct zone_resource_info *res_infos = zns_ftl->res_infos;

	res_infos[ACTIVE_ZONE] = (struct zone_resource_info){
		.total_cnt = zns_ftl->zp.nr_zones,
		.acquired_cnt = 0,
	};

	res_infos[OPEN_ZONE] = (struct zone_resource_info){
		.total_cnt = zns_ftl->zp.nr_zones,
		.acquired_cnt = 0,
	};

	res_infos[ZRWA_ZONE] = (struct zone_resource_info){
		.total_cnt = zns_ftl->zp.nr_zones,
		.acquired_cnt = 0,
	};
}

static void zns_init_params(struct znsparams *zpp, struct ssdparams *spp, uint64_t capacity)
{
	*zpp = (struct znsparams){
		.zone_size = ZONE_SIZE,
		.nr_zones = capacity / ZONE_SIZE,
		.dies_per_zone = DIES_PER_ZONE,
		.nr_active_zones = zpp->nr_zones, // max
		.nr_open_zones = zpp->nr_zones, // max
		.nr_zrwa_zones = MAX_ZRWA_ZONES,
		.zone_wb_size = ZONE_WB_SIZE,
		.zrwa_size = ZRWA_SIZE,
		.zrwafg_size = ZRWAFG_SIZE,
		.zrwa_buffer_size = ZRWA_BUFFER_SIZE,
		.lbas_per_zrwa = zpp->zrwa_size / spp->secsz,
		.lbas_per_zrwafg = zpp->zrwafg_size / spp->secsz,
	};

	NVMEV_ASSERT((capacity % zpp->zone_size) == 0);
	/* It should be 4KB aligned, according to lpn size */
	NVMEV_ASSERT((zpp->zone_size % spp->pgsz) == 0);

	NVMEV_INFO("zone_size=%u(Byte),%u(MB), # zones=%d # die/zone=%d \n", zpp->zone_size,
		   BYTE_TO_MB(zpp->zone_size), zpp->nr_zones, zpp->dies_per_zone);
}

static void zns_init_ftl(struct zns_ftl *zns_ftl, struct znsparams *zpp, struct ssd *ssd,
			 void *mapped_addr)
{
	*zns_ftl = (struct zns_ftl){
		.zp = *zpp, /*copy znsparams*/

		.ssd = ssd,
		.storage_base_addr = mapped_addr,
	};

	__init_descriptor(zns_ftl);
	__init_resource(zns_ftl);
}

static void display_zone_to_die(struct zns_ftl *zns_ftl) {
	uint32_t zid = 0;
	uint32_t die;
	uint64_t slpn;
	uint32_t channel, chip, lun;
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	struct znsparams *zpp = &zns_ftl->zp;
	NVMEV_INFO("=================ZNS================");
	for(; zid < zns_ftl->zp.nr_zones ; zid++){
		die = (zid * zpp->dies_per_zone) % spp->tt_luns;
		slpn = zone_to_slpn(zns_ftl, zid);
		channel = die_to_channel(zns_ftl, die);
		chip = die_to_chip(zns_ftl, die);
		lun = die_to_lun(zns_ftl, die);
		NVMEV_INFO("zone: %d ==> die: %d slpn: %lld channel: %d chip: %d lun: %d\n", zid, die, slpn, channel, chip, lun);
	}
	NVMEV_INFO("=================ZNS================");
}

void zns_init_namespace(struct nvmev_ns *ns, uint32_t id, uint64_t size, void *mapped_addr,
			uint32_t cpu_nr_dispatcher)
{
	struct ssd *ssd;
	struct zns_ftl *zns_ftl;

	struct ssdparams spp;
	struct znsparams zpp;

	const uint32_t nr_parts = 1; /* Not support multi partitions for zns*/
	NVMEV_ASSERT(nr_parts == 1);

	ssd = kmalloc(sizeof(struct ssd), GFP_KERNEL);
	ssd_init_params(&spp, size, nr_parts);
	ssd_init(ssd, &spp, cpu_nr_dispatcher);

	zns_ftl = kmalloc(sizeof(struct zns_ftl) * nr_parts, GFP_KERNEL);
	zns_init_params(&zpp, &spp, size);
	zns_init_ftl(zns_ftl, &zpp, ssd, mapped_addr);

	*ns = (struct nvmev_ns){
		.id = id,
		.csi = NVME_CSI_ZNS,
		.nr_parts = nr_parts,
		.ftls = (void *)zns_ftl,
		.size = size,
		.mapped = mapped_addr,

		/*register io command handler*/
		.proc_io_cmd = zns_proc_nvme_io_cmd,
		.proc_tr_cmd = zns_proc_nvme_tr_cmd,
	};

	display_zone_to_die(zns_ftl);
	return;
}

void zns_remove_namespace(struct nvmev_ns *ns)
{
	struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;

	ssd_remove(zns_ftl->ssd);

	__remove_descriptor(zns_ftl);
	kfree(zns_ftl->ssd);
	kfree(zns_ftl);

	ns->ftls = NULL;
}

// Caculate max idle time of ssd, why?
static void zns_flush(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	uint64_t start, latest;
	uint32_t i;
	struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;

	start = local_clock();
	latest = start;
	for (i = 0; i < ns->nr_parts; i++) {
		latest = max(latest, ssd_next_idle_time(zns_ftl[i].ssd));
	}

	NVMEV_DEBUG("%s latency=%llu\n", __func__, latest - start);

	ret->status = NVME_SC_SUCCESS;
	ret->nsecs_target = latest;
	return;
}

bool zns_proc_nvme_tr_cmd(struct nvmev_ns *ns, struct nvmev_tsu_tr* tr)
{
	struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;
	switch (tr->cmd) {
		case NAND_WRITE:
			if(!zns_write_tr(zns_ftl, tr))
				return false;
			break;
		case NAND_READ:
			if(!zns_read_tr(zns_ftl, tr))
				return false;
			break;
		break;
	}
	return true;
}

bool zns_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct nvme_command *cmd = req->cmd;
	NVMEV_ASSERT(ns->csi == NVME_CSI_ZNS);
	/*still not support multi partitions ...*/
	NVMEV_ASSERT(ns->nr_parts == 1);

	NVMEV_DEBUG("req sqid: %d sq_entry: %d opcode: %d \n", req->sq_id, req->sq_entry, req->cmd->common.opcode);
	switch (cmd->common.opcode) {
	case nvme_cmd_write:
	case nvme_cmd_zone_append:
		if (!zns_write(ns, req, ret))
			return false;
		break;
	case nvme_cmd_read:
		if (!zns_read(ns, req, ret))
			return false;
		break;
	case nvme_cmd_flush:
		zns_flush(ns, req, ret);
		break;
	case nvme_cmd_zone_mgmt_send:
		zns_zmgmt_send(ns, req, ret);
		break;
	case nvme_cmd_zone_mgmt_recv:
		zns_zmgmt_recv(ns, req, ret);
		break;
	default:
		NVMEV_ERROR("%s: unimplemented command: %s(%d)\n", __func__,
			    nvme_opcode_string(cmd->common.opcode), cmd->common.opcode);
		break;
	}

	return true;
}
