/*
 * devinfo: List RDMA devices, their lists, ports, and GID tables through
 * libibverbs. Program 1 of rdma_avo Phase 1.
 *
 * Usage: devinfo
 */

#include <arpa/inet.h>
#include <endian.h>
#include <inttypes.h>
#include <net/if.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <infiniband/verbs.h>

static const char *link_layer_str(uint8_t link_layer)
{
	switch (link_layer) {
	case IBV_LINK_LAYER_INFINIBAND:
		return "InfiniBand";
	case IBV_LINK_LAYER_ETHERNET:
		return "Ethernet";
	default:
		return "Unspecified";
	}
}

static const char *gid_type_str(uint32_t gid_type)
{
	switch (gid_type) {
	case IBV_GID_TYPE_IB:
		return "IB";
	case IBV_GID_TYPE_ROCE_V1:
		return "RoCE v1";
	case IBV_GID_TYPE_ROCE_V2:
		return "RoCE v2";
	default:
		return "unknown";
	}
}

static const char *atomic_cap_str(enum ibv_atomic_cap cap)
{
	switch (cap) {
	case IBV_ATOMIC_NONE:
		return "none";
	case IBV_ATOMIC_HCA:
		return "HCA (atomic within this device)";
	case IBV_ATOMIC_GLOB:
		return "glocal (atomic with CPU and other devices)";
	default:
		return "unknown";
	}
}

/* IBV_MTU_256 is 1 through IBV_MTU_4096 is 5, so bytes = 128 << mtu. */
static int mtu_bytes(enum ibv_mtu mtu)
{
	return 128 << mtu;
}

/* GUIDs are stored big-endian; print them the way ibv_defin does. */
static void print_guid(const char *label, __be64 guid_be)
{
	uint64_t g = be64toh(guid_be);

	printf("  %-18s %04x:%04x:%04x:%04x\n", label,
			(unsigned int)((g >> 48) & 0xffff),
			(unsigned int)((g >> 32) & 0xffff),
			(unsigned int)((g >> 16) & 0xffff),
			(unsigned int)(g & 0xffff));
}

static void print_gid_table(struct ibv_context *ctx, size_t max_entries) 
{
	struct ibv_gid_entry *entries;
	ssize_t n;

	entries = calloc(max_entries, sizeof(*entries));
	if (!entries) {
		perror("calloc");
		return;
	}

	/* Returns the number of valid entries, or a negative error. */
	n = ibv_query_gid_table(ctx, entries, max_entries, 0);
	if (n < 0) {
		fprintf(stderr, "ibv_query_gid_table: %s\n", strerror((int)-n));
		free(entries);
		return;
	}

	printf("  GID table (%zd valid entries):\n", n);
	for (ssize_t i = 0; i < n; i++) {
		const struct ibv_gid_entry *e = &entries[i];
		char gid_str[INET6_ADDRSTRLEN];
		char ifname[IF_NAMESIZE] = "-";

		/* A GID is 16 bytes laid out like an IPv6 address. */
		if (!inet_ntop(AF_INET6, e->gid.raw, gid_str, sizeof(gid_str)))
			strcpy(gid_str, "?");
		if (e->ndev_ifindex && !if_indextoname(e->ndev_ifindex, ifname))
			strcpy(ifname, "?");

		printf("    port %" PRIu32 " index %-3" PRIu32 " %-40s %-8s %s\n",
				e->port_num, e->gid_index, gid_str,
				gid_type_str(e->gid_type), ifname);
	}
	
	free(entries);
}

static int print_device(struct ibv_device *dev)
{
	struct ibv_context *ctx;
	struct ibv_device_attr dattr;
	size_t gid_capacity = 0;
	int ret;

	ctx = ibv_open_device(dev);
	if (!ctx) {
		fprintf(stderr, "ibv_open_device(%s) failed \n",
				ibv_get_device_name(dev));
		return -1;
	}

	/* Verbs query calls return 0 or a positive errno value. */
	ret = ibv_query_device(ctx, &dattr);
	if (ret) {
		fprintf(stderr, "ibv_query_device: %s\n", strerror(ret));
		ibv_close_device(ctx);
		return -1;
	}

	printf("device %s\n", ibv_get_device_name(dev));
	print_guid("node_guid", dattr.node_guid);
	print_guid("sys_image_guid", dattr.sys_image_guid);
	printf("  %-18s %s\n", "fw_ver", dattr.fw_ver[0] ? dattr.fw_ver : "-");
	printf("  %-18s 0x%" PRIx32 "\n", "vendor_id", dattr.vendor_id);
	printf("  %-18s %d\n", "max_pd", dattr.max_pd);							/* max protection domain */
	printf("  %-18s %d\n", "max_qp", dattr.max_qp);						/* max queue pair */
	printf("  %-18s %d\n", "max_qp_wr", dattr.max_qp_wr);					/* max work request */
	printf("  %-18s %d\n", "max_sge", dattr.max_sge);						/* max scatter/gather entries */
	printf("  %-18s %d\n", "max_cq", dattr.max_cq);							/* max completion queue */
	printf("  %-18s %d\n", "max_cqe", dattr.max_cqe);						/* max cq entries */
	printf("  %-18s %d\n",  "max_mr", dattr.max_mr);						/* max memory regions */
	printf("  %-18s %" PRIu64 "\n", "max_mr_size", dattr.max_mr_size);		/* max mr size */
	printf("  %-18s %s\n", "atomic_cap", atomic_cap_str(dattr.atomic_cap)); /* remote atomic capabilitys */
	printf("  %-18s %u\n", "phys_port_cnt", dattr.phys_port_cnt);

	/* Ports are numbered from 1. */
		for (unsigned int p = 1; p <= dattr.phys_port_cnt; p++) {
			struct ibv_port_attr pattr;

			ret = ibv_query_port(ctx, (uint8_t)p, &pattr);
			if (ret) {
				fprintf(stderr, "ibv_query_port(%u): %s\n", p, strerror(ret));
				continue;
			}

			printf("  port %u\n", p);
			printf("    %-16s %s\n", "state", ibv_port_state_str(pattr.state));
			printf("    %-16s %d\n", "max_mtu", mtu_bytes(pattr.max_mtu));
			printf("    %-16s %d\n", "active_mtu", mtu_bytes(pattr.active_mtu));
			printf("    %-16s %s\n", "link_layer", link_layer_str(pattr.link_layer));
			printf("    %-16s %u\n", "lid", pattr.lid);
			printf("    %-16s %d\n", "gid_tbl_len", pattr.gid_tbl_len);

			if (pattr.gid_tbl_len > 0)
			gid_capacity += (size_t)pattr.gid_tbl_len;
	}
	
	if (gid_capacity > 0)
		print_gid_table(ctx, gid_capacity);

	ibv_close_device(ctx);
	return 0;
}

int main(void) 
{
	struct ibv_device **list;
	int num;
	int status = EXIT_SUCCESS;

	list = ibv_get_device_list(&num);
	if (!list) {
		perror("ibv_get_device_list");
		return EXIT_FAILURE;
	}
	if (num == 0) {
		fprintf(stderr, "no RDMA devices found\n");
		ibv_free_device_list(list);
		return EXIT_FAILURE;
	}

	for(int i = 0; i < num; i++) {
		if (print_device(list[i]))
			status = EXIT_FAILURE;
	}

	ibv_free_device_list(list);
	return status;
}
