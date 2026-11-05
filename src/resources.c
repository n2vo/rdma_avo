/*
 * resources: Create the four verbs resources an RC connection needs (PD, MR, 
 * CQ, QP), then drive the QP through RESET, INIT, RTR, and RTS, connected
 * to itself. No data moves; Program 3 adds traffic between two nodes.
 * Program 2 of rdma_avo Phase 1.
 *
 * Usage: resources [device [gid_index]]
 *   device    Defaults to the first device found.
 *   gid_index Defaults to 1, the IPv4 RoCE v2 GID on the test VMs.
 */

#include <arpa/inet.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <infiniband/verbs.h>

#define PORT_NUM	1
#define BUF_SIZE	4096
#define CQ_DEPTH	16
#define QP_DEPTH	16


static const char *qp_state_Str(enum ibv_qp_state state)
{
	switch(state) {
	case IBV_QPS_RESET:
		return "RESET";
	case IBV_QPS_INIT:
		return "INIT";
	case IBV_QPS_RTR:
		return "RTR";
	case IBV_QPS_RTS:
		return "RTS";
	case IBV_QPS_SQD:
		return "SQD";
	case IBV_QPS_SQE:
		return "SQE";
	case IBV_QPS_ERR:
		return "ERR";
	default:
		return "unknown";
	}
}

/* Ask the device which state the QP is in, rather than assuming */
static int print_qp_state(struct ibv_qp *qp)
{
	struct ibv_qp_attr attr;
	struct ibv_qp_init_attr init_attr;
	int ret;

	ret = ibv_query_qp(qp, &attr, IBV_QP_STATE, &init_attr);
	if (ret) {
		fprintf(stderr, "ibv_query_qp: %s\n", strerror(ret));
		return -1;
	}
	printf("  QP state is now %s\n", qp_state_Str(attr.qp_state));
	return 0;
}

/* RESET to INIT: Bind the QP to a port and set what remote peers may do */
static int qp_to_init(struct ibv_qp *qp)
{
	struct ibv_qp_attr attr;
	int mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
		IBV_QP_ACCESS_FLAGS;
	int ret;

	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_INIT;
	attr.pkey_index = 0;
	attr.port_num = PORT_NUM;
	attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;

	printf("RESET -> INIT\n");
	printf("  pkey_index %u, port_num %u, qp_access_flags REMOTE_WRITE|REMOTE_READ\n",
			attr.pkey_index, attr.port_num);
	ret = ibv_modify_qp(qp, &attr, mask);
	if(ret) {
		fprintf(stderr, "ibv_modify_qp(INIT): %s\n", strerror(ret));
		return -1;
	}
	return print_qp_state(qp);
}

/*
 * INIT to RTR (Ready To Receive): Tell the QP who its peer is. HEre the peer
 * is the QP itself: Its own QP number, its own GID, and its own starting PSN.
 */
static int qp_to_rtr(struct ibv_qp *qp, enum ibv_mtu mtu, uint32_t remote_qpn,
		const union ibv_gid *remote_gid, uint8_t sgid_index,
		uint32_t remote_psn)
{
	struct ibv_qp_attr attr;
	int mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
		IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
		IBV_QP_MIN_RNR_TIMER;
	char gid_str[INET6_ADDRSTRLEN];
	int ret;

	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTR;
	attr.path_mtu = mtu;
	attr.dest_qp_num = remote_qpn;
	attr.rq_psn = remote_psn;
	/* RDMA READS and atomics the peer may have outstanding against us. */
	attr.max_dest_rd_atomic = 1;
	/* Receiver Not Ready timer code 12 is 0.64 ms. */
	attr.min_rnr_timer = 12;

	/* Address vector. RoCE hasn no LIDs. so the GID routes the packet. */
	attr.ah_attr.is_global = 1;
	attr.ah_attr.grh.dgid = *remote_gid;
	attr.ah_attr.grh.sgid_index = sgid_index;
	attr.ah_attr.grh.hop_limit = 1;
	attr.ah_attr.dlid = 0;
	attr.ah_attr.sl = 0;
	attr.ah_attr.src_path_bits = 0;
	attr.ah_attr.port_num = PORT_NUM;

	if (!inet_ntop(AF_INET6, remote_gid->raw, gid_str, sizeof(gid_str)))
		strcpy(gid_str, "?");

	printf("INIT -> RTR\n");
	printf("  path_mtu %d, dest_qp_num 0x%06" PRIx32 ", rq_psn 0x%06" PRIx32 "\n",
			128 << mtu, attr.dest_qp_num, attr.rq_psn);
	printf("  max_dest_rd_atomic %u, min_rnr_timer %u\n",
			attr.max_dest_rd_atomic, attr.min_rnr_timer);
	printf("  ah_attr: dgid %s, sgid_index %u, hop_limit %u, port_num %u\n",
			gid_str, attr.ah_attr.grh.sgid_index, attr.ah_attr.grh.hop_limit,
			attr.ah_attr.port_num);

	ret = ibv_modify_qp(qp, &attr, mask);
	if (ret) {
		fprintf(stderr, "ibv_modify_qp(RTR): %s\\n", strerror(ret));
		return -1;
	}
	return print_qp_state(qp);
}

/* RTR to RTS (Ready To Send): Set the send side's PSN and retry behavior. */
static int qp_to_rts(struct ibv_qp *qp, uint32_t local_psn)
{
	struct ibv_qp_attr attr;
	int mask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
		IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
	int ret;

	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTS;
	attr.sq_psn = local_psn;
	/* Ack timeout is 4.096 us * 2^timeout; 14 is about 67 ms. */
	attr.timeout = 14;
	/* Retransmissions after an ack timeout. 7 is the maximum. */
	attr.retry_cnt = 7;
	/* Retries after Receiver Not Ready: 7 means retry forever. */
	attr.rnr_retry = 7;
	/* RDMA READs and atomics we may have outstanding against the peer. */
	attr.max_rd_atomic = 1;

	printf("RTR -> RTS\n");
	printf("  sq_psn 0x%06" PRIx32 ", timeout %u, retry_cnt %u, rnr_retry %u, max_rd_atomic %u\n",
			attr.sq_psn, attr.timeout, attr.retry_cnt, attr.rnr_retry,
			attr.max_rd_atomic);

	ret = ibv_modify_qp(qp, &attr, mask);
	if (ret) {
		fprintf(stderr, "ibv_modify_qp(RTS): %s\n", strerror(ret));
		return -1;
	}
	return print_qp_state(qp);
}

static struct ibv_device *find_device(struct ibv_device **list, int num, 
		const char *name) 
{
	if (!name)
		return num >0 ? list[0] : NULL;
	for (int i = 0; i < num; i++) {
		if (strcmp(ibv_get_device_name(list[i]), name) == 0)
			return list[i];
	}
	return NULL;
}

int main(int argc, char *argv[])
{
	const char *dev_name = argc > 1 ? argv[1] : NULL;
	int gid_index = argc > 2 ? atoi(argv[2]) : 1;
	struct ibv_device **list = NULL;
	struct ibv_device *dev;
	struct ibv_context *ctx = NULL;
	struct ibv_port_attr port_attr;
	union ibv_gid gid;
	struct ibv_pd *pd = NULL;
	void *buf = NULL;
	struct ibv_mr *mr = NULL;
	struct ibv_cq *cq = NULL;
	struct ibv_qp *qp = NULL;
	struct ibv_qp_init_attr init_attr;
	uint32_t psn;
	int num;
	int ret; 
	int status = EXIT_FAILURE;

	list = ibv_get_device_list(&num);
	if (!list) {
		perror("ibv_get_device_list");
		return EXIT_FAILURE;
	}

	dev = find_device(list, num, dev_name);
	if (!dev) {
		fprintf(stderr, "device %s not found\n", dev_name ? dev_name: "(any)");
		goto out;
	}

	ctx = ibv_open_device(dev);
	if (!ctx) {
		fprintf(stderr, "ibv_open_device failed\n");
		goto out;
	}
	printf("device %s, port %d\n", ibv_get_device_name(dev), PORT_NUM);

	ret = ibv_query_port(ctx, PORT_NUM, &port_attr);
	if (ret) {
		fprintf(stderr, "ibv_query_port: %s\n", strerror(ret));
		goto out;
	}
	if (gid_index < 0 || gid_index > 255 ||
		gid_index >= port_attr.gid_tbl_len) {
		fprintf(stderr, "gid_index %d out of range \n", gid_index);
		goto out;
	}

	ret = ibv_query_gid(ctx, PORT_NUM, gid_index, &gid);
	if (ret) {
		fprintf(stderr, "ibv_query_gid: %s\n", strerror(ret));
		goto out;
	}

	/* Protection Domain: Groups resources that may be used together. */
	pd = ibv_alloc_pd(ctx);
	if (!pd) {
		fprintf(stderr, "ibv_alloc_pd failed\n");
		goto out;
	}
	printf("PD allocated\n");

	/* Memory Reqion: Pins the buffer and gives the device its translation. */
	if (posix_memalign(&buf, (size_t)sysconf(_SC_PAGESIZE), BUF_SIZE)) {
		fprintf(stderr, "posix_memalign failed\n");
		buf = NULL;
		goto out;
	}
	memset(buf, 0, BUF_SIZE);

	mr = ibv_reg_mr(pd, buf, BUF_SIZE, 
			IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
			IBV_ACCESS_REMOTE_READ);
	if (!mr) {
		perror("ibv_reg_mr");
		goto out;
	}
	printf("MR registered: addr %p, length %zu, lkey 0x%08" PRIx32 ", rkey 0x%08" PRIx32 "\n",
			mr->addr, mr->length, mr->lkey, mr->rkey);

	/* Completion Queue: Where finished work requests are reported. */
	cq = ibv_create_cq(ctx, CQ_DEPTH, NULL, NULL, 0);
	if (!cq) {
		perror("ibv_create_cq");
		goto out;
	}
	printf("CQ created: requested %d entries, got %d\n", CQ_DEPTH, cq->cqe);

	/* Queue Pair: A send queue and a receive queue, created in RESET. */
	memset(&init_attr, 0, sizeof(init_attr));
	init_attr.send_cq = cq;
	init_attr.recv_cq = cq;
	init_attr.qp_type = IBV_QPT_RC;
	init_attr.cap.max_send_wr = QP_DEPTH;
	init_attr.cap.max_recv_wr = QP_DEPTH;
	init_attr.cap.max_send_sge = 1;
	init_attr.cap.max_recv_sge = 1;
	init_attr.sq_sig_all = 0; 

	qp = ibv_create_qp(pd, &init_attr);
	if (!qp) {
		perror("ibv_create_qp");
		goto out;
	}
	/* ibv_create_qp writes the capacities actually granted into cap. */
	printf("QP created: qp_num 0x%06" PRIx32 ", type RC\n", qp->qp_num);
	printf("  granted max_send_wr %u, max_recv_wr %u, max_send_sge %u, max_recv_sge %u, max_inline_data %u\n",
			init_attr.cap.max_send_wr, init_attr.cap.max_recv_wr,
			init_attr.cap.max_send_sge, init_attr.cap.max_recv_sge,
			init_attr.cap.max_inline_data);
	if (print_qp_state(qp))
		goto out;

	/* Packet Sequence Numbers are 24 bits. Start at a random value. */
	srand48(time(NULL) ^ getpid());
	psn = (uint32_t)(lrand48() & 0xffffff);

	if (qp_to_init(qp))
		goto out;
	if (qp_to_rtr(qp, port_attr.active_mtu, qp->qp_num, &gid,
				(uint8_t)gid_index, psn))
		goto out;
	if (qp_to_rts(qp, psn))
		goto out;

	status = EXIT_SUCCESS;

out:
	/* Tear down in reverse order of creation. */
	if (qp && ibv_destroy_qp(qp))
		fprintf(stderr, "ibv_destroy_qp failed\n");
	if (cq && ibv_destroy_cq(cq))
		fprintf(stderr, "ibv_destroy_cq failed\n");
	if (mr && ibv_dereg_mr(mr))
		fprintf(stderr, "ibv_dereg_mr failed\n");
	if (pd && ibv_dealloc_pd(pd))
		fprintf(stderr, "ibv_dealloc_pd failed\n");
	free(buf);
	if (ctx && ibv_close_device(ctx))
		fprintf(stderr, "ibv_close_device failed\n");
	ibv_free_device_list(list);

	if (status == EXIT_SUCCESS)
		printf("teardown complete\n");
	return status;
}









