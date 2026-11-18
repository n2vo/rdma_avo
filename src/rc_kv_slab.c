/*
 * rc_kv_slab: Several RC writers push KV slabs into one server region. The
 * server registers one buffer: An 8-byte ticket counter at offset 0, then
 * slots slab-sized slots. Each writer reserves a slot with a remote
 * fetch-and-add on the counter (the returned prior value is its ticket; the
 * slot is ticket % slots), then pushes its slab into that slot with one RDMA
 * WRITE with immediate, carrying the ticket as the immediate. The server's CPU
 * takes part only through the receives it posted: Each WRITE consumes one and
 * produces a completion, on which the server checks that the ticket is new and
 * in range and that the slot already holds the whole slab.
 *
 * Every 8-byte word of a slab holds its ticket (bits 63..40), its writer id
 * (bits 39..32), and its word index within the slab (bits 31..0), so a slab in
 * the wrong slot, from the wrong writer, or only partly written fails the check.
 * total_slabs may not exceed slots: No slot is reused, so no flow control is
 * needed. Program 8 of rdma_avo.
 *
 * Usage:
 *   server: rc_kv_slab [-d device] [-g gid_index] [-p tcp_port] [-c clients]
 *                      [-k slots] [-s slab_size] [-n total_slabs]
 *   client: rc_kv_slab [-d device] [-g gid_index] [-p tcp_port] -w writer_id server_ip
 */

#include <arpa/inet.h>
#include <endian.h>
#include <errno.h>
#include <inttypes.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <infiniband/verbs.h>

#define PORT_NUM	1
#define QUEUE_DEPTH 16
#define MAX_CLIENTS	8
#define MAX_SLOTS	65536u		/* Tickets fit in 24 bits of a pattern word. */
#define MAX_REGION	(1ull << 30)	/* Server region cap: 1 GiB. */
#define COUNTER_SIZE	sizeof(uint64_t)

#define WRID_FAA	1
#define WRID_WRITE_IMM	2
#define WRID_RECV	3

/* Connection details swapped over TCP. Integers go in network byte order.
 * Server to client: addr and rkey name the region (counter at offset 0, slots
 * after it), plus the layout. Client to server: writer_id. Fields a side does
 * not use are sent as zero.
 */
struct conn_info {
	uint32_t qpn;
	uint32_t psn;
	uint8_t gid[16];
	uint64_t addr;
	uint32_t rkey;
	uint32_t writer_id;
	uint32_t slots;
	uint32_t slab_size;
	uint32_t slabs_per_writer;
	uint32_t total_slabs;
};

/* A writer's latency summary, sent to the server after its last slab. */
struct writer_stats {
	uint32_t writer_id;
	uint32_t slabs;
	uint64_t min_ns;
	uint64_t p50_ns;
	uint64_t max_ns;
	uint64_t elapsed_ns;
};

/* One device context, PD, CQ, and MR, shared by every QP. The client has one
 * QP; the server has one per writer.
 */
struct rdma_res {
	struct ibv_context *ctx;
	struct ibv_pd *pd;
	struct ibv_mr *mr;
	struct ibv_cq *cq;
	struct ibv_qp *qp[MAX_CLIENTS];
	int nqp;
	char *buf;	/* Client: Ticket at 0, slab source at 8. Server: Counter, then slots. */
	size_t size;
};

/* ---------- TCP: Used only to swap connection details ---------- */

static int write_all(int fd, const void *data, size_t len)
{
	const char *p = data;

	while (len > 0) {
		ssize_t n = write(fd, p, len);

		if (n < 0) {
			if (errno == EINTR)
				continue;
			perror("write");
			return -1;
		}
		p += n;
		len -= (size_t)n;
	}
	return 0;
}

static int read_all(int fd, void *data, size_t len)
{
	char *p = data;

	while (len > 0) {
		ssize_t n = read(fd, p, len);

		if (n < 0) {
			if (errno == EINTR)
				continue;
			perror("read");
			return -1;
		}
		if (n == 0) {
			fprintf(stderr, "peer closed the TCP connection\n");
			return -1;
		}
		p += n;
		len -= (size_t)n;
	}
	return 0;
}

/* Server: Open a listening socket for up to backlog clients. */
static int tcp_listen(const char *port, int backlog)
{
	struct addrinfo hints, *res;
	int lfd, on = 1, ret;

	memset(&hints, 0, sizeof(hints));	/* every field 0 or NULL: no unintended filters */
	hints.ai_family		= AF_INET;		/* IPv4 only */
	hints.ai_socktype	= SOCK_STREAM;	/* TCP */
	hints.ai_flags		= AI_PASSIVE;	/* address for a server to bind() */

	ret = getaddrinfo(NULL, port, &hints, &res);
	if (ret) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(ret));
		return -1;
	}

	lfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
	if (lfd < 0) {
		perror("socket");
		freeaddrinfo(res);
		return -1;
	}
	setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

	if (bind(lfd, res->ai_addr, res->ai_addrlen) || listen(lfd, backlog)) {
		perror("bind/listen");
		close(lfd);
		freeaddrinfo(res);
		return -1;
	}
	freeaddrinfo(res);
	return lfd;
}

/* Client: Connect to the server and return the connected socket. */
static int tcp_connect(const char *host, const char *port)
{
	struct addrinfo hints, *res;
	int fd, ret;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;

	ret = getaddrinfo(host, port, &hints, &res);
	if (ret) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(ret));
		return -1;
	}

	fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
	if (fd < 0) {
		perror("socket");
		freeaddrinfo(res);
		return -1;
	}
	if (connect(fd, res->ai_addr, res->ai_addrlen)) {
		perror("connect");
		close(fd);
		fd = -1;
	}
	freeaddrinfo(res);
	return fd;
}

/* Both sides write first, then read. The struct has padding bytes, so the
 * caller zeroes it before filling it; otherwise stack garbage goes on the wire.
 */
static int exchange_info(int fd, const struct conn_info *local,
			struct conn_info *remote)
{
	struct conn_info wire = *local;

	wire.qpn = htonl(local->qpn);
	wire.psn = htonl(local->psn);
	wire.addr = htobe64(local->addr);
	wire.rkey = htonl(local->rkey);
	wire.writer_id = htonl(local->writer_id);
	wire.slots = htonl(local->slots);
	wire.slab_size = htonl(local->slab_size);
	wire.slabs_per_writer = htonl(local->slabs_per_writer);
	wire.total_slabs = htonl(local->total_slabs);
	if (write_all(fd, &wire, sizeof(wire)) || read_all(fd, &wire, sizeof(wire)))
		return -1;

	*remote = wire;
	remote->qpn = ntohl(wire.qpn);
	remote->psn = ntohl(wire.psn);
	remote->addr = be64toh(wire.addr);
	remote->rkey = ntohl(wire.rkey);
	remote->writer_id = ntohl(wire.writer_id);
	remote->slots = ntohl(wire.slots);
	remote->slab_size = ntohl(wire.slab_size);
	remote->slabs_per_writer = ntohl(wire.slabs_per_writer);
	remote->total_slabs = ntohl(wire.total_slabs);
	return 0;
}

static int send_stats(int fd, const struct writer_stats *s)
{
	struct writer_stats wire;

	memset(&wire, 0, sizeof(wire));
	wire.writer_id = htonl(s->writer_id);
	wire.slabs = htonl(s->slabs);
	wire.min_ns = htobe64(s->min_ns);
	wire.p50_ns = htobe64(s->p50_ns);
	wire.max_ns = htobe64(s->max_ns);
	wire.elapsed_ns = htobe64(s->elapsed_ns);
	return write_all(fd, &wire, sizeof(wire));
}

static int recv_stats(int fd, struct writer_stats *s)
{
	struct writer_stats wire;

	if (read_all(fd, &wire, sizeof(wire)))
		return -1;
	s->writer_id = ntohl(wire.writer_id);
	s->slabs = ntohl(wire.slabs);
	s->min_ns = be64toh(wire.min_ns);
	s->p50_ns = be64toh(wire.p50_ns);
	s->max_ns = be64toh(wire.max_ns);
	s->elapsed_ns = be64toh(wire.elapsed_ns);
	return 0;
}

/* Neither side continues until both have reached this point. */
static int barrier(int fd)
{
	char c = 'b';

	return write_all(fd, &c, 1) || read_all(fd, &c, 1) ? -1 : 0;
}

/* ---------- QP state transitions ---------- */

static int qp_to_init(struct ibv_qp *qp)
{
	struct ibv_qp_attr attr;
	int ret;

	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_INIT;
	attr.pkey_index = 0;
	attr.port_num = PORT_NUM;

	/*
	 * Let the peer run atomics on the counter and RDMA WRITE into the
	 * slots through this QP. The MR must also allow both.
	 */
	attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

	ret = ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
				IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
	if (ret)
		fprintf(stderr, "ibv_modify_qp(INIT): %s\n", strerror(ret));
	return ret ? -1 : 0;
}

static int qp_to_rtr(struct ibv_qp *qp, enum ibv_mtu mtu,
			const struct conn_info *remote, uint8_t sgid_index)
{
	struct ibv_qp_attr attr;
	int ret;

	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTR;
	attr.path_mtu = mtu;
	attr.dest_qp_num = remote->qpn;
	attr.rq_psn = remote->psn;
	attr.max_dest_rd_atomic = 1;
	attr.min_rnr_timer = 12;
	attr.ah_attr.is_global = 1;
	memcpy(attr.ah_attr.grh.dgid.raw, remote->gid, sizeof(remote->gid));
	attr.ah_attr.grh.sgid_index = sgid_index;
	attr.ah_attr.grh.hop_limit = 1;
	attr.ah_attr.port_num = PORT_NUM;

	ret = ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_AV |
			IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
			IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
			IBV_QP_MIN_RNR_TIMER);
	if (ret)
		fprintf(stderr, "ibv_modify_qp(RTR): %s\n", strerror(ret));
	return ret ? -1 : 0;
}

static int qp_to_rts(struct ibv_qp *qp, uint32_t local_psn)
{
	struct ibv_qp_attr attr;
	int ret;

	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTS;
	attr.sq_psn = local_psn;
	attr.timeout = 14;
	attr.retry_cnt = 7;
	attr.rnr_retry = 7;
	attr.max_rd_atomic = 1;

	ret = ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_TIMEOUT |
				IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
				IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
	if (ret)
		fprintf(stderr, "ibv_modify_qp(RTS): %s\n", strerror(ret));
	return ret ? -1 : 0;
}

/* ---------- Slab pattern ---------- */

static uint64_t pattern_word(uint32_t ticket, uint32_t writer_id, uint32_t word)
{
	return ((uint64_t)ticket << 40) | ((uint64_t)(writer_id & 0xff) << 32) | word;
}

static void fill_slab(char *slab, uint32_t slab_size, uint32_t ticket,
			uint32_t writer_id)
{
	uint64_t *w = (uint64_t *)(void *)slab;

	for (uint32_t i = 0; i < slab_size / 8; i++)
		w[i] = pattern_word(ticket, writer_id, i);
}

/* Server: Check every word of a slot against the slab ticket's writer wrote. */
static int verify_slab(const char *slab, uint32_t slab_size, uint32_t ticket,
			uint32_t writer_id, uint32_t slot)
{
	const uint64_t *w = (const uint64_t *)(const void *)slab;

	for (uint32_t i = 0; i < slab_size / 8; i++) {
		uint64_t want = pattern_word(ticket, writer_id, i);

		if (w[i] != want) {
			fprintf(stderr, "slot %u word %u: ticket %" PRIu64 ", writer %" PRIu64
					", word %" PRIu64 "; expected ticket %u, writer %u, word %u\n",
					slot, i, w[i] >> 40, (w[i] >> 32) & 0xff,
					w[i] & 0xffffffffu, ticket, writer_id, i);
			return -1;
		}
	}
	return 0;
}

/* ---------- Data path ---------- */

static uint64_t now_ns(void)
{
	struct timespec t;

	clock_gettime(CLOCK_MONOTONIC, &t);
	return (uint64_t)t.tv_sec * 1000000000u + (uint64_t)t.tv_nsec;
}

/* Client: Busy-poll the CQ for one completion and check it. */
static int wait_send_completion(struct rdma_res *r, uint64_t wr_id,
			enum ibv_wc_opcode opcode)
{
	for (;;) {
		struct ibv_wc wc;
		int n = ibv_poll_cq(r->cq, 1, &wc);

		if (n < 0) {
			fprintf(stderr, "ibv_poll_cq failed\n");
			return -1;
		}
		if (n == 0)
			continue;

		if (wc.status != IBV_WC_SUCCESS) {
			fprintf(stderr, "completion error: %s (wr_id %" PRIu64 ")\n",
				ibv_wc_status_str(wc.status), wc.wr_id);
			return -1;
		}
		if (wc.wr_id != wr_id || wc.opcode != opcode) {
			fprintf(stderr, "unexpected completion: wr_id %" PRIu64 ", opcode %d\n",
				wc.wr_id, wc.opcode);
			return -1;
		}
		return 0;
	}
}

/*
 * Client: Reserve a slot. Fetch-and-add 1 on the server's counter; the device
 * writes the counter's prior value, the ticket, into the first 8 local bytes.
 */
static int reserve_ticket(struct rdma_res *r, const struct conn_info *remote,
			uint64_t *ticket)
{
	struct ibv_sge sge = {
		.addr = (uintptr_t)r->buf,
		.length = COUNTER_SIZE,
		.lkey = r->mr->lkey,
	};
	struct ibv_send_wr wr = {
		.wr_id = WRID_FAA,
		.sg_list = &sge,
		.num_sge = 1,
		.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD,
		.send_flags = IBV_SEND_SIGNALED,
	};
	struct ibv_send_wr *bad;
	int ret;

	/* The counter sits at the region's start, which is page aligned. */
	wr.wr.atomic.remote_addr = remote->addr;
	wr.wr.atomic.rkey = remote->rkey;
	wr.wr.atomic.compare_add = 1;

	ret = ibv_post_send(r->qp[0], &wr, &bad);
	if (ret) {
		fprintf(stderr, "ibv_post_send(FAA): %s\n", strerror(ret));
		return -1;
	}
	if (wait_send_completion(r, WRID_FAA, IBV_WC_FETCH_ADD))
		return -1;
	memcpy(ticket, r->buf, sizeof(*ticket));
	return 0;
}

/*
 * Client: Push the slab at local offset 8 into its slot with one RDMA WRITE
 * carrying the ticket as the immediate. On RC the completion arrives after
 * the server acknowledged the WRITE.
 */
static int push_slab(struct rdma_res *r, const struct conn_info *remote,
			uint32_t ticket)
{
	uint32_t slot = ticket % remote->slots;
	struct ibv_sge sge = {
		.addr = (uintptr_t)(r->buf + COUNTER_SIZE),
		.length = remote->slab_size,
		.lkey = r->mr->lkey,
	};
	struct ibv_send_wr wr = {
		.wr_id = WRID_WRITE_IMM,
		.sg_list = &sge,
		.num_sge = 1,
		.opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
		.send_flags = IBV_SEND_SIGNALED,
		/* The immediate value travels in network byte order. */
		.imm_data = htonl(ticket),
	};
	struct ibv_send_wr *bad;
	int ret;

	wr.wr.rdma.remote_addr = remote->addr + COUNTER_SIZE +
				(uint64_t)slot * remote->slab_size;
	wr.wr.rdma.rkey = remote->rkey;

	ret = ibv_post_send(r->qp[0], &wr, &bad);
	if (ret) {
		fprintf(stderr, "ibv_post_send(WRITE_IMM): %s\n", strerror(ret));
		return -1;
	}
	return wait_send_completion(r, WRID_WRITE_IMM, IBV_WC_RDMA_WRITE);
}

/* Server: Post one zero-SGE receive for an incoming WRITE with immediate. */
static int post_recv(struct ibv_qp *qp)
{
	struct ibv_recv_wr wr = {
		.wr_id = WRID_RECV,
		.sg_list = NULL,
		.num_sge = 0,
	};
	struct ibv_recv_wr *bad;
	int ret;

	ret = ibv_post_recv(qp, &wr, &bad);
	if (ret)
		fprintf(stderr, "ibv_post_recv: %s\n", strerror(ret));
	return ret ? -1 : 0;
}

/* ---------- Setup and teardown ---------- */

/* Device context and PD. */
static int open_dev(struct rdma_res *r, struct ibv_device *dev)
{
	r->ctx = ibv_open_device(dev);
	if (!r->ctx) {
		fprintf(stderr, "ibv_open_device failed\n");
		return -1;
	}

	r->pd = ibv_alloc_pd(r->ctx);
	if (!r->pd) {
		fprintf(stderr, "ibv_alloc_pd failed\n");
		return -1;
	}
	return 0;
}

/* The one CQ every QP shares, for both sends and receives. */
static int create_cq(struct rdma_res *r, int entries)
{
	r->cq = ibv_create_cq(r->ctx, entries, NULL, NULL, 0);
	if (!r->cq) {
		perror("ibv_create_cq");
		return -1;
	}
	return 0;
}

/* One RC QP on the shared CQ, moved to INIT. */
static int add_qp(struct rdma_res *r, uint32_t max_recv_wr)
{
	struct ibv_qp_init_attr init_attr;
	struct ibv_qp *qp;

	memset(&init_attr, 0, sizeof(init_attr));
	init_attr.send_cq = r->cq;
	init_attr.recv_cq = r->cq;
	init_attr.qp_type = IBV_QPT_RC;
	init_attr.cap.max_send_wr = QUEUE_DEPTH;
	init_attr.cap.max_recv_wr = max_recv_wr;
	init_attr.cap.max_send_sge = 1;
	init_attr.cap.max_recv_sge = 1;

	qp = ibv_create_qp(r->pd, &init_attr);
	if (!qp) {
		perror("ibv_create_qp");
		return -1;
	}
	r->qp[r->nqp++] = qp;
	return qp_to_init(qp);
}

/*
 * REMOTE_WRITE and REMOTE_ATOMIC let writers fill slots and bump the counter
 * (the server's role). Verbs requires LOCAL_WRITE alongside them, and it also
 * lets the device write returned tickets here (the client's role).
 */
static int add_buf(struct rdma_res *r, size_t size)
{
	r->size = size;
	if (posix_memalign((void **)&r->buf, (size_t)sysconf(_SC_PAGESIZE), size)) {
		fprintf(stderr, "posix_memalign failed\n");
		r->buf = NULL;
		return -1;
	}
	memset(r->buf, 0, size);

	r->mr = ibv_reg_mr(r->pd, r->buf, size, IBV_ACCESS_LOCAL_WRITE |
				IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
	if (!r->mr) {
		int err = errno;

		perror("ibv_reg_mr");
		/* Registration pins the buffer, which counts against RLIMIT_MEMLOCK. */
		if (err == ENOMEM)
			fprintf(stderr, "registering %zu bytes may exceed the locked-memory limit:"
					" Check ulimit -l (KiB), or use fewer or smaller slots\n", size);
		return -1;
	}
	return 0;
}

static void free_res(struct rdma_res *r)
{
	for (int i = 0; i < r->nqp; i++) {
		if (r->qp[i] && ibv_destroy_qp(r->qp[i]))
			fprintf(stderr, "ibv_destroy_qp failed\n");
	}
	if (r->cq && ibv_destroy_cq(r->cq))
		fprintf(stderr, "ibv_destroy_cq failed\n");
	if (r->mr && ibv_dereg_mr(r->mr))
		fprintf(stderr, "ibv_dereg_mr failed\n");
	if (r->pd && ibv_dealloc_pd(r->pd))
		fprintf(stderr, "ibv_dealloc_pd failed\n");
	free(r->buf);
	if (r->ctx && ibv_close_device(r->ctx))
		fprintf(stderr, "ibv_close_device failed\n");
}

static void print_info(const char *label, const struct conn_info *info)
{
	char gid_str[INET6_ADDRSTRLEN];

	if (!inet_ntop(AF_INET6, info->gid, gid_str, sizeof(gid_str)))
		strcpy(gid_str, "?");
	printf("  %-6s QPN 0x%06" PRIx32 ", PSN 0x%06" PRIx32 ", GID %s\n",
			label, info->qpn, info->psn, gid_str);
}

static void usage(const char *prog)
{
	fprintf(stderr,
		"usage: %s [-d device] [-g gid_index] [-p tcp_port] [-c clients] [-k slots]\n"
		"          [-s slab_size] [-n total_slabs]               (server)\n"
		"       %s [-d device] [-g gid_index] [-p tcp_port] -w writer_id server_ip  (client)\n",
		prog, prog);
}

static int cmp_u64(const void *a, const void *b)
{
	uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;

	return (x > y) - (x < y);
}

static double gib_per_sec(uint64_t bytes, uint64_t ns)
{
	return ns ? (double)bytes / (double)(1u << 30) / ((double)ns / 1e9) : 0.0;
}

/* ---------- Roles ---------- */

static int run_client(struct rdma_res *r, int fd, const struct conn_info *remote,
			uint32_t writer_id)
{
	uint32_t n = remote->slabs_per_writer;
	uint64_t *lat = calloc(n, sizeof(*lat));
	struct writer_stats s;
	uint64_t t0, t1, ticket;
	int status = -1;

	if (!lat) {
		fprintf(stderr, "calloc failed\n");
		return -1;
	}

	/* Both QPs must be in RTS before the first atomic; then wait for the
	 * server's start signal, sent to every writer at once.
	 */
	if (barrier(fd) || read_all(fd, &(char){0}, 1))
		goto out;

	t0 = now_ns();
	for (uint32_t i = 0; i < n; i++) {
		uint64_t a = now_ns();

		if (reserve_ticket(r, remote, &ticket))
			goto out;
		if (ticket >= remote->total_slabs) {
			fprintf(stderr, "ticket %" PRIu64 " out of range [0, %u)\n",
					ticket, remote->total_slabs);
			goto out;
		}
		fill_slab(r->buf + COUNTER_SIZE, remote->slab_size, (uint32_t)ticket,
				writer_id);
		if (push_slab(r, remote, (uint32_t)ticket))
			goto out;
		lat[i] = now_ns() - a;
	}
	t1 = now_ns();

	qsort(lat, n, sizeof(*lat), cmp_u64);
	s.writer_id = writer_id;
	s.slabs = n;
	s.min_ns = lat[0];
	s.p50_ns = lat[n / 2];
	s.max_ns = lat[n - 1];
	s.elapsed_ns = t1 - t0;
	if (send_stats(fd, &s))
		goto out;

	printf("writer %u: %u slabs of %u bytes, per slab (fetch-and-add + WRITE_WITH_IMM):\n"
			"  min %.1f us, p50 %.1f us, max %.1f us; %.3f GiB/s\n",
			writer_id, n, remote->slab_size, (double)s.min_ns / 1e3,
			(double)s.p50_ns / 1e3, (double)s.max_ns / 1e3,
			gib_per_sec((uint64_t)n * remote->slab_size, s.elapsed_ns));
	printf("note: Soft-RoCE runs in kernel software on the CPU; these numbers check\n"
			"function, not NIC line rate.\n");
	status = 0;
out:
	free(lat);
	return status;
}

static int run_server(struct rdma_res *r, const int *fds, uint32_t clients,
			const uint32_t *writer_of, uint32_t slots, uint32_t slab_size,
			uint32_t total)
{
	uint32_t *ticket_writer = calloc(total, sizeof(*ticket_writer));
	uint8_t *seen = calloc(total, 1);
	uint32_t received = 0, switches = 0;
	uint64_t t0, t1, counter;
	struct writer_stats st[MAX_CLIENTS];
	int status = -1;

	if (!ticket_writer || !seen) {
		fprintf(stderr, "calloc failed\n");
		goto out;
	}

	/* Every writer's QP is in RTS; start them all together. */
	for (uint32_t i = 0; i < clients; i++) {
		if (barrier(fds[i]))
			goto out;
	}
	t0 = now_ns();
	for (uint32_t i = 0; i < clients; i++) {
		if (write_all(fds[i], "g", 1))
			goto out;
	}

	/*
	 * One completion per slab. Each is checked as it arrives: No slot is
	 * reused, so a slot that fails the check at its completion was not
	 * fully written when the completion became visible.
	 */
	while (received < total) {
		struct ibv_wc wc[QUEUE_DEPTH];
		int n = ibv_poll_cq(r->cq, QUEUE_DEPTH, wc);

		if (n < 0) {
			fprintf(stderr, "ibv_poll_cq failed\n");
			goto out;
		}
		for (int j = 0; j < n; j++) {
			uint32_t q = clients, ticket, slot;

			if (wc[j].status != IBV_WC_SUCCESS) {
				fprintf(stderr, "completion error: %s (wr_id %" PRIu64 ")\n",
						ibv_wc_status_str(wc[j].status), wc[j].wr_id);
				goto out;
			}
			if (wc[j].wr_id != WRID_RECV || wc[j].opcode != IBV_WC_RECV_RDMA_WITH_IMM ||
					!(wc[j].wc_flags & IBV_WC_WITH_IMM)) {
				fprintf(stderr, "unexpected completion: wr_id %" PRIu64 ", opcode %d\n",
						wc[j].wr_id, wc[j].opcode);
				goto out;
			}
			if (wc[j].byte_len != slab_size) {
				fprintf(stderr, "WRITE length %u, expected %u\n",
						wc[j].byte_len, slab_size);
				goto out;
			}
			for (uint32_t i = 0; i < clients; i++) {
				if (r->qp[i]->qp_num == wc[j].qp_num)
					q = i;
			}
			if (q == clients) {
				fprintf(stderr, "completion on unknown QPN 0x%06x\n", wc[j].qp_num);
				goto out;
			}
			ticket = ntohl(wc[j].imm_data);
			if (ticket >= total) {
				fprintf(stderr, "ticket %u out of range [0, %u)\n", ticket, total);
				goto out;
			}
			if (seen[ticket]) {
				fprintf(stderr, "duplicate ticket %u\n", ticket);
				goto out;
			}
			seen[ticket] = 1;
			ticket_writer[ticket] = writer_of[q];
			slot = ticket % slots;
			if (verify_slab(r->buf + COUNTER_SIZE + (size_t)slot * slab_size,
					slab_size, ticket, writer_of[q], slot))
				goto out;
			received++;
		}
	}
	t1 = now_ns();

	/* Every writer has finished its atomics: The counter is final. */
	memcpy(&counter, r->buf, sizeof(counter));
	if (counter != total) {
		fprintf(stderr, "counter is %" PRIu64 ", expected %u\n", counter, total);
		goto out;
	}
	for (uint32_t t = 1; t < total; t++) {
		if (ticket_writer[t] != ticket_writer[t - 1])
			switches++;
	}

	for (uint32_t i = 0; i < clients; i++) {
		if (recv_stats(fds[i], &st[i]))
			goto out;
	}

	printf("\n%-8s %8s %10s %10s %10s %10s\n",
			"writer", "slabs", "min_us", "p50_us", "max_us", "GiB/s");
	for (uint32_t i = 0; i < clients; i++) {
		printf("%-8u %8u %10.1f %10.1f %10.1f %10.3f\n",
				st[i].writer_id, st[i].slabs, (double)st[i].min_ns / 1e3,
				(double)st[i].p50_ns / 1e3, (double)st[i].max_ns / 1e3,
				gib_per_sec((uint64_t)st[i].slabs * slab_size, st[i].elapsed_ns));
	}
	printf("\nslabs received:        %u of %u bytes\n", received, slab_size);
	printf("unique tickets:        %u (0 to %u, no duplicates, none out of range)\n",
			received, total - 1);
	printf("slab contents:         every word verified at its completion\n");
	printf("counter:               %" PRIu64 "\n", counter);
	printf("writer switches:       %u in ticket order (0 means the writers ran one after the other)\n",
			switches);
	printf("aggregate:             %.3f GiB/s, start signal to last completion\n",
			gib_per_sec((uint64_t)total * slab_size, t1 - t0));
	printf("note: Soft-RoCE runs in kernel software on the CPU; these numbers check\n"
			"function, not NIC line rate.\n");
	status = 0;
out:
	free(ticket_writer);
	free(seen);
	return status;
}

int main(int argc, char *argv[])
{
	const char *dev_name = NULL;
	const char *tcp_port = "18515";
	const char *server_ip = NULL;
	int gid_index = 1;
	long clients = 2, slots = 64, slab_size = 1 << 20, total = -1, writer_id = -1;
	struct ibv_device **list;
	struct ibv_device *dev = NULL;
	struct ibv_device_attr dattr;
	struct ibv_port_attr port_attr;
	union ibv_gid gid;
	struct rdma_res r;
	struct conn_info local, remote;
	uint32_t writer_of[MAX_CLIENTS];
	int fds[MAX_CLIENTS];
	int num, opt, ret, lfd = -1;
	int status = EXIT_FAILURE;

	for (int i = 0; i < MAX_CLIENTS; i++)
		fds[i] = -1;

	while ((opt = getopt(argc, argv, "d:g:p:c:k:s:n:w:")) != -1) {
		switch (opt) {
		case 'd':
			dev_name = optarg;
			break;
		case 'g':
			gid_index = atoi(optarg);
			break;
		case 'p':
			tcp_port = optarg;
			break;
		case 'c':
			clients = atol(optarg);
			break;
		case 'k':
			slots = atol(optarg);
			break;
		case 's':
			slab_size = atol(optarg);
			break;
		case 'n':
			total = atol(optarg);
			break;
		case 'w':
			writer_id = atol(optarg);
			break;
		default:
			usage(argv[0]);
			return EXIT_FAILURE;
		}
	}
	if (optind < argc)
		server_ip = argv[optind];
	if (total < 0)
		total = slots;

	if (server_ip) {
		if (writer_id < 0 || writer_id >= MAX_CLIENTS) {
			fprintf(stderr, "client needs -w writer_id in [0, %d)\n", MAX_CLIENTS);
			usage(argv[0]);
			return EXIT_FAILURE;
		}
	} else if (clients < 1 || clients > MAX_CLIENTS || slots < 1 ||
			slots > (long)MAX_SLOTS || slab_size < 8 || slab_size % 8 ||
			slab_size > (1l << 30) || total < clients || total > slots ||
			total % clients ||
			COUNTER_SIZE + (unsigned long)slots * (unsigned long)slab_size > MAX_REGION) {
		fprintf(stderr, "need 1 <= clients <= %d, 1 <= slots <= %u, slab_size a positive\n"
				"multiple of 8, clients <= total_slabs <= slots, total_slabs a multiple\n"
				"of clients, and 8 + slots * slab_size <= %llu bytes\n",
				MAX_CLIENTS, MAX_SLOTS, MAX_REGION);
		usage(argv[0]);
		return EXIT_FAILURE;
	}

	memset(&r, 0, sizeof(r));

	list = ibv_get_device_list(&num);
	if (!list) {
		perror("ibv_get_device_list");
		return EXIT_FAILURE;
	}
	for (int i = 0; i < num; i++) {
		if (!dev_name || !strcmp(ibv_get_device_name(list[i]), dev_name)) {
			dev = list[i];
			break;
		}
	}
	if (!dev) {
		fprintf(stderr, "device %s not found\n", dev_name ? dev_name : "(any)");
		goto out;
	}

	if (open_dev(&r, dev))
		goto out;

	/* Soft-RoCE reports IBV_ATOMIC_HCA: Atomic against other atomics on it. */
	ret = ibv_query_device(r.ctx, &dattr);
	if (ret) {
		fprintf(stderr, "ibv_query_device: %s\n", strerror(ret));
		goto out;
	}
	if (dattr.atomic_cap == IBV_ATOMIC_NONE) {
		fprintf(stderr, "device does not support atomics\n");
		goto out;
	}

	ret = ibv_query_port(r.ctx, PORT_NUM, &port_attr);
	if (ret) {
		fprintf(stderr, "ibv_query_port: %s\n", strerror(ret));
		goto out;
	}
	if (gid_index < 0 || gid_index > 255 || gid_index >= port_attr.gid_tbl_len) {
		fprintf(stderr, "gid_index %d out of range\n", gid_index);
		goto out;
	}
	ret = ibv_query_gid(r.ctx, PORT_NUM, gid_index, &gid);
	if (ret) {
		fprintf(stderr, "ibv_query_gid: %s\n", strerror(ret));
		goto out;
	}

	srand48(time(NULL) ^ getpid());

	if (server_ip) {
		/* ---------- Client: One QP, one writer ---------- */
		int fd;

		/* At most one completion is outstanding: Each is awaited in turn. */
		if (create_cq(&r, QUEUE_DEPTH) || add_qp(&r, 1))
			goto out;

		/* Zero first: The struct's padding bytes also go over TCP. */
		memset(&local, 0, sizeof(local));
		local.qpn = r.qp[0]->qp_num;
		local.psn = (uint32_t)(lrand48() & 0xffffff);
		memcpy(local.gid, gid.raw, sizeof(local.gid));
		local.writer_id = (uint32_t)writer_id;

		fd = tcp_connect(server_ip, tcp_port);
		if (fd < 0)
			goto out;
		fds[0] = fd;
		if (exchange_info(fd, &local, &remote))
			goto out;
		print_info("local", &local);
		print_info("remote", &remote);
		printf("  region addr 0x%016" PRIx64 ", rkey 0x%08" PRIx32 ": %u slots of %u bytes,"
				" %u slabs per writer\n", remote.addr, remote.rkey, remote.slots,
				remote.slab_size, remote.slabs_per_writer);
		if (remote.slabs_per_writer < 1 || remote.slab_size % 8 ||
				remote.slab_size > port_attr.max_msg_sz) {
			fprintf(stderr, "server layout unusable: %u slabs per writer, slab %u bytes,"
					" max message %u bytes\n", remote.slabs_per_writer,
					remote.slab_size, port_attr.max_msg_sz);
			goto out;
		}

		/* The local buffer: The returned ticket at 0, the slab source at 8. */
		if (add_buf(&r, COUNTER_SIZE + remote.slab_size))
			goto out;

		if (qp_to_rtr(r.qp[0], port_attr.active_mtu, &remote, (uint8_t)gid_index))
			goto out;
		if (qp_to_rts(r.qp[0], local.psn))
			goto out;

		if (run_client(&r, fd, &remote, (uint32_t)writer_id))
			goto out;
	} else {
		/* ---------- Server: One QP per writer, one region ---------- */
		uint32_t per_writer = (uint32_t)(total / clients);
		uint32_t claimed = 0;

		if ((uint64_t)per_writer > (uint64_t)dattr.max_qp_wr ||
				total > dattr.max_cqe ||
				(uint64_t)slab_size > (uint64_t)port_attr.max_msg_sz) {
			fprintf(stderr, "device limits: max_qp_wr %d, max_cqe %d, max_msg_sz %u\n",
					dattr.max_qp_wr, dattr.max_cqe, port_attr.max_msg_sz);
			goto out;
		}
		/* One receive completion per slab: The CQ holds every slab's. */
		if (create_cq(&r, (int)total))
			goto out;

		if (add_buf(&r, COUNTER_SIZE + (size_t)slots * (size_t)slab_size))
			goto out;

		/*
		 * Every WRITE with immediate consumes one receive on the QP it
		 * arrives on, so each QP gets one per slab its writer sends,
		 * posted before any writer can start.
		 */
		for (long i = 0; i < clients; i++) {
			if (add_qp(&r, per_writer))
				goto out;
			for (uint32_t j = 0; j < per_writer; j++) {
				if (post_recv(r.qp[i]))
					goto out;
			}
		}

		lfd = tcp_listen(tcp_port, (int)clients);
		if (lfd < 0)
			goto out;
		printf("waiting for %ld writers on TCP port %s: %ld slots of %ld bytes,"
				" %ld slabs\n", clients, tcp_port, slots, slab_size, total);

		for (long i = 0; i < clients; i++) {
			fds[i] = accept(lfd, NULL, NULL);
			if (fds[i] < 0) {
				perror("accept");
				goto out;
			}

			memset(&local, 0, sizeof(local));
			local.qpn = r.qp[i]->qp_num;
			local.psn = (uint32_t)(lrand48() & 0xffffff);
			memcpy(local.gid, gid.raw, sizeof(local.gid));
			local.addr = (uintptr_t)r.buf;
			local.rkey = r.mr->rkey;
			local.slots = (uint32_t)slots;
			local.slab_size = (uint32_t)slab_size;
			local.slabs_per_writer = per_writer;
			local.total_slabs = (uint32_t)total;
			if (exchange_info(fds[i], &local, &remote))
				goto out;

			if (remote.writer_id >= (uint32_t)clients ||
					(claimed & (1u << remote.writer_id))) {
				fprintf(stderr, "writer id %u is out of range or taken\n",
						remote.writer_id);
				goto out;
			}
			claimed |= 1u << remote.writer_id;
			writer_of[i] = remote.writer_id;
			printf("writer %u:\n", remote.writer_id);
			print_info("local", &local);
			print_info("remote", &remote);

			if (qp_to_rtr(r.qp[i], port_attr.active_mtu, &remote, (uint8_t)gid_index))
				goto out;
			if (qp_to_rts(r.qp[i], local.psn))
				goto out;
		}

		if (run_server(&r, fds, (uint32_t)clients, writer_of, (uint32_t)slots,
				(uint32_t)slab_size, (uint32_t)total))
			goto out;
	}
	status = EXIT_SUCCESS;

out:
	for (int i = 0; i < MAX_CLIENTS; i++) {
		if (fds[i] >= 0)
			close(fds[i]);
	}
	if (lfd >= 0)
		close(lfd);
	free_res(&r);
	ibv_free_device_list(list);
	return status;
}
