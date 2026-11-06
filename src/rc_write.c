/*
 * rc_write: Two-node RC one-sided RDMA WRITE. The server exposes a buffer by
 * sending its address and rkey over TCP; the client writes into that buffer
 * directly. The server's CPU posts nothing and sees no completions. It checks
 * the buffer once, at the end.
 * Program 4 of rdma_avo Phase 1.
 *
 * Usage:
 *   server: rc_write [-d device] [-g gid_index] [-p tcp_port] [-n iters] [-s size]
 *   client: rc_write [-d device] [-g gid_index] [-p tcp_port] [-n iters] [-s size] server_ip
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
#define MAX_MSG_SIZE	(1u << 20)

#define WRID_WRITE  1

/* Connection details swapped over TCP. Integers go in network byte order.
 * addr and rkey name the buffer the peer may write into.
 */
struct conn_info {
	uint32_t qpn;
	uint32_t psn;
	uint8_t gid[16];
	uint64_t addr;
	uint32_t rkey;
};

struct rdma_res {
	struct ibv_context *ctx;
	struct ibv_pd *pd;
	struct ibv_mr *mr;
	struct ibv_cq *cq;
	struct ibv_qp *qp;
	char *buf;		/* Client: WRITE source. Server: WRITE target. */
	uint32_t size;
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

/* Server: Wait for one client and return the connected socket. */
static int tcp_accept(const char *port)
{
	struct addrinfo hints, *res;
	int lfd, fd, on = 1, ret;

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

	if (bind(lfd, res->ai_addr, res->ai_addrlen) || listen(lfd, 1)) {
		perror("bind/listen");
		close(lfd);
		freeaddrinfo(res);
		return -1;
	}
	freeaddrinfo(res);

	printf("waiting for client on TCP port %s\n", port);
	fd = accept(lfd, NULL, NULL);
	if (fd < 0)
		perror("accept");
	close(lfd);
	return fd;
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
 *
 * Function  Name Reads As                   Width              Header
 * --------  ---------------------------     -----------------  -------------
 * htonl     host to network, long           32-bit (uint32_t)  <arpa/inet.h>
 * ntohl     network to host, long           32-bit (uint32_t)  <arpa/inet.h>
 * htobe64   host to big-endian, 64-bit      64-bit (uint64_t)  <endian.h>
 * be64toh   big-endian, 64-bit, to host     64-bit (uint64_t)  <endian.h>
 */
static int exchange_info(int fd, const struct conn_info *local,
			struct conn_info *remote)
{
	struct conn_info wire = *local;

	wire.qpn = htonl(local->qpn);
	wire.psn = htonl(local->psn);
	wire.addr = htobe64(local->addr);
	wire.rkey = htonl(local->rkey);
	if (write_all(fd, &wire, sizeof(wire)) || read_all(fd, &wire, sizeof(wire)))
		return -1;

	*remote = wire;
	remote->qpn = ntohl(wire.qpn);
	remote->psn = ntohl(wire.psn);
	remote->addr = be64toh(wire.addr);
	remote->rkey = ntohl(wire.rkey);
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
	 * Let the peer RDMA WRITE through this QP. The MR must also allow it;
	 * the QP flag permits the operation on this connection, the MR flag
	 * permits it on the memory.
	 */
	attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;

	/* Fault test: Client gets "remote invalid request error". */
	// attr.qp_access_flags = 0;

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

/* ---------- Data path ---------- */

/*
 * Fill the local buffer with seq's low byte, put seq itself in the first
 * 4 bytes, and RDMA WRITE the whole buffer to the peer's buffer.
 */
static int post_write(struct rdma_res *r, uint32_t seq,
			const struct conn_info *remote)
{
	struct ibv_sge sge = {
		.addr = (uintptr_t)r->buf,
		.length = r->size,
		.lkey = r->mr->lkey,
	};
	struct ibv_send_wr wr = {
		.wr_id = WRID_WRITE,
		.sg_list = &sge,
		.num_sge = 1,
		.opcode = IBV_WR_RDMA_WRITE,
		.send_flags = IBV_SEND_SIGNALED,
	};
	struct ibv_send_wr *bad;
	int ret;

	/* Where in the peer's memory to write, and the key that permits it. */
	wr.wr.rdma.remote_addr = remote->addr;
	wr.wr.rdma.rkey = remote->rkey;

	memset(r->buf, (int)(seq & 0xff), r->size);
	memcpy(r->buf, &seq, sizeof(seq));

	ret = ibv_post_send(r->qp, &wr, &bad);
	if (ret)
		fprintf(stderr, "ibv_post_send: %s\n", strerror(ret));
	return ret ? -1 : 0;
}

/*
 * Busy-poll the CQ for the WRITE's completion. On RC it arrives only after
 * the peer acknowledged the WRITE, so the data is in the peer's memory.
 */
static int wait_write_completion(struct rdma_res *r)
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
		if (wc.wr_id != WRID_WRITE || wc.opcode != IBV_WC_RDMA_WRITE) {
			fprintf(stderr, "unexpected completion: wr_id %" PRIu64 ", opcode %d\n",
				wc.wr_id, wc.opcode);
			return -1;
		}
		return 0;
	}
}

/* Server: Check that the buffer holds exactly what the last WRITE carried. */
static int verify_buffer(const struct rdma_res *r, uint32_t last_seq)
{
	const uint8_t fill = (uint8_t)(last_seq & 0xff);
	uint32_t seq;

	memcpy(&seq, r->buf, sizeof(seq));
	if (seq != last_seq) {
		fprintf(stderr, "buffer holds seq %u, expected %u\n", seq, last_seq);
		return -1;
	}
	for (uint32_t i = sizeof(seq); i < r->size; i++) {
		if ((uint8_t)r->buf[i] != fill) {
			fprintf(stderr, "byte %u is 0x%02x, expected 0x%02x\n",
					i, (uint8_t)r->buf[i], fill);
			return -1;
		}
	}
	printf("buffer verified: seq %u, %u bytes\n", seq, r->size);
	return 0;
}

/* ---------- Setup and teardown ---------- */

static int setup_res(struct rdma_res *r, struct ibv_device *dev, uint32_t size)
{
	struct ibv_qp_init_attr init_attr;

	r->size = size;

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

	if (posix_memalign((void **)&r->buf, (size_t)sysconf(_SC_PAGESIZE), size)) {
		fprintf(stderr, "posix_memalign failed\n");
		r->buf = NULL;
		return -1;
	}
	memset(r->buf, 0, size);

	/*
	 * REMOTE_WRITE lets the peer write into this memory. Verbs requires
	 * LOCAL_WRITE alongside it; ibv_reg_mr fails without it.
	 */
	r->mr = ibv_reg_mr(r->pd, r->buf, size,
				IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
	if (!r->mr) {
		perror("ibv_reg_mr");
		return -1;
	}

	r->cq = ibv_create_cq(r->ctx, QUEUE_DEPTH, NULL, NULL, 0);
	if (!r->cq) {
		perror("ibv_create_cq");
		return -1;
	}

	memset(&init_attr, 0, sizeof(init_attr));
	init_attr.send_cq = r->cq;
	init_attr.recv_cq = r->cq;
	init_attr.qp_type = IBV_QPT_RC;
	init_attr.cap.max_send_wr = QUEUE_DEPTH;
	init_attr.cap.max_recv_wr = QUEUE_DEPTH;
	init_attr.cap.max_send_sge = 1;
	init_attr.cap.max_recv_sge = 1;

	r->qp = ibv_create_qp(r->pd, &init_attr);
	if (!r->qp) {
		perror("ibv_create_qp");
		return -1;
	}
	return 0;
}

static void free_res(struct rdma_res *r)
{
	if (r->qp && ibv_destroy_qp(r->qp))
		fprintf(stderr, "ibv_destroy_qp failed\n");
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
	printf("         addr 0x%016" PRIx64 ", rkey 0x%08" PRIx32 "\n",
			info->addr, info->rkey);
}

static void usage(const char *prog)
{
	fprintf(stderr,
		"usage: %s [-d device] [-g gid_index] [-p tcp_port] [-n iters] [-s size] [server_ip]\n",
		prog);
}

int main(int argc, char *argv[])
{
	const char *dev_name = NULL;
	const char *tcp_port = "18515";
	const char *server_ip = NULL;
	int gid_index = 1;
	long iters = 1000;
	long size = 4096;
	struct ibv_device **list;
	struct ibv_device *dev = NULL;
	struct ibv_port_attr port_attr;
	union ibv_gid gid;
	struct rdma_res r;
	struct conn_info local, remote;
	struct timespec t0, t1;
	int num, opt, ret, fd = -1;
	int status = EXIT_FAILURE;

	while ((opt = getopt(argc, argv, "d:g:p:n:s:")) != -1) {
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
		case 'n':
			iters = atol(optarg);
			break;
		case 's':
			size = atol(optarg);
			break;
		default:
			usage(argv[0]);
			return EXIT_FAILURE;
		}
	}
	if (optind < argc)
		server_ip = argv[optind];
	if (iters < 1 || size < (long)sizeof(uint32_t) || size > MAX_MSG_SIZE) {
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

	if (setup_res(&r, dev, (uint32_t)size))
		goto out;

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

	if (qp_to_init(r.qp))
		goto out;

	/* Zero first: The struct's padding bytes also go over TCP. */
	memset(&local, 0, sizeof(local));
	srand48(time(NULL) ^ getpid());
	local.qpn = r.qp->qp_num;
	local.psn = (uint32_t)(lrand48() & 0xffffff);
	memcpy(local.gid, gid.raw, sizeof(local.gid));
	local.addr = (uintptr_t)r.buf;
	local.rkey = r.mr->rkey;

	/* With a server address we are the client; without one, the server. */
	fd = server_ip ? tcp_connect(server_ip, tcp_port) : tcp_accept(tcp_port);
	if (fd < 0)
		goto out;
	if (exchange_info(fd, &local, &remote))
		goto out;
	print_info("local", &local);
	print_info("remote", &remote);

	if (qp_to_rtr(r.qp, port_attr.active_mtu, &remote, (uint8_t)gid_index))
		goto out;
	if (qp_to_rts(r.qp, local.psn))
		goto out;

	/* Both QPs must be in RTS before the first WRITE. */
	if (barrier(fd))
		goto out;

	if (server_ip) {
		clock_gettime(CLOCK_MONOTONIC, &t0);
		for (uint32_t seq = 0; seq < (uint32_t)iters; seq++) {
			if (post_write(&r, seq, &remote) || wait_write_completion(&r))
				goto out;
		}
		clock_gettime(CLOCK_MONOTONIC, &t1);

		/* Tell the server every WRITE has landed. */
		if (barrier(fd))
			goto out;

		double secs = (double)(t1.tv_sec - t0.tv_sec) +
				(double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
		double bytes = (double)size * (double)iters;

		printf("%.0f bytes in %.2f seconds = %.2f Mbit/sec\n",
				bytes, secs, bytes * 8.0 / secs / 1e6);
		printf("%ld iters in %.2f seconds = %.2f usec/iter\n",
				iters, secs, secs * 1e6 / (double)iters);
	} else {
		/* Server: Nothing to post or poll. Wait for the client to finish. */
		if (barrier(fd))
			goto out;
		if (verify_buffer(&r, (uint32_t)(iters - 1)))
			goto out;
	}
	status = EXIT_SUCCESS;

out:
	if (fd >= 0)
		close(fd);
	free_res(&r);
	ibv_free_device_list(list);
	return status;
}
