/*
 * rc_send: Two-node RC SEND/RECV ping-pong. The two sides swap QP number,
 * PSN, and GID over TCP, connect their QPs to each other, then bounce a
 * message back and forth, checking a sequence number in every message.
 * Program 3 of rdma_avo Phase 1.
 * 
 * Usage:
 *   server: rc_send [-d device] [-g gid_index] [-p tcp_port] [-n iters] [-s size]
 *   client: rc_send [-d device] [-g gid_index] [-p tcp_port] [-n iters] [-s size]
 */

#include <arpa/inet.h>
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

/* wr_id values: The device hands these back in each completion. */
#define WRID_SEND	1
#define WRID_RECV	2

/* Connection details swapped over TCP. Integers go in network byte order. */
struct conn_info {
	uint32_t qpn;
	uint32_t psn;
	uint8_t gid[16];
};

struct rdma_res {
	struct ibv_context *ctx;
	struct ibv_pd *pd;
	struct ibv_mr *mr;
	struct ibv_cq *cq;
	struct ibv_qp *qp;
	char *buf;		/* [0, size) receives; [size, 2 * size) sends */
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

/* Both sides write first, then read. 24 bytes fit in any socket buffer. */
static int exchange_info(int fd, const struct conn_info *local,
			struct conn_info *remote)
{
	struct conn_info wire = *local;

	wire.qpn = htonl(local->qpn);
	wire.psn =  htonl(local->psn);
	if (write_all(fd, &wire, sizeof(wire)) || read_all(fd, &wire, sizeof(wire)))
		return -1;

	*remote = wire;
	remote->qpn = ntohl(wire.qpn);
	remote->psn = ntohl(wire.psn);
	return 0;
}

/* Neither side continues until both have reached this point. */
static int barrier(int fd)
{
	char c = 'b';

	return write_all(fd, &c, 1) || read_all(fd, &c, 1) ? -1 : 0;
}

/* ---------- QP state transitions, as in Program 2 ---------- */

static int qp_to_init(struct ibv_qp *qp)
{
	struct ibv_qp_attr attr;
	int ret;

	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_INIT;
	attr.pkey_index = 0;
	attr.port_num = PORT_NUM;
	/* SEND and RECV need no remote access rights; RDMA ops would. */
	attr.qp_access_flags = 0;

	ret = ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
				IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
	if (ret)
		fprintf(stderr, "ibv_modify_qp(INIT): %s\n", strerror(ret));
	return ret ? -1 : 0 ;
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
			IBV_QP_PATH_MTU |IBV_QP_DEST_QPN |
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

/* Hand the device an empty buffer to receive the next incoming SEND into. */
static int post_recv(struct rdma_res *r)
{
	struct ibv_sge sge = {
		.addr = (uintptr_t)r->buf,
		.length = r->size,
		.lkey = r->mr->lkey,
	};
	struct ibv_recv_wr wr = {
		.wr_id = WRID_RECV,
		.sg_list = &sge,
		.num_sge = 1,
	};
	struct ibv_recv_wr *bad;
	int ret = ibv_post_recv(r->qp, &wr, &bad);

	if (ret)
		fprintf(stderr, "ibv_post_recv: %s\n", strerror(ret));
	return ret ? -1 : 0;
}

/* Put seq at the start of the send buffer and SEND the whole buffer. */
static int post_send(struct rdma_res *r, uint32_t seq)
{
	char *send_buf = r->buf + r->size;
	struct ibv_sge sge = {
		.addr = (uintptr_t)send_buf,
		.length = r->size,
		.lkey = r->mr->lkey,
	};
	struct ibv_send_wr wr = {
		.wr_id = WRID_SEND,
		.sg_list = &sge,
		.num_sge = 1,
		.opcode = IBV_WR_SEND,
			.send_flags = IBV_SEND_SIGNALED,
	};
	struct ibv_send_wr *bad;
	int ret;

	memcpy(send_buf, &seq, sizeof(seq));
	ret = ibv_post_send(r->qp, &wr, &bad);
	if (ret)
		fprintf(stderr, "ibv_post_send: %s\n", strerror(ret));
	return ret ? -1 : 0;
}

/*
 * Busy-poll the CQ until the wanted numbers of send and receive completions
 * have arrived. Every receive is checked for length and sequence number.
 */
static int wait_completions(struct rdma_res *r, int sends, int recvs,
			uint32_t expect_seq)
{
	while (sends > 0 || recvs > 0) {
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

		if (wc.wr_id == WRID_SEND) {
			sends--;
		} else if (wc.wr_id == WRID_RECV) {
			uint32_t seq;

			if (wc.byte_len != r->size) {
				fprintf(stderr, "received %u bytes, expected %u\n",
					wc.byte_len, r->size);
				return -1;
			}
			memcpy(&seq, r->buf, sizeof(seq));
			if (seq != expect_seq) {
				fprintf(stderr, "received seq %u, expected %u\n",
					seq, expect_seq);
				return -1;
			}
			recvs--;
		} else {
			fprintf(stderr, "unexpected wr_id %" PRIu64 "\n", wc.wr_id);
			return -1;
		}
	}
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

	if (posix_memalign((void **)&r->buf, (size_t)sysconf(_SC_PAGESIZE),
				2 * (size_t)size)) {
		fprintf(stderr, "posix_memalign failed\n");
		r->buf = NULL;
		return -1;
	}
	memset(r->buf, 0, 2 * (size_t)size);

	/* LOCAL_WRITE lets the device write incoming SENDs into the buffer. */
	r->mr = ibv_reg_mr(r->pd, r->buf, 2 * (size_t)size,
				IBV_ACCESS_LOCAL_WRITE);
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
		switch(opt) {
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
		if(!dev_name || !strcmp(ibv_get_device_name(list[i]), dev_name)) {
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

	/* Post the first receive before the peer can possibly send. */
	if (post_recv(&r))
		goto out;

	srand48(time(NULL) ^ getpid());
	local.qpn = r.qp->qp_num;
	local.psn = (uint32_t)(lrand48() & 0xffffff);
	memcpy(local.gid, gid.raw, sizeof(local.gid));

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
	if(qp_to_rts(r.qp, local.psn))
		goto out;

	/* Both QPs must be in RTS before the first SEND. */
	if(barrier(fd))
		goto out;

	clock_gettime(CLOCK_MONOTONIC, &t0);
	for (uint32_t seq = 0; seq < (uint32_t)iters; seq++) {
		if (server_ip) {
			/* Client: SEND seq, then wait for our SEND and the echo. */
			if (post_send(&r, seq) || wait_completions(&r, 1, 1, seq))
				goto out;
			if(post_recv(&r))
				goto out;
		} else {
			/* Server: Wait for seq, re-arm the receive, echo seq back. */
			if(wait_completions(&r, 0, 1, seq) || post_recv(&r))
				goto out;
			if (post_send(&r, seq) || wait_completions(&r, 1, 0, 0))
				goto out;
		}
	}
	clock_gettime(CLOCK_MONOTONIC, &t1);

	/* Keep the QP alive until the peer has its last completion. */
	if (barrier(fd))
		goto out;

	{
		double secs = (double)(t1.tv_sec - t0.tv_sec) +
				(double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
		double bytes = (double)size * (double)iters * 2.0;

		printf("%.0f bytes in %.2f seconds = %.2f Mbit/sec\n",
				bytes, secs, bytes * 8.0 / secs /1e6);
		printf("%ld iters in %.2f seconds = %.2f usec/iter\n",
				iters, secs, secs * 1e6 / (double)iters);
	}
	status = EXIT_SUCCESS;

out:
	if(fd >= 0)
		close(fd);
	free_res(&r);
	ibv_free_device_list(list);
	return status;
}












