### rdma_avo

Remote Direct Memory Access (RDMA) programming against libibverbs and librdmacm, built up one program per concept. The test bed is two arm64 Ubuntu 26.04 virtual machines connected by Soft-RoCE (`rdma_rxe`).

#### Build

Requires `libibverbs-dev` and `librdmacm-dev`.

```bash
make
```

#### Progress
| Program | Concepts |
|---|---|
| `devinfo` | Device list, device limits, port attributes, GID table |
| `resources` | PD, MR, CQ, and RC QP creation; QP state transitions RESET, INIT, RTR, RTS; teardown order |
| `rc_send` | Two-node RC SEND/RECV over a TCP exchange of QP number, PSN, and GID; work requests, SGEs, completion polling |
| `rc_write` | One-sided RDMA WRITE into a peer buffer named by address and rkey; MR and QP remote-write access; target-side verification |
| `rc_read` | One-sided RDMA READ from a peer buffer named by address and rkey; MR and QP remote-read access; outstanding-read limits; pattern verification on both sides |
