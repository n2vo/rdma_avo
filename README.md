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
| `rc_write_imm` | RDMA WRITE with immediate data; zero-SGE receives consumed by the WRITE; target-side completions carrying the immediate in order |
| `rc_atomic` | Remote fetch-and-add and compare-and-swap on an 8-byte counter; MR and QP remote-atomic access; prior values returned to the requester; device atomic capability |
| `rc_kv_slab` | Several writers on separate RC QPs share one server MR and CQ; fetch-and-add on an 8-byte counter reserves a slot (ticket); RDMA WRITE with immediate places the slab and carries the ticket; server checks unique tickets, slab contents at each completion, and the final counter |

#### Soft-RoCE Scope

- **What It Shows:** Concurrent writers receive disjoint tickets from one remote atomic counter, each slab is in place when its WRITE with immediate completes, and the ticket reaches the server through the immediate value without the server reading memory. `rc_kv_slab` checks all three.
- **What It Cannot Show:** Line-rate throughput or latency, because `rdma_rxe` runs verbs in kernel software on the CPU, and lossy-fabric behavior (reordering, retransmission, congestion control) of hardware transports such as RoCEv2 NICs or Falcon.
- **Locked Memory:** Registering an MR pins it against `ulimit -l` (8 MiB by default on these VMs). `rc_kv_slab` registers `8 + slots * slab_size` bytes on the server, so 1 MiB slabs need the limit raised; `-s 65536` fits.
