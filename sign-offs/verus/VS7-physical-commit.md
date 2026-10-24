# VS7-02 physical commit transition review

`PhysicalCommitGroup` calls the executable ownership kernel when draining pending or uncertain entries and when applying each storage result. The proof covers retained epoch, byte, and operation conservation, monotone committed frontier, and ACK only on the durable branch. It does not prove the backend's durability contract, Tokio scheduling, or process crash behavior.

The gated tests pause before storage submission, after `ShardDb::write_batch` returns, and after `ShardDb::flush` returns before ACK. Cancellation and timeout of a requester at each gate leave the driver-owned flight intact; a direct driver-drop test retains exact uncertain ownership. A separate LFS subprocess matrix kills the process at those cuts and compares recovery with the exact legal atomic histories, including overwrite, delete, merge count, frontier, and later queued work. Graceful completion and injected pre-operation write failure are separate cases.
