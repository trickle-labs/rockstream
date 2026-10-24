# VS7 exchange ownership review

The executable ownership kernels are called by `FlowController` during handoff and release. The pinned verifier proved exact owner comparison and local/aggregate release accounting. Runtime tests cover the confirmed old-permit/new-channel collision, delivered and cancelled reservations, duplicate/late ACK, teardown, and a lowered live row budget.

The original code at `90c468a6` permitted immediate channel-key reuse after teardown. A locally owned old permit then reduced the new channel from `(available 0, rows 1, batches 1, bytes 3)` to `(available 1, rows 0, batches 0, bytes 1)` while its new 3-byte reservation remained live. The regression failed on that original code and passes after the repair.

Undelivered permits may reuse a key because each channel instance has an owner token. Delivered keys are retired after teardown because wire ACKs have no generation. Retired keys remain in controller memory; a wire generation is required before high-churn reuse can be supported. The proof does not cover Tokio scheduling or transport delivery.
