#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
evidence_dir="$repo_root/.artifacts/post-ack-block-crash"
work_dir="$(mktemp -d)"
mount_dir="$work_dir/first-mount"
recovery_mount="$work_dir/recovery-mount"
mapper=rockstream-post-ack
base_image="$work_dir/base.raw"
log_image="$work_dir/log.raw"
cut_image="$work_dir/post-ack.raw"
ack_file="$evidence_dir/ack.txt"
writer_log="$evidence_dir/writer.log"
recovery_log="$evidence_dir/recovery.log"
loop_base=""
loop_log=""
loop_cut=""
cargo_pid=""
mounted=0
recovery_mounted=0
started_seconds=$SECONDS
mkdir -p "$evidence_dir" "$mount_dir" "$recovery_mount"

cleanup() {
    local result=$?
    set +e
    if [ -n "$cargo_pid" ] && kill -0 "$cargo_pid" 2>/dev/null; then
        kill "$cargo_pid"
        wait "$cargo_pid"
    fi
    if [ "$recovery_mounted" -eq 1 ]; then
        sudo umount "$recovery_mount"
    fi
    if [ "$mounted" -eq 1 ]; then
        sudo umount "$mount_dir"
    fi
    sudo dmsetup remove "$mapper" 2>/dev/null
    if [ -n "$loop_cut" ]; then sudo losetup -d "$loop_cut"; fi
    if [ -n "$loop_log" ]; then sudo losetup -d "$loop_log"; fi
    if [ -n "$loop_base" ]; then sudo losetup -d "$loop_base"; fi
    rm -rf "$work_dir"
    exit "$result"
}
trap cleanup EXIT

rm -f "$ack_file" "$writer_log" "$recovery_log"
python3 "$repo_root/scripts/replay-dm-log-writes.py" --self-test
sudo modprobe dm-log-writes || true
sudo dmsetup targets | awk '$1 == "log-writes" { found = 1 } END { exit !found }'
truncate -s 256M "$base_image" "$log_image"
loop_base="$(sudo losetup --find --show --direct-io=on "$base_image")"
loop_log="$(sudo losetup --find --show --direct-io=on "$log_image")"
sectors="$(sudo blockdev --getsz "$loop_base")"
sudo dmsetup create "$mapper" --table "0 $sectors log-writes $loop_base $loop_log"
sudo mkfs.ext4 -F -E nodiscard "/dev/mapper/$mapper"
sudo dmsetup message "$mapper" 0 mark mkfs
sudo mount -o nodiscard "/dev/mapper/$mapper" "$mount_dir"
mounted=1
sudo mkdir -p "$mount_dir/shard"
sudo chown -R "$(id -u):$(id -g)" "$mount_dir/shard"
source_sha="${ROCKSTREAM_SOURCE_SHA:-$GITHUB_SHA}"
export ROCKSTREAM_SOURCE_SHA="$source_sha"
{
    printf 'source_sha=%s\nworkflow_sha=%s\ncheckout_sha=%s\n' \
        "$source_sha" "$GITHUB_SHA" "$(git rev-parse HEAD)"
    rustc --version
    cargo --version
    uname -a
    sudo dmsetup version
    sudo dmsetup targets
    sudo modinfo dm-log-writes || true
    mkfs.ext4 -V
    findmnt --noheadings --output FSTYPE,OPTIONS "$mount_dir"
} >"$evidence_dir/environment.txt" 2>&1

export ROCKSTREAM_POST_ACK_MODE=write
export ROCKSTREAM_POST_ACK_ROOT="$mount_dir/shard"
export ROCKSTREAM_POST_ACK_SIGNAL="$ack_file"
env -u ROCKSTREAM_OBJECT_STORE_ENDPOINT \
    CARGO_TERM_COLOR=never \
    cargo test --locked -p rockstream-storage --test enospc_recovery_tests \
    dm_log_writes_replays_the_exact_post_ack_epoch -- --ignored --exact --nocapture \
    >"$writer_log" 2>&1 &
cargo_pid=$!
deadline=$((SECONDS + 1800))
while [ ! -s "$ack_file" ]; do
    if ! ps -p "$cargo_pid" -o stat= | grep -qv 'Z'; then
        wait "$cargo_pid" || true
        cat "$writer_log"
        echo "writer exited before the durable ACK signal"
        exit 1
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo "timed out waiting for the durable ACK signal"
        exit 1
    fi
    sleep 1
done

ack="$(<"$ack_file")"
printf '%s\n' "$ack"
expected_state='EpochState { row_1: Some([49, 48, 58, 50, 48, 48]), row_2: None, row_3: Some([51, 48, 58, 55, 53]), source_offset: Some([49, 53]), frontier: Some([0, 0, 0, 0, 0, 0, 0, 2]), operation_id: Some([101, 112, 111, 99, 104, 45, 50]) }'
ack_state="$(printf '%s\n' "$ack" | python3 -c 'import sys; print(sys.stdin.read().split(" state=", 1)[1])')"
test "$ack_state" = "$expected_state"
test_binary="$(sed -n 's/^ *Running tests\/enospc_recovery_tests.rs (\(.*\))$/\1/p' "$writer_log")"
test -x "$test_binary"
printf 'test_binary=%s\n' "$test_binary" >>"$evidence_dir/environment.txt"
sha256sum "$test_binary" >"$evidence_dir/test-binary.sha256"
test_pid="$(printf '%s\n' "$ack" | python3 -c 'import re,sys; print(re.search(r"\bpid=(\d+)\b", sys.stdin.read()).group(1))')"
sudo dmsetup message "$mapper" 0 mark post_ack
kill -KILL "$test_pid"
if wait "$cargo_pid"; then
    echo "writer test unexpectedly exited successfully before the kill"
    exit 1
else
    writer_status=$?
fi
cargo_pid=""
if kill -0 "$test_pid" 2>/dev/null; then
    echo "writer process survived SIGKILL"
    exit 1
fi
echo "writer_process=SIGKILL cargo_status=$writer_status"

sudo umount "$mount_dir"
mounted=0
sudo dmsetup remove "$mapper"
sudo losetup -d "$loop_base"
loop_base=""
sudo losetup -d "$loop_log"
loop_log=""
truncate -s 256M "$cut_image"
python3 "$repo_root/scripts/replay-dm-log-writes.py" \
    --log "$log_image" --image "$cut_image" --end-mark post_ack
gzip -1 -c "$log_image" >"$evidence_dir/dm-log-writes.raw.gz"
sha256sum "$evidence_dir/dm-log-writes.raw.gz" >"$evidence_dir/log-sha256.txt"

loop_cut="$(sudo losetup --find --show --direct-io=on "$cut_image")"
sudo mount -o nodiscard "$loop_cut" "$recovery_mount"
recovery_mounted=1
sudo chown -R "$(id -u):$(id -g)" "$recovery_mount/shard"
ROCKSTREAM_POST_ACK_MODE=recover \
    ROCKSTREAM_POST_ACK_ROOT="$recovery_mount/shard" \
    env -u ROCKSTREAM_OBJECT_STORE_ENDPOINT \
        CARGO_TERM_COLOR=never \
        cargo test --locked -p rockstream-storage --test enospc_recovery_tests \
        dm_log_writes_replays_the_exact_post_ack_epoch -- --ignored --exact --nocapture \
        2>&1 | tee "$recovery_log"
printf 'post_ack_dm_log_writes_result=passed source_sha=%s runtime_seconds=%s\n' \
    "$source_sha" "$((SECONDS - started_seconds))" \
    | tee "$evidence_dir/result.txt"
