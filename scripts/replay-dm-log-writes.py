#!/usr/bin/env python3
"""Replay dm-log-writes v1 entries through a named durable-write mark."""

import argparse
import os
import struct
import tempfile
from pathlib import Path

MAGIC = 0x6A736677736872
VERSION = 1
FLUSH = 1
FUA = 2
DISCARD = 4
MARK = 8
METADATA = 16
ENTRY = struct.Struct("<QQQQ")
SUPER = struct.Struct("<QQQI")


def replay(log_path: Path, image_path: Path, end_mark: str) -> str:
    with log_path.open("rb") as log, image_path.open("r+b") as image:
        header = log.read(SUPER.size)
        if len(header) != SUPER.size:
            raise ValueError("truncated dm-log-writes header")
        magic, version, count, sector_size = SUPER.unpack(header)
        if magic != MAGIC or version != VERSION:
            raise ValueError(f"unsupported dm-log-writes header: {magic:x}/{version}")
        if sector_size < ENTRY.size or sector_size & (sector_size - 1):
            raise ValueError(f"invalid sector size: {sector_size}")
        image.seek(0, 2)
        image_size = image.tell()
        log.seek(sector_size)
        entries = writes = sectors = 0

        for _ in range(count):
            block = log.read(sector_size)
            if len(block) != sector_size:
                raise ValueError("truncated dm-log-writes entry")
            sector, number, flags, data_len = ENTRY.unpack_from(block)
            entries += 1
            if flags & MARK:
                mark = block[ENTRY.size :].split(b"\0", 1)[0].decode("utf-8")
                if not data_len or data_len > sector_size - ENTRY.size:
                    raise ValueError(f"invalid mark length: {data_len}")
                if mark == end_mark:
                    image.flush()
                    os.fsync(image.fileno())
                    return (
                        f"replayed_entries={entries} writes={writes} "
                        f"sectors={sectors} end_mark={mark}"
                    )
                continue
            if flags & DISCARD:
                raise ValueError(
                    "discard entry encountered; format and mount with nodiscard"
                )
            if flags & ~(FLUSH | FUA | METADATA):
                raise ValueError(f"unsupported entry flags: {flags}")
            if not number:
                continue

            size = number * sector_size
            offset = sector * sector_size
            if offset + size > image_size:
                raise ValueError(
                    f"write exceeds replay image: offset={offset} size={size}"
                )
            data = log.read(size)
            if len(data) != size:
                raise ValueError("truncated dm-log-writes data")
            image.seek(offset)
            image.write(data)
            writes += 1
            sectors += number

    raise ValueError(f"end mark not found: {end_mark}")


def self_test() -> str:
    sector_size = 512
    write = ENTRY.pack(1, 1, 0, 0) + bytes(sector_size - ENTRY.size) + b"A" * sector_size

    def mark_entry(mark: bytes) -> bytes:
        return (
            ENTRY.pack(0, 0, MARK, len(mark) + 1)
            + mark
            + b"\0"
            + bytes(sector_size - ENTRY.size - len(mark) - 1)
        )

    after_mark = ENTRY.pack(2, 1, 0, 0) + bytes(sector_size - ENTRY.size) + b"B" * sector_size
    entries = write + mark_entry(b"ack") + after_mark + mark_entry(b"end")
    header = SUPER.pack(MAGIC, VERSION, 4, sector_size)

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        log_path = root / "log.bin"
        image_path = root / "image.bin"
        log_path.write_bytes(header + bytes(sector_size - SUPER.size) + entries)
        image_path.write_bytes(bytes(4 * sector_size))
        summary = replay(log_path, image_path, "ack")
        expected = bytes(sector_size) + b"A" * sector_size + bytes(2 * sector_size)
        assert image_path.read_bytes() == expected
        assert summary == "replayed_entries=2 writes=1 sectors=1 end_mark=ack"

    return f"self-test=ok {summary}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path)
    parser.add_argument("--image", type=Path)
    parser.add_argument("--end-mark")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        print(self_test())
        return
    if not args.log or not args.image or not args.end_mark:
        parser.error("--log, --image, and --end-mark are required")
    print(replay(args.log, args.image, args.end_mark))


if __name__ == "__main__":
    main()
