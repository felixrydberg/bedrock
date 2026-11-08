#!/usr/bin/env python3
"""Samples a program's CPU time through perf_event_open, for `scripts/vopr.sh profile`. Linux x86-64.

The kernel takes the samples, so nothing runs inside the simulated process. An in-process profiler cannot work there: madsim
replaces the clock with one behind a lock, and a signal handler that reads it can stop on a lock its own thread holds. The
program needs frame pointers. Prints the busiest functions and writes stacks in the collapsed format speedscope.app reads.

Usage: vopr-profile.py OUTPUT PROGRAM [ARGS...]
"""
import bisect
import collections
import ctypes
import mmap
import os
import re
import signal
import struct
import subprocess
import sys
import time

HERTZ = 999
LISTED = 30
PAGES = 1024
PERF_EVENT_OPEN = 298
SAMPLE = 9
# disabled, inherit, exclude_kernel, exclude_hv, freq, enable_on_exec
FLAGS = 1 | 1 << 1 | 1 << 5 | 1 << 6 | 1 << 10 | 1 << 12
IP_AND_CALLCHAIN = 1 | 1 << 5


def attribute():
    # perf_event_attr, version 5: a software CPU clock.
    return struct.pack("IIQQQQQIIQQQQIiQIHH", 1, 112, 0, HERTZ, IP_AND_CALLCHAIN, 0, FLAGS, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)


def open_rings(pid):
    libc = ctypes.CDLL(None, use_errno=True)
    libc.syscall.restype = ctypes.c_long
    size = (1 + PAGES) * mmap.PAGESIZE
    rings = []
    for cpu in range(os.cpu_count()):
        attr = ctypes.create_string_buffer(attribute())
        fd = libc.syscall(PERF_EVENT_OPEN, attr, pid, cpu, -1, 0)
        if fd < 0:
            error = ctypes.get_errno()
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
            sys.exit(f"perf_event_open failed: {os.strerror(error)}")
        rings.append(mmap.mmap(fd, size))
    return rings


def drain(ring, stacks):
    head, tail = struct.unpack_from("<Q", ring, 1024)[0], struct.unpack_from("<Q", ring, 1032)[0]
    size = PAGES * mmap.PAGESIZE
    base = mmap.PAGESIZE

    def read(at, length):
        at %= size
        end = min(at + length, size)
        return ring[base + at : base + end] + ring[base : base + length - (end - at)]

    while tail < head:
        kind, _, length = struct.unpack("<IHH", read(tail, 8))
        if kind == SAMPLE:
            body = read(tail + 8, length - 8)
            count = struct.unpack_from("<Q", body, 8)[0]
            chain = struct.unpack_from(f"<{count}Q", body, 16)
            # Values this high are markers between kernel and user frames.
            stacks[tuple(ip for ip in chain if ip < 0xFFFFFFFFFFFFF000)] += 1
        tail += length
    struct.pack_into("<Q", ring, 1032, tail)


def symbols(program):
    listing = subprocess.run(["nm", "-n", "--defined-only", "--demangle=rust", program], capture_output=True, text=True).stdout
    table = []
    for line in listing.splitlines():
        parts = line.split(" ", 2)
        if len(parts) == 3 and parts[1] in "tTwW":
            table.append((int(parts[0], 16), re.sub(r"::h[0-9a-f]{16}$", "", parts[2])))
    return [address for address, _ in table], [name for _, name in table]


def text_slide(pid, program):
    """What to subtract from a running address to get the address `nm` lists."""
    real = os.path.realpath(program)
    for line in open(f"/proc/{pid}/maps"):
        fields = line.split()
        if len(fields) >= 6 and fields[5] == real and "x" in fields[1]:
            start, offset = int(fields[0].split("-")[0], 16), int(fields[2], 16)
            segments = subprocess.run(["readelf", "-lW", program], capture_output=True, text=True).stdout
            page = ~(mmap.PAGESIZE - 1)
            for segment in re.findall(r"LOAD\s+(0x\w+)\s+(0x\w+).*R E", segments):
                if int(segment[0], 16) & page == offset:
                    return start - (int(segment[1], 16) & page)
    sys.exit(f"{program} is not mapped")


def report(stacks, program, slide, output):
    addresses, names = symbols(program)

    def name(ip):
        at = bisect.bisect_right(addresses, ip - slide) - 1
        # Past the last symbol is outside the program too: libc's malloc, free and memcpy live there.
        return names[at] if 0 <= at < len(names) - 1 and ip >= slide else "[shared libraries]"

    own, under = collections.Counter(), collections.Counter()
    samples = sum(stacks.values())
    with open(output, "w") as folded:
        for stack, count in stacks.items():
            frames = [name(ip) for ip in stack]
            if frames:
                own[frames[0]] += count
            for frame in set(frames):
                under[frame] += count
            folded.write(";".join(frame.replace(";", ",") for frame in reversed(frames)) + f" {count}\n")
    for title, counts in (("own time", own), ("time under each function", under)):
        print(f"{samples} samples, by {title}:")
        for frame, count in counts.most_common(LISTED):
            print(f"{count * 100 / samples:5.1f}%  {frame[:110]}")


def main():
    output, program = sys.argv[1:3]
    go_read, go_write = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(go_write)
        os.read(go_read, 1)
        try:
            os.execv(program, sys.argv[2:])
        except OSError:
            os._exit(127)
    rings = open_rings(pid)
    os.write(go_write, b"x")
    stacks = collections.Counter()
    slide = None
    status = None
    while status is None:
        time.sleep(0.05)
        for ring in rings:
            drain(ring, stacks)
        if slide is None and stacks:
            slide = text_slide(pid, program)
        done, result = os.waitpid(pid, os.WNOHANG)
        if done:
            status = result
    for ring in rings:
        drain(ring, stacks)
    if slide is None:
        sys.exit("no samples")
    report(stacks, program, slide, output)
    sys.exit(os.waitstatus_to_exitcode(status))


if __name__ == "__main__":
    main()
