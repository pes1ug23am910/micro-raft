#!/usr/bin/env python3
"""Build shard-service from an explicit, hashed source copy for experiments."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import os
import posixpath
from pathlib import Path, PurePosixPath
import re
import signal
import stat
import subprocess
import sys
import tarfile
import time
import tomllib

MANIFEST = "scripts/shard_source_manifest.json"
ROOT_INPUTS = {"Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "LICENSE"}
CRATES = {"raft-core", "kv-node", "state-store", "sim", "shard-service"}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def strict_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate manifest field")
        result[key] = value
    return result


def safe_name(name):
    if not isinstance(name, str) or not name or "\\" in name or ":" in name:
        raise ValueError("require normalized relative POSIX input paths")
    path = PurePosixPath(name)
    if path.is_absolute() or str(path) != name or any(p in (".", "..") for p in path.parts):
        raise ValueError("unsafe manifest path")
    if name in ROOT_INPUTS:
        return name
    parts = path.parts
    if len(parts) < 3 or parts[0] != "crates" or parts[1] not in CRATES:
        raise ValueError("input outside explicit build-source allowlist")
    if len(parts) == 3 and parts[2] == "Cargo.toml":
        return name
    if (len(parts) >= 4 and parts[2] in ("src", "tests") and path.suffix == ".rs"
            and re.fullmatch(r"[A-Za-z0-9_-]+\.rs", parts[-1])
            and all(re.fullmatch(r"[A-Za-z0-9_-]+", p) for p in parts[3:-1])):
        return name
    raise ValueError("input outside explicit Rust source/test allowlist")


def checked_file(root, name):
    current = root
    for part in (None, *PurePosixPath(name).parts):
        if part is not None:
            current /= part
        info = current.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError("symlink or reparse-point input")
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("source input must be a regular file")
    return current


def validate_manifests(inputs):
    workspace = tomllib.loads(inputs["Cargo.toml"].decode("utf-8"))
    if set(workspace.get("workspace", {}).get("members", [])) != {f"crates/{c}" for c in CRATES}:
        raise ValueError("workspace differs from explicit crate allowlist")

    def inspect(value, base):
        if isinstance(value, dict):
            for key, child in value.items():
                if key == "path":
                    if not isinstance(child, str) or "\\" in child or ":" in child or child.startswith("/"):
                        raise ValueError("unsafe Cargo path input")
                    name = posixpath.normpath(str(PurePosixPath(base).parent / child))
                    if name not in inputs and name + "/Cargo.toml" not in inputs:
                        raise ValueError("Cargo path escapes sealed inputs")
                inspect(child, base)
        elif isinstance(value, list):
            for child in value:
                inspect(child, base)

    for name, raw in inputs.items():
        if name.endswith("Cargo.toml"):
            inspect(tomllib.loads(raw.decode("utf-8")), name)


def snapshot(root):
    raw = checked_file(root, MANIFEST).read_bytes()
    if len(raw) > 256 * 1024:
        raise ValueError("manifest too large")
    manifest = json.loads(raw, object_pairs_hook=strict_object)
    if (not isinstance(manifest, dict) or set(manifest) != {"version", "files"}
            or type(manifest["version"]) is not int or manifest["version"] != 1
            or not isinstance(manifest["files"], list) or not 1 <= len(manifest["files"]) <= 1024):
        raise ValueError("unsupported source manifest")
    names = [safe_name(name) for name in manifest["files"]]
    required = ROOT_INPUTS | {f"crates/{crate}/Cargo.toml" for crate in CRATES}
    required.add("crates/shard-service/src/main.rs")
    if len(set(names)) != len(names) or not required.issubset(names):
        raise ValueError("duplicate or missing required workspace input")
    result = {MANIFEST: raw}
    for name in names:
        remaining = 64 * 1024 * 1024 - sum(map(len, result.values()))
        with checked_file(root, name).open("rb") as stream:
            data = stream.read(remaining + 1)
        if len(data) > remaining:
            raise ValueError("source exceeds64MiB")
        result[name] = data
    # Every current Rust module must be in the versioned manifest; adding one
    # requires an explicit manifest update before it can enter an experiment.
    actual = set()
    for crate in CRATES:
        for area in ("src", "tests"):
            start = root / "crates" / crate / area
            if not start.exists():
                continue
            for base, directories, files in os.walk(start, followlinks=False):
                for child in directories:
                    info = (Path(base) / child).lstat()
                    if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
                        raise ValueError("linked source directory")
                for filename in files:
                    if filename.endswith(".rs"):
                        actual.add(safe_name((Path(base) / filename).relative_to(root).as_posix()))
    if actual != {name for name in names if name.endswith(".rs")}:
        raise ValueError("source manifest omits or invents Rust modules")
    validate_manifests(result)
    return result


def materialize(inputs, out):
    context, archive = out / "context", out / "source.tar"
    context.mkdir()
    with archive.open("xb") as stream, tarfile.open(fileobj=stream, mode="w", format=tarfile.PAX_FORMAT) as tar:
        for name, data in sorted(inputs.items()):
            member = tarfile.TarInfo(name)
            member.size, member.mode = len(data), 0o644
            tar.addfile(member, io.BytesIO(data))
            destination = context / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(data)
    return context, archive


def verify_context(context, inputs):
    actual = {p.relative_to(context).as_posix() for p in context.rglob("*") if p.is_file()}
    if actual != set(inputs):
        raise ValueError("build context inventory changed")
    for name, expected in inputs.items():
        if checked_file(context, name).read_bytes() != expected:
            raise ValueError(f"build input changed: {name}")


def build_process(command, context, log, timeout):
    options = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt" else {"start_new_session": True}
    with log.open("xb") as output:
        child = subprocess.Popen(command, cwd=context, stdout=output, stderr=subprocess.STDOUT, **options)
        try:
            return child.wait(timeout=timeout)
        except BaseException:
            # Terminate only this new process group, never by executable name.
            try:
                if child.poll() is None:
                    if os.name == "nt":
                        subprocess.run(["taskkill", "/PID", str(child.pid), "/T", "/F"],
                                       stdout=output, stderr=subprocess.STDOUT, timeout=5, check=False)
                    else:
                        os.killpg(child.pid, signal.SIGKILL)
            finally:
                child.wait(timeout=5)
            raise


def compiled_binary(log, context, target):
    """Use Cargo's selected artifact, including any configured target triple."""
    if log.stat().st_size > 64 * 1024 * 1024:
        raise ValueError("build log exceeds artifact parser bound")
    artifacts = []
    expected_manifest = (context / "crates/shard-service/Cargo.toml").resolve()
    expected_source = (context / "crates/shard-service/src/main.rs").resolve()
    for line in log.read_text(encoding="utf-8", errors="strict").splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue  # Cargo progress and rendered diagnostics share this log.
        if not isinstance(item, dict) or item.get("reason") != "compiler-artifact":
            continue
        cargo_target = item.get("target", {})
        if cargo_target.get("name") != "shard-service" or cargo_target.get("kind") != ["bin"]:
            continue
        if (not isinstance(item.get("manifest_path"), str)
                or Path(item["manifest_path"]).resolve() != expected_manifest
                or not isinstance(cargo_target.get("src_path"), str)
                or Path(cargo_target["src_path"]).resolve() != expected_source
                or not isinstance(item.get("executable"), str)):
            raise ValueError("compiler artifact has wrong package or source identity")
        binary = Path(item["executable"])
        if not binary.is_absolute() or not binary.resolve().is_relative_to(target.resolve()):
            raise ValueError("compiler artifact outside chosen target directory")
        relative = binary.relative_to(target).as_posix()
        artifacts.append(checked_file(target, relative))
    if len(artifacts) != 1:
        raise ValueError("require exactly one matching Cargo executable artifact")
    return artifacts[0]


def execute(args):
    root = args.source_root.absolute()
    # Check lexical ancestry before resolving so a linked parent cannot turn a
    # caller's input boundary into a different source directory silently.
    for path in [*reversed(root.parents), root]:
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError("linked source ancestry")
    root = root.resolve(strict=True)
    out = args.out.resolve()
    target = (args.target_dir or out / "target").resolve()
    if out.exists() or out.is_relative_to(root) or root.is_relative_to(out):
        raise ValueError("output must be fresh and outside the source directory")
    if target.is_relative_to(root) or target.is_relative_to(out / "context"):
        raise ValueError("target directory must be outside source inputs")
    inputs = snapshot(root)
    out.mkdir(parents=True)
    record = dict(schema_version=1, result="INCOMPLETE", source_root=str(root),
                  source_files=[dict(path=n, sha256=digest(d)) for n, d in sorted(inputs.items())],
                  started_unix_ns=time.time_ns())
    save(out / "build.json", record)
    try:
        context, archive = materialize(inputs, out)
        if snapshot(root) != inputs:
            raise ValueError("source changed while sealing")
        command = ["cargo", "build", "--locked", "-p", "shard-service", "--bin", "shard-service",
                   "--message-format=json-render-diagnostics", "--target-dir", str(target), "-j", str(args.jobs)]
        if args.release:
            command.append("--release")
        if args.offline:
            command.append("--offline")
        record.update(command=command, cwd=str(context), target_dir=str(target),
                      source_archive_sha256=digest(archive.read_bytes()))
        save(out / "build.json", record)
        code = build_process(command, context, out / "build.log", args.timeout_seconds)
        record["exit_code"] = code
        verify_context(context, inputs)
        if snapshot(root) != inputs:
            raise ValueError("source changed during sealed build")
        if code != 0:
            raise RuntimeError(f"Cargo exited {code}; see build.log")
        binary = compiled_binary(out / "build.log", context, target)
        binary_bytes = binary.read_bytes()
        if not binary_bytes:
            raise ValueError("empty build artifact")
        copied = out / "bin" / binary.name
        copied.parent.mkdir()
        copied.write_bytes(binary_bytes)
        copied.chmod(binary.stat().st_mode & 0o777)
        if copied.read_bytes() != binary_bytes or binary.read_bytes() != binary_bytes:
            raise ValueError("build artifact changed while copying")
        record.update(result="PASS", source_hashes_matched_build=True,
                      binary_sha256=digest(binary_bytes), binary_path=str(copied), ended_unix_ns=time.time_ns())
        save(out / "build.json", record)
        binding = dict(schema_version=1, source_archive=archive.name,
                       source_archive_sha256=digest(archive.read_bytes()), files=len(inputs),
                       build_record="build.json", build_record_sha256=digest((out / "build.json").read_bytes()),
                       binary_sha256=digest(binary_bytes), binary_path=str(copied), source_hashes_matched_build=True)
        save(out / "source-binding.json", binding)
        return binding
    except BaseException as error:
        record.update(result="FAIL", error=f"{type(error).__name__}: {error}", ended_unix_ns=time.time_ns())
        save(out / "build.json", record)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--target-dir", type=Path)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=float, default=900)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--release", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.jobs <= 64 or not math.isfinite(args.timeout_seconds) or not 1 <= args.timeout_seconds <= 3600:
        parser.error("jobs1..64 and finite timeout1..3600seconds required")
    print(json.dumps(execute(args), sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
