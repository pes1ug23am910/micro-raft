import argparse
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import seal_shard_build as seal


class SealedBuildTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.root = self.base / "source"
        self.root.mkdir()
        self.files = {
            "LICENSE": "MIT fixture license\n",
            "Cargo.toml": "[workspace]\nmembers=" + json.dumps(["crates/" + c for c in sorted(seal.CRATES)]) + "\n",
            "Cargo.lock": "version = 4\n",
            "rust-toolchain.toml": '[toolchain]\nchannel="1.96.0"\n',
        }
        for crate in seal.CRATES:
            self.files[f"crates/{crate}/Cargo.toml"] = f'[package]\nname="{crate}"\nversion="0.1.0"\n'
            filename = "main.rs" if crate == "shard-service" else "lib.rs"
            self.files[f"crates/{crate}/src/{filename}"] = "fn main() {}\n"
        for name, text in self.files.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        (self.root / "scripts").mkdir()
        self.write_manifest()
        self.args = argparse.Namespace(source_root=self.root, out=self.base / "output", target_dir=None,
            jobs=1, timeout_seconds=10, release=False, offline=True)

    def write_manifest(self, names=None, version=1):
        seal.save(self.root / seal.MANIFEST, dict(version=version, files=sorted(self.files) if names is None else names))

    def fake_build(self, command, context, log, timeout):
        self.assertIn("--locked", command)
        self.assertNotEqual(context, self.root)
        target = Path(command[command.index("--target-dir") + 1]) / "debug"
        target.mkdir(parents=True)
        binary = target / ("shard-service.exe" if os.name == "nt" else "shard-service")
        binary.write_bytes(b"test artifact; never executed")
        log.write_text(json.dumps(dict(reason="compiler-artifact",
            manifest_path=str(context / "crates/shard-service/Cargo.toml"),
            target=dict(name="shard-service",kind=["bin"],src_path=str(context / "crates/shard-service/src/main.rs")),
            executable=str(binary)))+"\n")
        return 0

    def test_complete_binding_uses_copied_context_and_exact_artifact(self):
        with patch.object(seal, "build_process", self.fake_build):
            binding = seal.execute(self.args)
        self.assertIs(binding["source_hashes_matched_build"], True)
        self.assertEqual(binding["binary_sha256"], seal.digest(Path(binding["binary_path"]).read_bytes()))
        self.assertEqual(binding["source_archive_sha256"], seal.digest((self.args.out / "source.tar").read_bytes()))
        seal.verify_context(self.args.out / "context", seal.snapshot(self.root))

    def test_paths_outside_positive_allowlist_are_rejected(self):
        for name in ("../Cargo.toml", "/Cargo.toml", "C:/Cargo.toml", "crates/kv-node/../../secret.rs",
                     "docs/session.md", "crates/kv-node/notes.txt", "crates/kv-node/src/.hidden.rs",
                     "crates/kv-node/src/main.rs/extra", "crates/kv-node/src\\main.rs"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                seal.safe_name(name)

    def test_missing_duplicate_bool_and_unlisted_module_fail(self):
        self.write_manifest(list(self.files) + ["Cargo.toml"])
        with self.assertRaises(ValueError): seal.snapshot(self.root)
        self.write_manifest(version=True)
        with self.assertRaises(ValueError): seal.snapshot(self.root)
        self.write_manifest()
        (self.root / "crates/kv-node/src/extra.rs").write_text("pub fn extra() {}")
        with self.assertRaises(ValueError): seal.snapshot(self.root)
        (self.root / "crates/kv-node/src/extra.rs").unlink()
        (self.root / "Cargo.lock").unlink()
        with self.assertRaises(FileNotFoundError): seal.snapshot(self.root)

    def test_cargo_path_dependency_cannot_read_outside_archive(self):
        path = self.root / "crates/kv-node/Cargo.toml"
        path.write_text(path.read_text() + '[dependencies]\nescape={path="../../../../outside"}\n')
        with self.assertRaisesRegex(ValueError, "escapes"):
            seal.snapshot(self.root)

    def test_context_mutation_produces_failed_record_and_no_binding(self):
        def mutate(command, context, log, timeout):
            result = self.fake_build(command, context, log, timeout)
            (context / "crates/shard-service/src/main.rs").write_text("fn main(){panic!()}\n")
            return result
        with patch.object(seal, "build_process", mutate), self.assertRaisesRegex(ValueError, "input changed"):
            seal.execute(self.args)
        self.assertEqual(json.loads((self.args.out / "build.json").read_text())["result"], "FAIL")
        self.assertFalse((self.args.out / "source-binding.json").exists())

    def test_original_source_mutation_is_not_a_successful_seal(self):
        def mutate(command, context, log, timeout):
            result = self.fake_build(command, context, log, timeout)
            (self.root / "crates/shard-service/src/main.rs").write_text("fn changed() {}\n")
            return result
        with patch.object(seal, "build_process", mutate), self.assertRaisesRegex(ValueError, "source changed"):
            seal.execute(self.args)
        self.assertFalse((self.args.out / "source-binding.json").exists())

    def test_reparse_file_is_rejected_without_reading_target(self):
        original = Path.lstat
        class Linked:
            st_mode = 0o100644
            st_file_attributes = 0x400
        def fake(path):
            return Linked() if path.name == "Cargo.lock" else original(path)
        with patch.object(Path, "lstat", fake), self.assertRaisesRegex(ValueError, "reparse"):
            seal.snapshot(self.root)

    def test_failed_build_never_emits_success_binding(self):
        def fail(command, context, log, timeout):
            log.write_text("fixture compiler failure\n")
            return 101
        with patch.object(seal, "build_process", fail), self.assertRaisesRegex(RuntimeError, "101"):
            seal.execute(self.args)
        self.assertFalse((self.args.out / "source-binding.json").exists())

    def test_configured_target_artifact_wins_over_stale_native_cache(self):
        def cross_target(command, context, log, timeout):
            self.fake_build(command, context, log, timeout)
            target = Path(command[command.index("--target-dir") + 1])
            fresh = target / "configured-triple" / "debug" / "shard-service"
            fresh.parent.mkdir(parents=True)
            fresh.write_bytes(b"current source artifact")
            item = json.loads(log.read_text())
            item["executable"] = str(fresh)
            log.write_text(json.dumps(item)+"\n")
            return 0
        with patch.object(seal, "build_process", cross_target):
            binding = seal.execute(self.args)
        self.assertEqual(Path(binding["binary_path"]).read_bytes(), b"current source artifact")

    def test_success_without_unique_owned_compiler_artifact_is_refused(self):
        for mode in ("missing", "duplicate", "outside", "wrong-package"):
            self.args.out = self.base / mode
            def invalid(command, context, log, timeout):
                self.fake_build(command, context, log, timeout)
                item = json.loads(log.read_text())
                if mode == "outside": item["executable"] = str(self.root / "Cargo.lock")
                if mode == "wrong-package": item["manifest_path"] = str(context / "crates/kv-node/Cargo.toml")
                log.write_text("" if mode == "missing" else (json.dumps(item)+"\n") * (2 if mode == "duplicate" else 1))
                return 0
            with self.subTest(mode=mode), patch.object(seal, "build_process", invalid), self.assertRaises(ValueError):
                seal.execute(self.args)
            self.assertFalse((self.args.out / "source-binding.json").exists())


if __name__ == "__main__":
    unittest.main()
