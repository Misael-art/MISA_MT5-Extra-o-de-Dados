"""Pacotes incompletos ou adulterados não podem qualificar a recuperação."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess

import pytest


SOURCE = Path(__file__).resolve().parents[1] / "scripts" / "verify_recovery.py"
SPEC = importlib.util.spec_from_file_location("verify_recovery", SOURCE)
recovery = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(recovery)


@pytest.fixture
def package(tmp_path):
    manifest = {"schema_version": 1, "complete": True, "files": []}
    for group in sorted(recovery.REQUIRED_GROUPS):
        name = f"cópias/{group}.tar.zst"
        target = tmp_path / name
        target.parent.mkdir(exist_ok=True)
        content = ("conteúdo privado de teste: " + group).encode()
        target.write_bytes(content)
        manifest["files"].append({"path": name, "group": group,
                                  "bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()})
    return tmp_path, manifest


def test_verification_preserves_all_bytes_and_never_claims_external_copy_or_restore(package):
    root, manifest = package
    before = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    result = recovery.verify(root, manifest)
    assert len(result["files_verified"]) == 5 and result["archive_integrity_verified"]
    assert not result["external_storage_location_verified"]
    assert not result["restore_exercised_by_this_command"]
    assert not result["operational_acceptance"]
    assert before == {p: p.read_bytes() for p in before}


@pytest.mark.parametrize("mutation", ("same_size_tamper", "missing_last", "wrong_size", "directory"))
def test_missing_or_divergent_archive_is_rejected(package, mutation):
    root, manifest = package
    target = root / manifest["files"][-1]["path"]
    if mutation == "same_size_tamper":
        data = target.read_bytes()
        target.write_bytes(bytes([data[0] ^ 1]) + data[1:])
    elif mutation == "missing_last":
        target.unlink()
    elif mutation == "wrong_size":
        manifest["files"][-1]["bytes"] += 1
    else:
        target.unlink()
        target.mkdir()
    with pytest.raises((recovery.VerificationError, OSError)):
        recovery.verify(root, manifest)


@pytest.mark.parametrize("name", ("../outside", "/absolute", "C:/outside", "C:outside", "..\\outside", "a/../b", ""))
def test_cross_platform_paths_cannot_escape_backup(package, name):
    root, manifest = package
    manifest["files"][-1]["path"] = name
    with pytest.raises(recovery.VerificationError):
        recovery.verify(root, manifest)


@pytest.mark.parametrize("mutation", ("incomplete", "missing_group", "duplicate", "bad_hash", "boolean_size", "boolean_version"))
def test_inventory_cannot_qualify_by_omission_or_duplicate_entry(package, mutation):
    root, manifest = package
    if mutation == "incomplete":
        manifest["complete"] = False
    elif mutation == "missing_group":
        manifest["files"].pop()
    elif mutation == "duplicate":
        manifest["files"].append(copy.deepcopy(manifest["files"][0]))
    elif mutation == "bad_hash":
        manifest["files"][0]["sha256"] = "not-a-hash"
    elif mutation == "boolean_size":
        manifest["files"][0]["bytes"] = True
    else:
        manifest["schema_version"] = True
    with pytest.raises(recovery.VerificationError):
        recovery.verify(root, manifest)


def test_cli_refuses_partial_manifest_and_reports_error_without_writing(package, capsys):
    root, manifest = package
    path = root / "manifest.json"
    manifest["complete"] = False
    path.write_text(json.dumps(manifest))
    before = path.read_bytes()
    assert recovery.main(["--backup-dir", str(root), "--manifest", str(path)]) == 1
    result = json.loads(capsys.readouterr().out)
    assert not result["ok"] and result["preserve_source"]
    assert path.read_bytes() == before


def test_link_to_external_folder_is_not_accepted_as_a_backup_copy(package, tmp_path):
    root, manifest = package
    outside = tmp_path.parent / (tmp_path.name + "-external")
    outside.mkdir()
    original = root / manifest["files"][0]["path"]
    (outside / "archive.tar.zst").write_bytes(original.read_bytes())
    link = root / "alias"
    if os.name == "nt":
        # Junctions do not need the symlink privilege on Windows.
        subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(outside)],
                       check=True, capture_output=True)
    else:
        link.symlink_to(outside, target_is_directory=True)
    manifest["files"][0]["path"] = "alias/archive.tar.zst"
    with pytest.raises(recovery.VerificationError):
        recovery.verify(root, manifest)
    assert (outside / "archive.tar.zst").read_bytes() == original.read_bytes()


def test_development_profile_is_explicit_and_does_not_qualify_native_archives(package):
    root, manifest = package
    manifest["files"] = [item for item in manifest["files"]
                         if item["group"] in recovery.PROFILES["source_development_v1"]]
    # Legacy/default manifests still require native evidence and toolchain.
    with pytest.raises(recovery.VerificationError, match="categorias"):
        recovery.verify(root, manifest)
    manifest["preservation_profile"] = "source_development_v1"
    result = recovery.verify(root, manifest)
    assert len(result["files_verified"]) == 3
    assert result["preservation_profile"] == "source_development_v1"
    assert "native_evidence" not in result["groups_verified"]
    assert not result["operational_acceptance"]
    manifest["files"].pop()
    with pytest.raises(recovery.VerificationError, match="categorias"):
        recovery.verify(root, manifest)


@pytest.mark.parametrize("profile", (None, True, "source", {"name": "source_development_v1"}))
def test_unknown_or_malformed_profile_cannot_reduce_requirements(package, profile):
    root, manifest = package
    manifest["preservation_profile"] = profile
    with pytest.raises(recovery.VerificationError, match="Perfil"):
        recovery.verify(root, manifest)
