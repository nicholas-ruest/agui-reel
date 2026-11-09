use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;

#[test]
fn valid_fixture_passes_with_json_report() {
    let mut command = Command::cargo_bin("agui-reel").expect("binary");
    command
        .args(["check", "examples/valid.jsonl", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"valid\": true"))
        .stdout(predicate::str::contains("\"event_count\": 13"));
}

#[test]
fn invalid_fixture_exits_two() {
    let mut command = Command::cargo_bin("agui-reel").expect("binary");
    command
        .args(["check", "examples/broken.jsonl"])
        .assert()
        .code(2)
        .stdout(predicate::str::contains("INVALID"))
        .stdout(predicate::str::contains("lineage_mismatch"));
}

#[test]
fn renderer_protects_existing_output() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("timeline.svg");
    fs::write(&output, "keep me").expect("fixture");

    let mut command = Command::cargo_bin("agui-reel").expect("binary");
    command
        .args(["render", "examples/valid.jsonl", "--output"])
        .arg(&output)
        .assert()
        .failure()
        .stderr(predicate::str::contains("File exists"));
    assert_eq!(fs::read_to_string(output).expect("read"), "keep me");
}

#[test]
fn renderer_writes_standalone_svg() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("timeline.svg");
    let mut command = Command::cargo_bin("agui-reel").expect("binary");
    command
        .args(["render", "examples/valid.jsonl", "--output"])
        .arg(&output)
        .assert()
        .success();
    let svg = fs::read_to_string(output).expect("read");
    assert!(svg.starts_with("<svg"));
    assert!(svg.contains("worker-1"));
}
