#!/usr/bin/env python3
"""Generate a markdown test + coverage report for PR comments."""

from __future__ import annotations

import argparse
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path


MODULE_PREFIX_RE = re.compile(r"^github\.com/[^/]+/[^/]+/")


@dataclass
class FileCoverage:
    total_stmts: int = 0
    covered_stmts: int = 0

    @property
    def pct(self) -> float:
        if self.total_stmts == 0:
            return 0.0
        return 100.0 * self.covered_stmts / self.total_stmts


@dataclass
class PackageCoverage:
    files: dict[str, FileCoverage] = field(default_factory=dict)

    @property
    def total_stmts(self) -> int:
        return sum(f.total_stmts for f in self.files.values())

    @property
    def covered_stmts(self) -> int:
        return sum(f.covered_stmts for f in self.files.values())

    @property
    def pct(self) -> float:
        if self.total_stmts == 0:
            return 0.0
        return 100.0 * self.covered_stmts / self.total_stmts


@dataclass
class TestSuite:
    name: str
    tests: int
    failures: int
    errors: int
    time: float


def rel_path(abs_path: str) -> str:
    return MODULE_PREFIX_RE.sub("", abs_path)


def parse_coverprofile(path: Path) -> dict[str, FileCoverage]:
    # Merged profiles from multi-package `go test -coverpkg=...` repeat the same
    # source blocks once per test binary. Keep the max hit count per block.
    blocks: dict[str, tuple[int, int]] = {}
    for line in path.read_text().splitlines():
        if line.startswith("mode:"):
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        block_id = parts[0]
        stmts = int(parts[1])
        hit = int(parts[2])
        if block_id in blocks:
            prev_stmts, prev_hit = blocks[block_id]
            blocks[block_id] = (prev_stmts, max(prev_hit, hit))
        else:
            blocks[block_id] = (stmts, hit)

    files: dict[str, FileCoverage] = {}
    for block_id, (stmts, hit) in blocks.items():
        abs_file = block_id.split(":")[0]
        rel = rel_path(abs_file)
        fc = files.setdefault(rel, FileCoverage())
        fc.total_stmts += stmts
        if hit > 0:
            fc.covered_stmts += stmts
    return files


def group_by_package(files: dict[str, FileCoverage]) -> dict[str, PackageCoverage]:
    packages: dict[str, PackageCoverage] = defaultdict(PackageCoverage)
    for rel, fc in files.items():
        pkg = str(Path(rel).parent) if "/" in rel else "(root)"
        packages[pkg].files[rel] = fc
    return dict(packages)


def parse_junit(path: Path) -> dict[str, TestSuite]:
    root = ET.parse(path).getroot()
    suites: dict[str, TestSuite] = {}
    for suite in root.iter("testsuite"):
        name = suite.attrib.get("name", "")
        if not name:
            continue
        key = rel_path(name) if name.startswith("github.com/") else name
        suites[key] = TestSuite(
            name=key,
            tests=int(suite.attrib.get("tests", 0)),
            failures=int(suite.attrib.get("failures", 0)),
            errors=int(suite.attrib.get("errors", 0)),
            time=float(suite.attrib.get("time", 0)),
        )
    return suites


def fmt_pct(pct: float) -> str:
    return f"{pct:.1f}%"


def coverage_badge(pct: float) -> str:
    if pct >= 85:
        return "🟢"
    if pct >= 60:
        return "🟡"
    return "🔴"


def render_report(
    packages: dict[str, PackageCoverage],
    suites: dict[str, TestSuite],
    total_pct: float,
    commit: str,
) -> str:
    lines: list[str] = [
        "<!-- coverage-pr-report -->",
        "## Test & Coverage Report",
        "",
        "| | |",
        "|---|---|",
        f"| **Commit** | `{commit}` |",
        f"| **Total coverage** | {coverage_badge(total_pct)} **{fmt_pct(total_pct)}** |",
        f"| **Scope** | `internal/*` excluding `testutil` and `examples` |",
        "",
        "| Package / File | Tests | Failed | Errors | Time (s) | Coverage | Stmts |",
        "|----------------|------:|-------:|-------:|---------:|----------|------:|",
    ]

    total_tests = total_failures = total_errors = 0
    total_time = 0.0
    total_stmts = total_covered = 0

    for pkg in sorted(packages):
        pc = packages[pkg]
        suite = suites.get(pkg)
        tests = suite.tests if suite else ""
        failures = suite.failures if suite else ""
        errors = suite.errors if suite else ""
        time_s = f"{suite.time:.3f}" if suite else ""
        status = ""
        if suite:
            status = "✅ " if suite.failures == 0 and suite.errors == 0 else "❌ "
            total_tests += suite.tests
            total_failures += suite.failures
            total_errors += suite.errors
            total_time += suite.time

        total_stmts += pc.total_stmts
        total_covered += pc.covered_stmts

        lines.append(
            f"| {status}**`{pkg}`** | {tests} | {failures} | {errors} | {time_s} | "
            f"{fmt_pct(pc.pct)} | {pc.covered_stmts} / {pc.total_stmts} |"
        )

        for rel in sorted(pc.files):
            fc = pc.files[rel]
            filename = Path(rel).name
            lines.append(
                f"| &nbsp;&nbsp;`{filename}` | | | | | {fmt_pct(fc.pct)} | "
                f"{fc.covered_stmts} / {fc.total_stmts} |"
            )

    overall = "✅" if total_failures == 0 and total_errors == 0 else "❌"
    lines.append(
        f"| **{overall} Total** | **{total_tests}** | **{total_failures}** | "
        f"**{total_errors}** | **{total_time:.3f}** | **{fmt_pct(total_pct)}** | "
        f"**{total_covered} / {total_stmts}** |"
    )

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverprofile", default="coverage.out", type=Path)
    parser.add_argument("--junit", default="test-results/report.xml", type=Path)
    parser.add_argument("--output", default="coverage-report.md", type=Path)
    parser.add_argument("--commit", default="unknown", type=str)
    args = parser.parse_args()

    if not args.coverprofile.is_file():
        print(f"coverprofile not found: {args.coverprofile}", file=sys.stderr)
        return 1

    profile_files = parse_coverprofile(args.coverprofile)
    packages = group_by_package(profile_files)

    total_stmts = sum(f.total_stmts for f in profile_files.values())
    total_covered = sum(f.covered_stmts for f in profile_files.values())
    total_pct = 100.0 * total_covered / total_stmts if total_stmts else 0.0

    suites: dict[str, TestSuite] = {}
    if args.junit.is_file():
        suites = parse_junit(args.junit)

    report = render_report(packages, suites, total_pct, args.commit)
    args.output.write_text(report)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
