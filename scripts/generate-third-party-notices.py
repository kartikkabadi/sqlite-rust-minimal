#!/usr/bin/env python3
"""Render a deterministic Markdown notice file from cargo-about JSON."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    data = json.loads(args.input.read_text(encoding="utf-8"))
    project_name = "minisqlite"

    components: list[tuple[str, str, str, str]] = []
    for item in data["crates"]:
        package = item["package"]
        if package["name"] == project_name:
            continue
        components.append(
            (
                package["name"],
                package["version"],
                item["license"],
                package.get("repository") or "",
            )
        )
    components.sort(key=lambda component: (component[0].lower(), component[1]))

    lines = [
        "# Third-Party Notices",
        "",
        "This file records third-party components in the locked Rust runtime dependency graph for the current Linux release target (`x86_64-unknown-linux-gnu`).",
        "",
        "It was generated with `cargo-about` from `Cargo.lock`. Build-only and development-only dependencies are excluded. A release for another target must regenerate this file and its SBOM.",
        "",
        "## Components",
        "",
        "| Component | Version | Declared license | Source |",
        "|---|---:|---|---|",
    ]

    for name, version, license_expression, repository in components:
        source = f"[{repository}]({repository})" if repository else ""
        lines.append(
            f"| `{name}` | `{version}` | `{license_expression}` | {source} |"
        )

    lines.extend(
        [
            "",
            "## Bundled SQLite",
            "",
            "The release binary uses `rusqlite` with its `bundled` feature, so it contains SQLite. The SQLite project states that its deliverable code and documentation are dedicated to the public domain. See <https://www.sqlite.org/copyright.html>.",
            "",
            "## License texts",
            "",
        ]
    )

    for license_info in data["licenses"]:
        users = sorted(
            {
                use["crate"]["name"]
                for use in license_info.get("used_by", [])
                if use["crate"]["name"] != project_name
            }
        )
        if not users:
            continue
        formatted_users = ", ".join(f"`{user}`" for user in users)
        lines.extend(
            [
                f"### {license_info['name']} (`{license_info['id']}`) — {formatted_users}",
                "",
                "```text",
                license_info["text"].rstrip(),
                "```",
                "",
            ]
        )

    args.output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
