#!/usr/bin/env python3
import argparse
from collections import defaultdict
from pathlib import Path


IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff")


def is_image_file(path: Path) -> bool:
    return path.is_file() and path.name.lower().endswith(IMAGE_EXTS)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Count images by bucket: <root>/<variant>/<kategori>/<year>/..."
        )
    )
    parser.add_argument(
        "--root",
        type=Path,
        required=True,
        help="Dataset root path (e.g. /images_set).",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=3,
        help="Number of leading path parts to group by (default: 3).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.expanduser().resolve()
    if not root.exists():
        print(f"root not found: {root}")
        return 2

    counts = defaultdict(int)
    for path in root.rglob("*"):
        if not is_image_file(path):
            continue
        try:
            rel_parts = path.relative_to(root).parts
        except ValueError:
            continue
        if len(rel_parts) < args.depth:
            continue
        bucket = Path(*rel_parts[: args.depth])
        counts[str(bucket)] += 1

    if not counts:
        print("no images found")
        return 0

    for bucket in sorted(counts):
        print(f"{bucket}\t{counts[bucket]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
