#!/usr/bin/env python3
import argparse
import shutil
import sys
from pathlib import Path


IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff")


def is_image_file(path: Path) -> bool:
    return path.is_file() and path.name.lower().endswith(IMAGE_EXTS)


def unique_dest(path: Path) -> Path:
    if not path.exists():
        return path
    suffix = "".join(path.suffixes)
    base = path.name[: -len(suffix)] if suffix else path.name
    for i in range(1, 10_000):
        candidate = path.with_name(f"{base}__dup{i}{suffix}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"too many name collisions for {path}")


def iter_images(root: Path):
    for path in root.rglob("*"):
        if is_image_file(path):
            yield path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Restore images from blacklist back to the dataset root."
    )
    parser.add_argument(
        "--blacklist-dir",
        type=Path,
        required=True,
        help="Blacklist folder that stores filtered images.",
    )
    parser.add_argument(
        "--target-dir",
        type=Path,
        required=True,
        help="Root folder to restore images into (e.g. SC).",
    )
    parser.add_argument(
        "--copy",
        action="store_true",
        help="Copy instead of move.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without moving files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    blacklist_dir = args.blacklist_dir.expanduser().resolve()
    target_dir = args.target_dir.expanduser().resolve()

    if not blacklist_dir.exists():
        print(f"blacklist dir not found: {blacklist_dir}", file=sys.stderr)
        return 2

    target_dir.mkdir(parents=True, exist_ok=True)

    moved = 0
    images = list(iter_images(blacklist_dir))
    if not images:
        print("no images found in blacklist")
        return 0

    for path in images:
        try:
            rel = path.relative_to(blacklist_dir)
        except ValueError:
            rel = Path(path.name)

        dest = unique_dest(target_dir / rel)
        if args.dry_run:
            print(f"DRY RUN: move {path} -> {dest}")
        else:
            dest.parent.mkdir(parents=True, exist_ok=True)
            if args.copy:
                shutil.copy2(path, dest)
            else:
                shutil.move(path, dest)
        moved += 1

    print(f"done. restored={moved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
