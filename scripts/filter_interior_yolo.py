#!/usr/bin/env python3
import argparse
import shutil
import sys
from pathlib import Path

from typing import Iterable, Optional, Sequence

from ultralytics import YOLO
from tqdm import tqdm


IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff")


def is_image_file(path: Path) -> bool:
    return path.is_file() and path.name.lower().endswith(IMAGE_EXTS)


def find_class_id(names, target: str) -> Optional[int]:
    if isinstance(names, dict):
        for k, v in names.items():
            if v == target:
                return int(k)
        return None
    if isinstance(names, (list, tuple)):
        for i, name in enumerate(names):
            if name == target:
                return int(i)
    return None


def find_class_ids(names, targets: Sequence[str]) -> set[int]:
    class_ids = set()
    for target in targets:
        class_id = find_class_id(names, target)
        if class_id is not None:
            class_ids.add(class_id)
    return class_ids


def has_any_class(result, class_ids: Iterable[int]) -> bool:
    if result.boxes is None or len(result.boxes) == 0:
        return False
    class_ids = set(class_ids)
    for cls_id in result.boxes.cls.tolist():
        if int(cls_id) in class_ids:
            return True
    return False


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


def iter_images(root: Path, blacklist_dir: Path):
    for path in root.rglob("*"):
        if blacklist_dir in path.parents:
            continue
        if is_image_file(path):
            yield path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Move images without a detected 'car' object into a blacklist folder."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Root folder to scan for images.",
    )
    parser.add_argument(
        "--blacklist-dir",
        type=Path,
        required=True,
        help="Destination folder for filtered images.",
    )
    parser.add_argument(
        "--model",
        default="yolo11n.pt",
        help="Ultralytics model path or name (e.g. yolov8n.pt, yolo11n.pt).",
    )
    parser.add_argument(
        "--conf",
        type=float,
        default=0.25,
        help="Confidence threshold for detection.",
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=16,
        help="Batch size for inference.",
    )
    parser.add_argument(
        "--imgsz",
        type=int,
        default=640,
        help="Inference image size (square). Larger can improve accuracy.",
    )
    parser.add_argument(
        "--iou",
        type=float,
        default=0.7,
        help="IOU threshold for NMS.",
    )
    parser.add_argument(
        "--device",
        default=None,
        help="Device for inference (e.g. cpu, 0, 0,1).",
    )
    parser.add_argument(
        "--keep-classes",
        default="car",
        help="Comma-separated class names to treat as 'car' (default: car).",
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
    input_dir = args.input_dir.expanduser().resolve()
    blacklist_dir = args.blacklist_dir.expanduser().resolve()

    if not input_dir.exists():
        print(f"input dir not found: {input_dir}", file=sys.stderr)
        return 2

    blacklist_dir.mkdir(parents=True, exist_ok=True)

    model = YOLO(args.model)
    keep_classes = [c.strip() for c in args.keep_classes.split(",") if c.strip()]
    class_ids = find_class_ids(model.names, keep_classes)
    if not class_ids:
        print(
            "model does not contain requested classes. "
            "Use a COCO-trained model like yolo11n.pt.",
            file=sys.stderr,
        )
        return 3

    images = list(iter_images(input_dir, blacklist_dir))
    if not images:
        print("no images found")
        return 0

    moved = 0
    kept = 0

    for i in tqdm(
        range(0, len(images), args.batch),
        desc="Inference",
        unit="batch",
    ):
        batch_paths = images[i : i + args.batch]
        results = model.predict(
            source=[str(p) for p in batch_paths],
            conf=args.conf,
            iou=args.iou,
            imgsz=args.imgsz,
            device=args.device,
            verbose=False,
        )
        for path, result in zip(batch_paths, results):
            if has_any_class(result, class_ids):
                kept += 1
                continue

            try:
                rel = path.relative_to(input_dir)
            except ValueError:
                rel = Path(path.name)

            dest = unique_dest(blacklist_dir / rel)
            if args.dry_run:
                print(f"DRY RUN: move {path} -> {dest}")
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                if args.copy:
                    shutil.copy2(path, dest)
                else:
                    shutil.move(path, dest)
            moved += 1

    print(f"done. kept={kept} moved={moved} total={len(images)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
