import os
import logging
from PIL import Image

logger = logging.getLogger(__name__)


def average_hash(image, hash_size=8):
    """Generate average hash for an image."""
    img = image.convert("L").resize((hash_size, hash_size), resample=Image.LANCZOS)
    pixels = list(img.getdata())
    avg = sum(pixels) / len(pixels)
    bits = "".join("1" if p > avg else "0" for p in pixels)
    return int(bits, 2)


def hamming_distance(h1, h2):
    """Calculate Hamming distance between two hashes."""
    return bin(h1 ^ h2).count("1")


def dedupe_images(folder, hash_size=8, max_distance=1):
    """
    Remove near-duplicate images in `folder` by comparing
    average hashes and deleting any whose Hamming distance ≤ max_distance.
    
    Args:
        folder: Path to folder containing images
        hash_size: Size of hash for comparison
        max_distance: Maximum Hamming distance to consider duplicates
    """
    # Collect (path, hash)
    imgs = []
    for fn in os.listdir(folder):
        if fn.lower().endswith((".jpg", ".jpeg", ".png")):
            path = os.path.join(folder, fn)
            try:
                img = Image.open(path)
                hsh = average_hash(img, hash_size)
                imgs.append((path, hsh))
            except Exception as e:
                logger.warning(f"Could not hash {path}: {e}")

    # Find duplicates
    to_remove = set()
    for i in range(len(imgs)):
        p1, h1 = imgs[i]
        for j in range(i + 1, len(imgs)):
            p2, h2 = imgs[j]
            if hamming_distance(h1, h2) <= max_distance:
                # Mark the later one for deletion
                to_remove.add(p2)

    # Delete duplicates
    for path in to_remove:
        try:
            os.remove(path)
            logger.info(f"🗑️ Removed duplicate image: {path}")
        except Exception as e:
            logger.error(f"Failed to remove {path}: {e}")