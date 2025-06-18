import os
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def count_existing_images(dir_path: str) -> int:
    """Count existing image files in a directory."""
    if not os.path.isdir(dir_path):
        return 0
    return sum(
        1
        for fn in os.listdir(dir_path)
        if fn.lower().endswith((".jpg", ".jpeg", ".png"))
    )


def prefilter_listings_for_download(parsed_apts: List[Dict[str, Any]], image_dir: str) -> List[Dict[str, Any]]:
    """
    Filter listings to determine which ones need image downloading.
    
    Args:
        parsed_apts: List of apartment data with image_urls
        image_dir: Directory where images are stored
        
    Returns:
        List of listings that need image downloading
    """
    to_download = []
    
    for apt in parsed_apts:
        offer_id = str(apt.get("offer_id"))
        urls = apt.get("image_urls") or []
        expected = len(urls)
        
        if expected == 0:
            continue

        folder = os.path.join(image_dir, offer_id)
        existing = count_existing_images(folder)
        missing = expected - existing

        if existing < expected / 2:  # Only download if less than half of images exist
            logger.info(
                f"⚠️ {offer_id}: have {existing}/{expected}, missing {missing} images, queuing"
            )
            to_download.append(apt)
        else:
            logger.debug(
                f"⏩ {offer_id}: have {existing}/{expected}, skipping (less than half missing)"
            )

    logger.info(f"🆕 {len(to_download)} offers need images")
    return to_download