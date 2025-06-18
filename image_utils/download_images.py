import os
import requests
import random
import time
from PIL import Image
from typing import List
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import io
import numpy as np
import glob
from image_utils.load_model import load_model

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.7039.115 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]

# Thread-local storage for sessions
thread_local = threading.local()


def get_session():
    """Get a session for the current thread."""
    if not hasattr(thread_local, "session"):
        thread_local.session = make_image_session(USER_AGENTS)
    return thread_local.session


def make_image_session(user_agents):
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
            "Referer": "https://www.cian.ru/",
            "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "'Windows'",
            "Sec-Fetch-Dest": "image",
            "Sec-Fetch-Mode": "no-cors",
            "Sec-Fetch-Site": "cross-site",
            "Connection": "keep-alive",
            "User-Agent": random.choice(user_agents),
        }
    )
    logger.debug("🔧 New session with UA: %s", session.headers["User-Agent"])
    return session


def download_single_image(args):
    """Download a single image and process with ML prediction. Used by worker threads."""
    offer_id, image_url, abs_idx, total_images, offer_folder, max_retries, model = args

    if not image_url:
        return f"{offer_id}_{abs_idx}", False, "Empty URL"

    session = get_session()

    existing_files = glob.glob(os.path.join(offer_folder, f"*_{abs_idx+1}.jpg"))
    if existing_files:
        return f"{offer_id}_{abs_idx}", True, "Already exists"

    for retry in range(max_retries):
        try:
            # Add some randomization to avoid overwhelming the server
            if retry > 0:
                sleep_time = random.uniform(1, 3) * (retry + 1)
                time.sleep(sleep_time)

            logger.info(
                f"⬇️ [{offer_id}] Downloading image {abs_idx+1}/{total_images} (try {retry+1})"
            )
            resp = session.get(image_url, timeout=10)

            if resp.status_code == 200:
                # Process image through ML model if provided
                if model is not None:
                    try:

                        # Load image from bytes
                        img = Image.open(io.BytesIO(resp.content))

                        # Convert to RGB if needed
                        if img.mode != "RGB":
                            img = img.convert("RGB")

                        # Resize to target size (224, 224)
                        img = img.resize((224, 224))

                        # Convert to array and preprocess
                        img_array = np.array(img, dtype=np.float32)
                        img_array = np.expand_dims(img_array, axis=0)

                        # Make prediction
                        prediction = model.predict(img_array, verbose=0)
                        probability = float(prediction[0][0])

                        # Create filename with probability prefix
                        prob_str = f"{probability:.4f}"
                        filename = os.path.join(
                            offer_folder, f"{prob_str}_{abs_idx+1}.jpg"
                        )

                        logger.info(
                            f"🤖 Prediction: {probability:.4f} for image {abs_idx+1}"
                        )

                    except Exception as e:
                        logger.warning(
                            f"⚠️ Prediction failed for {offer_id}_{abs_idx}: {e}, saving without prediction"
                        )
                        filename = os.path.join(offer_folder, f"{abs_idx+1}.jpg")
                else:
                    # No model provided, use original filename
                    filename = os.path.join(offer_folder, f"{abs_idx+1}.jpg")

                # Save the image
                with open(filename, "wb") as f:
                    f.write(resp.content)
                logger.info(f"✅ Saved to {filename}")
                return f"{offer_id}_{abs_idx}", True, "Success"

            else:
                logger.warning(
                    f"❌ [{offer_id}] Status {resp.status_code} on img {abs_idx+1}"
                )
                if resp.status_code in (403, 429):
                    # Rate limiting - wait longer
                    backoff = 5 + retry * 5
                    time.sleep(backoff)
                elif resp.status_code == 404:
                    # Image not found - don't retry
                    return f"{offer_id}_{abs_idx}", False, f"HTTP {resp.status_code}"
                else:
                    backoff = 1 + retry * 2
                    time.sleep(backoff)

        except (requests.RequestException, ConnectionError, TimeoutError) as e:
            error_msg = str(e)
            if retry < max_retries - 1:
                backoff = 3 + retry * 2
                logger.warning(
                    f"⚠️ [{offer_id}] Error on img {abs_idx+1} try {retry+1}: {error_msg}. Retrying in {backoff}s"
                )
                time.sleep(backoff)
            else:
                logger.error(
                    f"❌ [{offer_id}] Giving up img {abs_idx+1} after {max_retries} tries: {error_msg}"
                )
                return f"{offer_id}_{abs_idx}", False, error_msg

    return f"{offer_id}_{abs_idx}", False, "Max retries exceeded"


def download_images_for_listing_parallel(
    listing, image_dir, max_retries, max_workers=4, model=None
):
    """Download images for a single listing using parallel workers."""
    offer_id = listing["offer_id"]
    images = listing.get("image_urls", [])
    offer_folder = os.path.join(image_dir, str(offer_id))
    os.makedirs(offer_folder, exist_ok=True)

    logger.info(
        f"🖼️ Starting parallel download for offer {offer_id} ({len(images)} images, {max_workers} workers)"
    )

    # Prepare download tasks
    download_tasks = []
    for idx, image_url in enumerate(images):
        if image_url:
            download_tasks.append(
                (
                    offer_id,
                    image_url,
                    idx,
                    len(images),
                    offer_folder,
                    max_retries,
                    model,
                )
            )

    success_count = 0
    error_count = 0

    # Process images in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_task = {
            executor.submit(download_single_image, task): task
            for task in download_tasks
        }

        # Process completed tasks
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                task_id, success, message = future.result()
                if success:
                    success_count += 1
                else:
                    error_count += 1
                    logger.debug(f"Failed {task_id}: {message}")
            except Exception as exc:
                error_count += 1
                logger.error(f"Task {task} generated an exception: {exc}")

    logger.info(
        f"📊 Finished offer {offer_id}: {success_count}/{len(images)} succeeded, {error_count} errors"
    )
    return success_count, error_count


def count_existing_images(dir_path):
    if not os.path.isdir(dir_path):
        return 0
    return sum(
        1
        for fn in os.listdir(dir_path)
        if fn.lower().endswith((".jpg", ".jpeg", ".png"))
    )


def download_and_predict_images(
    parsed_apts,
    image_dir,
    model_path,
    max_retries=3,
    batch_size=5,
    hash_size=8,
    max_distance=0,
    workers=4,
    parallel_mode=True,
):
    """
    Enhanced batch download with worker support.

    Args:
        parsed_apts: List of apartment data
        image_dir: Directory to save images
        max_retries: Number of retry attempts
        batch_size: Number of listings to process at once
        hash_size: Hash size for duplicate detection
        max_distance: Maximum hash distance for duplicates
        workers: Number of worker threads (only used in parallel mode)
        parallel_mode: If True, use parallel downloads within each listing
    """
    logger.info(f"🔄 Batch download start: {len(parsed_apts)} listings total")
    logger.info(
        f"📋 Mode: {'Parallel' if parallel_mode else 'Sequential'}, Workers: {workers if parallel_mode else 'N/A'}"
    )
    model = load_model(model_path)
    # Use the pre-filtered listings directly
    to_download = parsed_apts

    # Process in batches
    total_batches = (len(to_download) + batch_size - 1) // batch_size
    overall_success = 0
    overall_errors = 0

    for i in range(total_batches):
        chunk = to_download[i * batch_size : (i + 1) * batch_size]
        logger.info(
            f"📦 Processing batch {i+1}/{total_batches} ({len(chunk)} listings)"
        )

        for apt in chunk:
            success, errors = download_images_for_listing_parallel(
                apt, image_dir, max_retries, workers, model
            )

            overall_success += success
            overall_errors += errors

        if i + 1 < total_batches:
            pause = 10 + random.random() * 20
            logger.info(f"😴 Sleeping {pause:.1f}s between batches...")
            time.sleep(pause)

    logger.info(
        f"✅ All batches complete. Total: {overall_success} success, {overall_errors} errors"
    )
