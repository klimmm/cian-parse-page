from typing import Tuple
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scraper")


# Utility functions moved outside the class
def format_time(seconds: float) -> str:
    """Format time in seconds to a human-readable string"""
    minutes = seconds / 60

    if minutes > 60:
        return f"{minutes/60:.1f}h"
    elif minutes > 1:
        return f"{minutes:.1f}m"
    else:
        return f"{seconds:.0f}s"


def calculate_eta(count: int, elapsed: float, total: int) -> Tuple[float, str]:
    """Calculate the ETA based on completed count and elapsed time"""
    if count <= 0:
        return 0, "calculating..."

    avg_time = elapsed / count
    remaining = total - count
    eta_seconds = remaining * avg_time
    eta_str = format_time(eta_seconds)

    return avg_time, eta_str


def log_eta_stats(counters: dict, start_time_value: float) -> None:
    """Calculate and log ETA statistics"""
    started = counters["start"].value
    successful = counters["success"].value
    errors = counters["error"].value
    total_urls = counters["total"].value

    elapsed = time.time() - start_time_value

    completed = successful + errors
    avg_time_succ, eta_str = calculate_eta(successful, elapsed, total_urls)
    avg_time_run, eta_run_str = calculate_eta(completed, elapsed, total_urls)

    logger.info(
        f"(Global: {started}/{total_urls} started, "
        f"{successful}/{started} successful, "
        f"{errors}/{started} errors, "
        f"Elapsed: {format_time(elapsed)}, "
        f"Average_succ: {avg_time_succ:.0f}s, "
        f"ETA this run: {eta_run_str}, "
        f"ETA total: {eta_str})"
    )


def format_url_for_display(url: str, max_length: int = 50) -> str:
    """Format URL for display by truncating if necessary"""
    return url[:max_length]


def format_error_for_display(error_msg: str, max_length: int = 100) -> str:
    """Format error message for display by taking first line and truncating"""
    return error_msg.split("\n")[0][:max_length]


def log_process(
    process_id: int,
    status: str,
    url: str = "",
    error_msg: str = "",
    global_counters: dict = None,
    local_counters: dict = None,
) -> None:
    """Unified logging function for all process states"""
    if status == "start" and global_counters and local_counters:
        display_url = format_url_for_display(url)
        global_started = global_counters["start"].value
        global_total = global_counters["total"].value
        local_started = local_counters["start"]
        local_total = local_counters["total"]

        logger.info(
            f"Starting P[{process_id}]: "
            f"(Global: {global_started}/{global_total}, "
            f"Local: {local_started}/{local_total}): "
            f"{display_url}"
        )
    elif status == "success":
        logger.info(f"P[{process_id}] ✅ Success")
    elif status == "error":
        display_error = format_error_for_display(error_msg)
        logger.error(f"P[{process_id}] ❌ Error: {display_error}")


def log_process_start(
    process_id: int,
    url: str,
    global_started: int,
    global_total: int,
    local_started: int,
    local_total: int,
) -> None:
    """Log the start of processing a URL"""
    display_url = format_url_for_display(url)
    logger.info(
        f"Starting P[{process_id}]: "
        f"(Global: {global_started}/{global_total}, "
        f"Local: {local_started}/{local_total}): "
        f"{display_url}"
    )


def log_process_success(process_id: int) -> None:
    """Log successful processing"""
    logger.info(f"P[{process_id}] ✅ Success")


def log_process_error(process_id: int, error_msg: str) -> None:
    """Log processing error"""
    display_error = format_error_for_display(error_msg)
    logger.error(f"P[{process_id}] ❌ Error: {display_error}")


def log_process_with_stats(
    process_id: int,
    status: str,
    global_counters: dict,
    start_time_value: float,
    url: str = "",
    error_msg: str = "",
    local_counters: dict = None,
) -> None:
    """Unified logging function that handles both process logging and ETA stats"""
    # Log the process status
    log_process(
        process_id,
        status,
        url=url,
        error_msg=error_msg,
        global_counters=global_counters,
        local_counters=local_counters,
    )

    # Log ETA stats only for success/error statuses
    if status in ["success", "error"]:
        log_eta_stats(global_counters, start_time_value)
