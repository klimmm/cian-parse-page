# factory.py
import logging
from scraper.scraper_core import BaseScraper
from scraper.thread_scraper import ThreadScraperStrategy
from scraper.process_scraper import ProcessScraperStrategy
from scraper.memory_monitoring import MonitoredScraper

logger = logging.getLogger(__name__)


def create_scraper(
    mode="thread",
    cpu_monitoring=True,
    system_monitoring=False,
    **kwargs,
):
    """
    Enhanced factory function to create a scraper with optional monitoring

    Args:
        mode (str): "thread" or "process" to select the scraping strategy
        cpu_monitoring (bool): Enable CPU monitoring of the scraper process
        system_monitoring (bool): Enable system-wide resource monitoring
        **kwargs: Additional arguments to pass to the BaseScraper constructor

    Returns:
        BaseScraper or MonitoredScraper: A scraper instance
    """
    # Create the base scraper with the appropriate strategy
    if mode.lower() == "thread":
        scraper = BaseScraper(**kwargs)
        scraper.set_strategy(ThreadScraperStrategy())
    elif mode.lower() == "process":
        scraper = BaseScraper(**kwargs)
        scraper.set_strategy(ProcessScraperStrategy())
    else:
        raise ValueError(f"Unknown scraper mode: {mode}")

    # Return plain scraper if monitoring is disabled
    if not cpu_monitoring or system_monitoring:
        return scraper

    return MonitoredScraper(
        scraper, cpu_monitoring, system_monitoring, cpu_threshold=80.0, monitoring_interval=1.0
    )


def create_thread_scraper(**kwargs):
    return create_scraper(mode="thread", **kwargs)


def create_process_scraper(**kwargs):
    return create_scraper(mode="process", **kwargs)
