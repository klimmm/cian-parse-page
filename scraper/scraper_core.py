# scraper_core.py

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from typing import Dict, List, Literal, Optional, Any, Protocol

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Data Structures and Configuration
# -----------------------------------------------------------------------------


@dataclass
class ScraperConfig:
    """Configuration for the scraper"""

    num_processes: int = 2
    concurrent_limit: int = 4
    timeout: int = 30000  # ms
    headless: bool = True
    wait_until: str = "domcontentloaded"
    delay_base: float = 0.5
    delay_min: float = 0.1
    delay_max: float = 1.0
    browser_args: List[str] = None
    viewport: Dict[str, int] = None
    user_agents: List[str] = None
    locale: str = "en-US"
    timezone: str = "UTC"
    bypass_csp: bool = True
    extra_headers: Dict[str, str] = None

    def __post_init__(self):
        """Initialize default values"""
        if self.browser_args is None:
            self.browser_args = ["--disable-gpu", "--no-sandbox"]
        if self.viewport is None:
            self.viewport = {"width": 1920, "height": 1080}
        if self.user_agents is None:
            self.user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
            ]
        if self.extra_headers is None:
            self.extra_headers = {}

    def get_launch_options(self) -> Dict[str, Any]:
        """Get browser launch options"""
        return {"headless": self.headless, "args": self.browser_args}

    def get_context_options(self) -> Dict[str, Any]:
        """Get browser context options"""
        return {
            "viewport": self.viewport,
            "user_agent": random.choice(self.user_agents),
            "locale": self.locale,
            "timezone_id": self.timezone,
            "bypass_csp": self.bypass_csp,
        }


@dataclass
class ScrapingTask:
    """Task definition for scraping a batch of URLs"""

    urls_batch: List[str]
    process_id: int
    parsing_script: str
    config: ScraperConfig
    proxy_config: Optional[Dict] = None
    vpn_manager: Optional[Any] = None


# -----------------------------------------------------------------------------
# Strategy Interface
# -----------------------------------------------------------------------------


class ScraperStrategy(Protocol):
    """Interface for scraper execution strategies"""

    async def execute_tasks(
        self,
        tasks: List[ScrapingTask],
        urls: List[str],
        parsing_script: str,
        config: ScraperConfig,
    ) -> List[Dict]:
        """Execute scraping tasks and return results"""
        ...


# -----------------------------------------------------------------------------
# Utility Class with Shared Functions
# -----------------------------------------------------------------------------


class ScraperUtils:
    """Static utility methods shared by different scraper strategies"""

    @staticmethod
    async def setup_page_handlers(page, process_id: int):
        """Set up event handlers for the page"""

        def handle_console_message(msg):
            text = msg.text
            if any(
                pattern in text
                for pattern in [
                    "[AdFox]",
                    "Minified React error",
                    "reactjs.org/docs/error-decoder",
                    "Unknown bundle undefined",
                ]
            ):
                return
            logger.debug(f"[P{process_id}] JS Console ({msg.type}): {text}")

        page.on("console", handle_console_message)

        def handle_page_error(error):
            error_text = str(error)
            if any(
                pattern in error_text
                for pattern in [
                    "Minified React error",
                    "reactjs.org/docs/error-decoder",
                ]
            ):
                return
            logger.info(f"[P{process_id}] JS Error: {error_text}")

        page.on("pageerror", handle_page_error)

    @staticmethod
    async def apply_delay(config: ScraperConfig, process_id: int):
        """Calculate and apply a random delay between requests"""
        delay = config.delay_base + random.uniform(config.delay_min, config.delay_max)
        logger.info(f"[P{process_id}] delay: {delay:.1f}s")
        await asyncio.sleep(delay)

    @staticmethod
    def log_with_lock(
        process_id: int,
        status: Literal["start", "success", "error"],
        counters: Dict,
        start_time: Any,
        url: str = "",
        error_msg: str = "",
        local_counters: Optional[Dict] = None,
    ) -> None:
        """Log messages with counter lock and consistent formatting"""
        with counters[status].get_lock():
            counters[status].value += 1

            # Simple logging implementation
            elapsed = time.time() - start_time.value
            total = counters["total"].value
            success = counters["success"].value
            error = counters["error"].value

            status_indicator = {"start": "⏱️", "success": "✅", "error": "❌"}.get(
                status, ""
            )

            if status == "error" and error_msg:
                logger.error(
                    f"[P{process_id}] {status_indicator} {status.upper()} [{success}/{error}/{total}] {elapsed:.1f}s {url} - {error_msg}"
                )
            # else:
            # logger.info(f"[P{process_id}] {status_indicator} {status.upper()} [{success}/{error}/{total}] {elapsed:.1f}s {url}")


class BaseScraper:
    """Base scraper that can use different execution strategies"""

    def __init__(
        self, parsing_script=None, config=None, vpn_manager=None, strategy=None
    ):
        self.parsing_script = parsing_script
        self.config = config or ScraperConfig()
        self.vpn_manager = vpn_manager
        self.strategy = strategy  # Will be set by the factory

    def set_strategy(self, strategy: ScraperStrategy):
        """Change the scraping strategy at runtime"""
        self.strategy = strategy
        return self

    def scrape_all(self, urls: List[str]) -> List[Dict]:
        """Main entry point - delegates to the selected strategy"""
        if not self.strategy:
            raise ValueError("No strategy set. Use set_strategy() or factory function.")

        n = min(self.config.num_processes, len(urls))
        indices = [len(urls) * i // n for i in range(n + 1)]
        url_chunks = [urls[indices[i]: indices[i + 1]] for i in range(n)]

        # Create scraping tasks
        tasks = self._create_tasks(url_chunks)

        # Execute using the selected strategy
        return asyncio.run(
            self.strategy.execute_tasks(tasks, urls, self.parsing_script, self.config)
        )

    def _create_tasks(self, url_chunks: List[List[str]]) -> List[ScrapingTask]:
        """Create task objects from URL chunks"""
        tasks = []
        for i, chunk in enumerate(url_chunks):
            proxy_config = None
            if self.vpn_manager:
                proxy_config = self.vpn_manager.get_proxy(i)

            task = ScrapingTask(
                urls_batch=chunk,
                process_id=i,
                parsing_script=self.parsing_script,
                config=self.config,
                proxy_config=proxy_config,
                vpn_manager=self.vpn_manager,  # Pass vpn_manager to task
            )
            tasks.append(task)
        return tasks