from typing import Dict, List, Optional
import logging
import time
import threading
from dataclasses import dataclass
from scraper.scraper_config import ScraperConfig
import asyncio
from playwright.async_api import async_playwright, Browser, Page
from typing import Literal
import random
from utils.utils import log_process_with_stats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Threaded replacement for multiprocessing.Value
class ThreadingValue:
    """Thread-safe value container similar to multiprocessing.Value"""

    def __init__(self, typecode, value=0):
        self.value = value
        self._lock = threading.Lock()

    def get_lock(self):
        return self._lock


class PagePool:
    """Pool of reusable Playwright pages"""

    def __init__(self, context, size=3):
        self.context = context
        self.size = size
        self.available = asyncio.Queue(maxsize=size)
        self._lock = asyncio.Lock()

    async def initialize(self):
        """Create initial pages in the pool"""
        for _ in range(self.size):
            page = await self.context.new_page()
            await self.available.put(page)

    async def get_page(self):
        """Get a page from the pool, waiting if none available"""
        return await self.available.get()

    async def return_page(self, page):
        """Return a page to the pool after use"""
        try:
            # Clear page state by navigating to blank
            await page.goto("about:blank")
            await self.available.put(page)
        except Exception as e:
            logger.error(f"Error returning page to pool: {e}")
            # Create a replacement page
            new_page = await self.context.new_page()
            await self.available.put(new_page)


@dataclass
class ScrapingTask:
    urls_batch: List[str]
    process_id: int
    parsing_script: str
    config: ScraperConfig
    proxy_config: Optional[Dict] = None


class Scraper:
    """Web scraper with shared browser and page pooling"""

    def __init__(
        self,
        parsing_script: str = None,
        config: ScraperConfig = None,
        vpn_manager=None,
    ):
        self.parsing_script = parsing_script
        self.config = config if config is not None else ScraperConfig()
        self.vpn_manager = vpn_manager

    async def _setup_page_handlers(self, page, process_id):
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

    def _log_with_lock(
        self,
        process_id: int,
        status: Literal["start", "success", "error"],
        counters,
        start_time,
        url: str = "",
        error_msg: str = "",
        local_counters=None,
    ) -> None:
        """Log messages with counter lock and consistent formatting"""
        with counters[status].get_lock():
            counters[status].value += 1

            # Call unified logging function - same as original
            log_process_with_stats(
                process_id,
                status,
                global_counters=counters,
                start_time_value=start_time.value,
                url=url,
                error_msg=error_msg,
                local_counters=local_counters,
            )

    async def _apply_delay(self, process_id):
        """Calculate and apply a random delay between requests"""
        delay = self.config.delay_base + random.uniform(
            self.config.delay_min, self.config.delay_max
        )
        logger.info(f"[P{process_id}] delay: {delay:.1f}s")
        await asyncio.sleep(delay)

    async def _scrape_worker(self, task, page_pool, counters, start_time):
        """Worker that scrapes URLs using a page pool"""
        process_id = task.process_id
        urls_batch = task.urls_batch

        logger.info(f"[P{process_id}] starting with {len(urls_batch)} URLs")

        # Create semaphore for concurrent limiting
        concurrent_limit = task.config.concurrent_limit
        semaphore = asyncio.Semaphore(concurrent_limit)

        async def scrape_single_url(url):
            """Scrape a single URL with all setup and error handling"""
            local_counters = {
                "start": 0,
                "success": 0,
                "error": 0,
                "total": len(urls_batch),
            }

            async with semaphore:
                # Apply delay before starting
                await self._apply_delay(process_id)

                page = None
                try:
                    # Get a page from the pool
                    page = await page_pool.get_page()

                    # Setup page
                    timeout = task.config.timeout
                    page.set_default_timeout(timeout)
                    await self._setup_page_handlers(page, process_id)

                    # Log start
                    local_counters["start"] += 1
                    self._log_with_lock(
                        process_id=process_id,
                        status="start",
                        url=url,
                        counters=counters,
                        start_time=start_time,
                        local_counters=local_counters,
                    )

                    # Navigate and extract data
                    await page.goto(url, wait_until=task.config.wait_until)

                    # Extract data
                    result = await page.evaluate(task.parsing_script)
                    result["url"] = url

                    # Log success
                    local_counters["success"] += 1
                    self._log_with_lock(
                        process_id=process_id,
                        status="success",
                        url=url,
                        counters=counters,
                        start_time=start_time,
                        local_counters=local_counters,
                    )

                    # Return page to pool and return result
                    await page_pool.return_page(page)
                    return result

                except Exception as e:
                    result = {"url": url, "error": str(e)}

                    # Log error
                    local_counters["error"] += 1
                    self._log_with_lock(
                        process_id=process_id,
                        status="error",
                        url=url,
                        error_msg=str(e),
                        counters=counters,
                        start_time=start_time,
                        local_counters=local_counters,
                    )

                    # Return page to pool and return error result
                    if page:
                        await page_pool.return_page(page)
                    return result

        # Create tasks for all URLs
        tasks = [scrape_single_url(url) for url in urls_batch]
        results = await asyncio.gather(*tasks)

        logger.info(f"[P{process_id}] completed {len(results)} URLs")
        return results

    async def _run_scraper(self, tasks, urls):
        """Run scraper with all tasks in a single browser instance"""
        # Initialize counters
        counters = {
            "start": ThreadingValue("i", 0),
            "success": ThreadingValue("i", 0),
            "error": ThreadingValue("i", 0),
            "total": ThreadingValue("i", len(urls)),
            "start_time": ThreadingValue("d", time.time())
        }
        start_time = ThreadingValue("d", time.time())
        # Initialize playwright and browser
        async with async_playwright() as playwright:
            # Launch a single browser for all tasks
            launch_options = self.config.browser.get_launch_options()
            browser = await playwright.chromium.launch(**launch_options)

            # Create page pools for each task
            page_pools = []
            for task in tasks:
                # Create context with proxy if needed
                context_options = task.config.context.get_context_options()

                if task.proxy_config:
                    context_options["proxy"] = task.proxy_config

                context = await browser.new_context(**context_options)

                # Create page pool for this context
                pool = PagePool(context, size=task.config.concurrent_limit)
                await pool.initialize()
                page_pools.append(pool)

            # Run all tasks concurrently
            worker_tasks = [
                self._scrape_worker(task, page_pools[i], counters, start_time)
                for i, task in enumerate(tasks)
            ]

            all_results = []
            for results in await asyncio.gather(*worker_tasks):
                all_results.extend(results)

            # Close the browser
            await browser.close()

            return all_results

    def scrape_all(self, urls: List[str]) -> List[Dict]:
        """Main method to scrape all URLs with shared browser"""
        n = min(self.config.num_processes, len(urls))
        indices = [len(urls) * i // n for i in range(n + 1)]
        url_chunks = [urls[indices[i]: indices[i + 1]] for i in range(n)]

        # Create scraping tasks for each chunk
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
            )
            tasks.append(task)

        # Run the async event loop
        return asyncio.run(self._run_scraper(tasks, urls))
