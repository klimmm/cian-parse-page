# thread_scraper.py

import asyncio
import logging
import threading
from typing import Dict, List
from playwright.async_api import async_playwright, BrowserContext, Page
from scraper.scraper_core import (
    ScraperConfig,
    ScrapingTask,
    ScraperStrategy,
    ScraperUtils,
)

logger = logging.getLogger(__name__)


# Thread-safe counter for thread-based approach
class ThreadingValue:
    """Thread-safe value container similar to multiprocessing.Value"""

    def __init__(self, typecode, value=0):
        self.value = value
        self._lock = threading.Lock()

    def get_lock(self):
        return self._lock


# Page pool for the thread-based approach
class PagePool:
    """Pool of reusable Playwright pages"""

    def __init__(self, context: BrowserContext, size: int = 3):
        self.context = context
        self.size = size
        self.available = asyncio.Queue(maxsize=size)
        self._lock = asyncio.Lock()

    async def initialize(self):
        """Create initial pages in the pool"""
        for _ in range(self.size):
            page = await self.context.new_page()
            await self.available.put(page)

    async def get_page(self) -> Page:
        """Get a page from the pool, waiting if none available"""
        return await self.available.get()

    async def return_page(self, page: Page):
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


class ThreadScraperStrategy(ScraperStrategy):
    """Thread-based scraper strategy using page pooling with dynamic proxy switching"""

    def __init__(self, vpn_manager=None):
        self.vpn_manager = vpn_manager
        self.proxy_manager = DynamicProxyManager(vpn_manager) if vpn_manager else None

    async def execute_tasks(
        self,
        tasks: List[ScrapingTask],
        urls: List[str],
        parsing_script: str,
        config: ScraperConfig,
    ) -> List[Dict]:
        """Execute tasks using asyncio in a single process"""
        # Initialize counters
        counters = {
            "start": ThreadingValue("i", 0),
            "success": ThreadingValue("i", 0),
            "error": ThreadingValue("i", 0),
            "total": ThreadingValue("i", len(urls)),
            "start_time": ThreadingValue("d", 0),
        }
        import time

        counters["start_time"].value = time.time()
        start_time = counters["start_time"]

        # Initialize playwright and browser
        async with async_playwright() as playwright:
            # Launch a single browser for all tasks
            launch_options = config.get_launch_options()
            browser = await playwright.chromium.launch(**launch_options)

            # Create page pools for each task
            page_pools = []
            for task in tasks:
                context_options = task.config.get_context_options()

                if task.proxy_config:
                    context_options["proxy"] = task.proxy_config

                context = await browser.new_context(**context_options)
                await context.set_extra_http_headers(task.config.extra_headers)

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

    async def _scrape_worker(
        self,
        task: ScrapingTask,
        page_pool: PagePool,
        counters: Dict,
        start_time: ThreadingValue,
    ) -> List[Dict]:
        """Worker that scrapes URLs using a page pool"""
        process_id = task.process_id
        urls_batch = task.urls_batch

        logger.info(f"[P{process_id}] starting with {len(urls_batch)} URLs")

        # Create semaphore for concurrent limiting
        concurrent_limit = task.config.concurrent_limit
        semaphore = asyncio.Semaphore(concurrent_limit)

        # Track consecutive network errors and worker-level counters
        consecutive_network_errors = 0
        network_error_threshold = 3
        local_counters = {
            "start": 0,
            "success": 0,
            "error": 0,
            "total": len(urls_batch),
        }

        async def scrape_single_url(url):
            """Scrape a single URL with all setup and error handling"""
            nonlocal consecutive_network_errors

            async with semaphore:
                # Apply delay before starting
                await ScraperUtils.apply_delay(task.config, process_id)

                page = None
                try:
                    # Get a page from the pool
                    page = await page_pool.get_page()

                    # Setup page
                    timeout = task.config.timeout
                    page.set_default_timeout(timeout)
                    await ScraperUtils.setup_page_handlers(page, process_id)

                    # Log start
                    local_counters["start"] += 1
                    print(local_counters)

                    ScraperUtils.log_with_lock(
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
                    ScraperUtils.log_with_lock(
                        process_id=process_id,
                        status="success",
                        url=url,
                        counters=counters,
                        start_time=start_time,
                        local_counters=local_counters,
                    )

                    # Reset consecutive network errors on success
                    consecutive_network_errors = 0

                    # Return page to pool and return result
                    await page_pool.return_page(page)
                    return result

                except Exception as e:
                    error_str = str(e)
                    result = {"url": url, "error": error_str}

                    # Check if it's a network error
                    network_error_patterns = [
                        "ERR_CONNECTION_CLOSED",
                        "ERR_PROXY_CONNECTION_FAILED",
                        "ERR_TUNNEL_CONNECTION_FAILED",
                        "ERR_SOCKS_CONNECTION_FAILED",
                        "ERR_CONNECTION_RESET",
                        "ERR_CONNECTION_REFUSED",
                        "ERR_CONNECTION_TIMED_OUT",
                        "ERR_NETWORK_CHANGED",
                        "ERR_NAME_NOT_RESOLVED"
                    ]

                    is_network_error = any(
                        pattern in error_str for pattern in network_error_patterns
                    )

                    if is_network_error:
                        consecutive_network_errors += 1
                        if (
                            consecutive_network_errors >= network_error_threshold
                            and task.vpn_manager
                        ):
                            logger.error(
                                f"[P{process_id}] {network_error_threshold} "
                                f"consecutive network errors, re-testing VPN servers"
                            )

                            task.vpn_manager._run_speed_test_and_sort_servers()
                            task.proxy_config = task.vpn_manager.get_proxy(process_id)
                            consecutive_network_errors = 0

                            result["proxy_switched"] = True
                    else:
                        # Not a network error, reset counter
                        consecutive_network_errors = 0

                    # Log error
                    local_counters["error"] += 1
                    ScraperUtils.log_with_lock(
                        process_id=process_id,
                        status="error",
                        url=url,
                        error_msg=error_str,
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
