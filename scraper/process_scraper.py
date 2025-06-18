# process_scraper.py

import asyncio
import logging
import multiprocessing
from typing import Dict, List

from playwright.async_api import async_playwright

from scraper.scraper_core import (
    ScraperConfig,
    ScrapingTask,
    ScraperStrategy,
    ScraperUtils,
)

logger = logging.getLogger(__name__)


class ScraperProcess(multiprocessing.Process):
    """Process class for scraping"""

    def __init__(self, task, counters, start_time, results_list, shared_state=None):
        super().__init__()
        self.task = task
        self.counters = counters
        self.start_time = start_time
        self.results = results_list
        self.shared_state = shared_state  # For proxy switching coordination

    def run(self):
        """Run the scraping process"""
        results = asyncio.run(self._scrape_batch())
        # Copy results to the shared list
        for result in results:
            self.results.append(result)

    async def _scrape_batch(self):
        """Process runs this async function with simplified parameters"""
        process_id = self.task.process_id
        urls_batch = self.task.urls_batch
        logger.info(f"[P{process_id}] starting with {len(urls_batch)} URLs")

        # Initialize browser and context pool
        playwright = await async_playwright().start()
        browser = await self._setup_browser(playwright)
        contexts = await self._create_context_pool(browser)

        # Create semaphore for concurrent limiting
        concurrent_limit = self.task.config.concurrent_limit
        semaphore = asyncio.Semaphore(concurrent_limit)

        # Shared state for proxy switching (accessible from concurrent tasks)
        proxy_state = {
            "consecutive_errors": 0,
            "error_threshold": 3,
            "contexts": contexts,
            "lock": asyncio.Lock(),
        }

        async def scrape_with_proxy_switching(url, index):
            """Scrape URL with proxy switching capability."""
            nonlocal proxy_state, browser

            while True:  # Retry loop for proxy switching
                try:
                    result = await self._scrape_single_url(
                        url, index, proxy_state["contexts"], semaphore
                    )

                    # Reset error count on success
                    async with proxy_state["lock"]:
                        proxy_state["consecutive_errors"] = 0

                    return result

                except Exception as e:
                    error_str = str(e)

                    # Check if it's a network error
                    network_error_patterns = [
                        "ERR_CONNECTION_CLOSED",
                        "ERR_PROXY_CONNECTION_FAILED",
                        "ERR_TUNNEL_CONNECTION_FAILED",
                        "ERR_SOCKS_CONNECTION_FAILED",
                        "ERR_CONNECTION_RESET",
                        "ERR_CONNECTION_REFUSED",
                        "ERR_NAME_NOT_RESOLVED"
                    ]

                    is_network_error = any(
                        pattern in error_str for pattern in network_error_patterns
                    )

                    if is_network_error and self.task.vpn_manager:
                        async with proxy_state["lock"]:
                            proxy_state["consecutive_errors"] += 1

                            if (
                                proxy_state["consecutive_errors"]
                                >= proxy_state["error_threshold"]
                            ):
                                logger.error(
                                    f"[P{process_id}] {proxy_state['error_threshold']} consecutive network errors, switching proxy"
                                )

                                # Re-test VPNs and get new proxy
                                self.task.vpn_manager._run_speed_test_and_sort_servers()
                                self.task.proxy_config = self.task.vpn_manager.get_proxy(process_id)

                                # Recreate contexts with new proxy
                                for ctx in proxy_state["contexts"]:
                                    await ctx.close()
                                proxy_state["contexts"] = (
                                    await self._create_context_pool(browser)
                                )
                                proxy_state["consecutive_errors"] = 0

                                logger.info(
                                    f"[P{process_id}] Re-tested VPNs, using proxy: {self.task.proxy_config.get('server_name', 'direct') if self.task.proxy_config else 'direct'}"
                                )
                                continue  # Retry with new proxy

                    # Return error result if not a network error or proxy switching failed
                    return {"url": url, "error": error_str}

        # Scrape all URLs concurrently
        tasks = [
            scrape_with_proxy_switching(url, i) for i, url in enumerate(urls_batch)
        ]
        results = await asyncio.gather(*tasks)

        # Cleanup resources
        await self._cleanup_resources(browser, contexts, playwright)

        logger.info(f"[P{process_id}] completed {len(results)} URLs")
        return results

    async def _setup_browser(self, playwright):
        launch_options = self.task.config.get_launch_options()
        return await playwright.chromium.launch(**launch_options)

    async def _create_context_pool(self, browser):
        """Create a pool of browser contexts for concurrent processing"""
        config = self.task.config
        contexts = []

        for i in range(config.concurrent_limit):
            context_options = config.get_context_options()

            if self.task.proxy_config:
                context_options["proxy"] = self.task.proxy_config

            context = await browser.new_context(**context_options)
            await context.set_extra_http_headers(config.extra_headers)
            contexts.append(context)

        return contexts

    async def _cleanup_resources(self, browser, contexts, playwright):
        """Clean up browser resources"""
        # Clean up contexts
        for context in contexts:
            await context.close()

        await browser.close()
        await playwright.stop()

    async def _scrape_single_url(self, url, index, contexts, semaphore):
        """Scrape a single URL with all necessary setup and error handling"""
        local_counters = {
            "start": 0,
            "success": 0,
            "error": 0,
            "total": len(self.task.urls_batch),
        }

        async with semaphore:
            # Apply delay before starting
            await ScraperUtils.apply_delay(self.task.config, self.task.process_id)

            page = None
            try:
                # Get a context from the pool (round-robin)
                context = contexts[index % len(contexts)]
                # Create fresh page for each URL
                page = await context.new_page()

                timeout = self.task.config.timeout
                page.set_default_timeout(timeout)
                await ScraperUtils.setup_page_handlers(page, self.task.process_id)

                local_counters["start"] += 1
                ScraperUtils.log_with_lock(
                    process_id=self.task.process_id,
                    status="start",
                    url=url,
                    counters=self.counters,
                    start_time=self.start_time,
                    local_counters=local_counters,
                )

                # Navigate and extract data
                await page.goto(url, wait_until=self.task.config.wait_until)
                result = await page.evaluate(self.task.parsing_script)
                result["url"] = url

                local_counters["success"] += 1
                ScraperUtils.log_with_lock(
                    process_id=self.task.process_id,
                    status="success",
                    url=url,
                    counters=self.counters,
                    start_time=self.start_time,
                    local_counters=local_counters,
                )
                return result

            except Exception as e:
                result = {"url": url, "error": str(e)}

                local_counters["error"] += 1
                ScraperUtils.log_with_lock(
                    process_id=self.task.process_id,
                    status="error",
                    url=url,
                    counters=self.counters,
                    start_time=self.start_time,
                    local_counters=local_counters,
                    error_msg=str(e),
                )
                return result

            finally:
                if page:
                    await page.close()


class ProcessScraperStrategy(ScraperStrategy):
    """Process-based scraper strategy using multiple OS processes"""

    async def execute_tasks(
        self,
        tasks: List[ScrapingTask],
        urls: List[str],
        parsing_script: str,
        config: ScraperConfig,
    ) -> List[Dict]:
        """Execute tasks using separate processes"""
        # Setup shared objects for inter-process communication
        manager = multiprocessing.Manager()
        results_list = manager.list()

        # Setup counters
        counters = {
            "start": multiprocessing.Value("i", 0),
            "success": multiprocessing.Value("i", 0),
            "error": multiprocessing.Value("i", 0),
            "total": multiprocessing.Value("i", len(urls)),
        }
        start_time = multiprocessing.Value("d", 0)
        import time

        start_time.value = time.time()

        # Create and start processes
        processes = []
        for task in tasks:
            process = ScraperProcess(task, counters, start_time, results_list)
            processes.append(process)
            process.start()

        # Wait for all processes to complete
        for process in processes:
            process.join()

        # Convert results from Manager.list to regular list
        return list(results_list)
