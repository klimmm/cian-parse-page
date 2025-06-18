import asyncio
from playwright.async_api import async_playwright
from typing import Literal
import logging
import random
import multiprocessing
from utils.utils import log_process_with_stats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ScraperProcess(multiprocessing.Process):
    """Process class for scraping"""

    def __init__(self, task, counters, start_time, results_list):
        super().__init__()
        self.task = task
        self.counters = counters
        self.start_time = start_time
        self.results = results_list

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
        playwright = await self._initialize_playwright()
        browser = await self._setup_browser(playwright)
        contexts = await self._create_context_pool(browser)

        # Create semaphore for concurrent limiting
        concurrent_limit = self.task.config.concurrent_limit
        semaphore = asyncio.Semaphore(concurrent_limit)

        # Scrape all URLs
        tasks = [
            self._scrape_single_url(url, i, contexts, semaphore)
            for i, url in enumerate(urls_batch)
        ]
        results = await asyncio.gather(*tasks)

        # Cleanup resources
        await self._cleanup_resources(browser, contexts, playwright)

        logger.info(f"[P{process_id}] completed {len(results)} URLs")
        return results

    async def _initialize_playwright(self):
        return await async_playwright().start()

    async def _setup_browser(self, playwright):
        launch_options = {
            "headless": self.task.config.headless,
            "args": self.task.config.browser_args,
        }
        if self.task.proxy_config:
            launch_options["proxy"] = self.task.proxy_config
        return await playwright.chromium.launch(**launch_options)

    async def _create_context_pool(self, browser):
        """Create a pool of browser contexts for concurrent processing"""
        config = self.task.config
        contexts = []

        for i in range(config.concurrent_limit):
            context = await browser.new_context(
                viewport=config.viewport,
                user_agent=random.choice(config.user_agents),
                locale=config.locale,
                timezone_id=config.timezone,
                bypass_csp=config.bypass_csp,
            )
            await context.set_extra_http_headers(config.extra_headers)
            contexts.append(context)

        return contexts

    async def _setup_page_handlers(self, page):
        """Set up event handlers for the page"""

        # Add console log listener to capture JS logs with filtering
        def handle_console_message(msg):
            text = msg.text
            # Filter out known third-party warnings/errors
            if any(
                pattern in text
                for pattern in [
                    "[AdFox]",
                    "Minified React error",
                    "reactjs.org/docs/error-decoder",
                    "Unknown bundle undefined",
                ]
            ):
                return  # Skip these messages
            logger.debug(f"[P{self.task.process_id}] JS Console ({msg.type}): {text}")

        page.on("console", handle_console_message)

        def handle_page_error(error):
            error_text = str(error)
            # Filter out known third-party errors
            if any(
                pattern in error_text
                for pattern in [
                    "Minified React error",
                    "reactjs.org/docs/error-decoder",
                ]
            ):
                return  # Skip these errors
            logger.info(f"[P{self.task.process_id}] JS Error: {error_text}")

        page.on("pageerror", handle_page_error)

    async def _apply_delay(self):
        """Calculate and apply a random delay between requests"""
        config = self.task.config
        delay = config.delay_base + random.uniform(config.delay_min, config.delay_max)
        logger.info(f"[P{self.task.process_id}] delay: {delay:.1f}s")
        await asyncio.sleep(delay)

    async def _navigate_and_scrape(self, page, url):
        """Navigate to URL and scrape data"""
        await page.goto(url, wait_until=self.task.config.wait_until)
        result = await page.evaluate(self.task.parsing_script)
        result["url"] = url
        return result

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
            await self._apply_delay()

            page = None
            try:
                # Get a context from the pool (round-robin)
                context = contexts[index % len(contexts)]
                # Create fresh page for each URL
                page = await context.new_page()

                timeout = self.task.config.timeout
                page.set_default_timeout(timeout)
                await self._setup_page_handlers(page)

                local_counters["start"] += 1
                self._log_with_lock(
                    status="start", url=url, local_counters=local_counters
                )

                result = await self._navigate_and_scrape(page, url)

                local_counters["success"] += 1
                self._log_with_lock(
                    status="success", url=url, local_counters=local_counters
                )
                return result

            except Exception as e:
                result = {"url": url, "error": str(e)}

                local_counters["error"] += 1
                self._log_with_lock(
                    status="error",
                    url=url,
                    local_counters=local_counters,
                    error_msg=str(e),
                )
                return result

            finally:
                if page:
                    await page.close()

    def _log_with_lock(
        self,
        status: Literal["start", "success", "error"],
        url: str = "",
        error_msg: str = "",
        local_counters=None,
    ) -> None:
        """Log messages with counter lock and consistent formatting"""
        with self.counters[status].get_lock():
            self.counters[status].value += 1
            process_id = self.task.process_id

            # Call unified logging function
            log_process_with_stats(
                process_id,
                status,
                global_counters=self.counters,
                start_time_value=self.start_time.value,
                url=url,
                error_msg=error_msg,
                local_counters=local_counters,
            )