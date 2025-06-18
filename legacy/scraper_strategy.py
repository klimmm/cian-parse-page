import asyncio
import logging
import random
import time
import threading
import multiprocessing
from dataclasses import dataclass
from typing import Dict, List, Literal, Optional, Any
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
from scraper.scraper_config import ScraperConfig, BrowserConfig, ContextConfig, ProcessConfig
from scraper.scraper_thread import ThreadScraperStrategy
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Data Structures and Configuration
# -----------------------------------------------------------------------------

@dataclass
class ScrapingTask:
    """Task definition for scraping a batch of URLs"""
    urls_batch: List[str]
    process_id: int
    parsing_script: str
    config: ScraperConfig
    proxy_config: Optional[Dict] = None




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


# -----------------------------------------------------------------------------
# Utility Class with Shared Functions
# -----------------------------------------------------------------------------

class ScraperUtils:
    """Static utility methods shared by different scraper strategies"""
    
    @staticmethod
    async def setup_page_handlers(page: Page, process_id: int):
        """Set up event handlers for the page"""
        def handle_console_message(msg):
            text = msg.text
            if any(pattern in text for pattern in [
                "[AdFox]", "Minified React error", 
                "reactjs.org/docs/error-decoder", "Unknown bundle undefined"
            ]):
                return
            logger.debug(f"[P{process_id}] JS Console ({msg.type}): {text}")
        
        page.on("console", handle_console_message)
        
        def handle_page_error(error):
            error_text = str(error)
            if any(pattern in error_text for pattern in [
                "Minified React error", "reactjs.org/docs/error-decoder"
            ]):
                return
            logger.info(f"[P{process_id}] JS Error: {error_text}")
        
        page.on("pageerror", handle_page_error)
    
    @staticmethod
    async def apply_delay(config: ScraperConfig, process_id: int):
        """Calculate and apply a random delay between requests"""
        delay = config.delay_base + random.uniform(
            config.delay_min, config.delay_max
        )
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
            
            # Call unified logging function - assumed to be imported from utils
            # For demonstration, we'll use a simple logging approach here
            elapsed = time.time() - start_time.value
            total = counters["total"].value
            success = counters["success"].value
            error = counters["error"].value
            
            status_indicator = {
                "start": "⏱️",
                "success": "✅",
                "error": "❌"
            }.get(status, "")
            
            if status == "error" and error_msg:
                logger.error(f"[P{process_id}] {status_indicator} {status.upper()} [{success}/{error}/{total}] {elapsed:.1f}s {url} - {error_msg}")
            else:
                logger.info(f"[P{process_id}] {status_indicator} {status.upper()} [{success}/{error}/{total}] {elapsed:.1f}s {url}")


# -----------------------------------------------------------------------------
# Strategy Interface
# -----------------------------------------------------------------------------

class ScraperStrategy:
    """Interface for scraper execution strategies"""
    
    async def execute_tasks(self, tasks: List[ScrapingTask], urls: List[str], 
                           parsing_script: str, config: ScraperConfig) -> List[Dict]:
        """Execute scraping tasks and return results"""
        raise NotImplementedError("Strategies must implement this method")


# -----------------------------------------------------------------------------
# Thread-Based Strategy Implementation
# -----------------------------------------------------------------------------

class ThreadScraperStrategy(ScraperStrategy):
    """Thread-based scraper strategy using page pooling"""
    
    async def execute_tasks(self, tasks: List[ScrapingTask], urls: List[str], 
                           parsing_script: str, config: ScraperConfig) -> List[Dict]:
        """Execute tasks using asyncio in a single process"""
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
            launch_options = config.browser.get_launch_options()
            browser = await playwright.chromium.launch(**launch_options)
            
            # Create page pools for each task
            page_pools = []
            for task in tasks:
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
    
    async def _scrape_worker(self, task: ScrapingTask, page_pool: PagePool, 
                           counters: Dict, start_time: ThreadingValue) -> List[Dict]:
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

                    # Return page to pool and return result
                    await page_pool.return_page(page)
                    return result

                except Exception as e:
                    result = {"url": url, "error": str(e)}

                    # Log error
                    local_counters["error"] += 1
                    ScraperUtils.log_with_lock(
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


# -----------------------------------------------------------------------------
# Process-Based Strategy Implementation
# -----------------------------------------------------------------------------

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
        playwright = await async_playwright().start()
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

    async def _setup_browser(self, playwright):
        launch_options = self.task.config.browser.get_launch_options()
        return await playwright.chromium.launch(**launch_options)

    async def _create_context_pool(self, browser):
        """Create a pool of browser contexts for concurrent processing"""
        config = self.task.config
        contexts = []

        for i in range(config.concurrent_limit):
            context_options = config.context.get_context_options()
            
            if self.task.proxy_config:
                context_options["proxy"] = self.task.proxy_config
                
            context = await browser.new_context(**context_options)
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
                    local_counters=local_counters
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
                    local_counters=local_counters
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
    
    async def execute_tasks(self, tasks: List[ScrapingTask], urls: List[str], 
                           parsing_script: str, config: ScraperConfig) -> List[Dict]:
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
        start_time = multiprocessing.Value("d", time.time())
        
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


# -----------------------------------------------------------------------------
# Main Scraper Class
# -----------------------------------------------------------------------------

class BaseScraper:
    """Base scraper that can use different execution strategies"""
    
    def __init__(self, parsing_script=None, config=None, vpn_manager=None, strategy=None):
        self.parsing_script = parsing_script
        self.config = config or ScraperConfig()
        self.vpn_manager = vpn_manager
        self.strategy = strategy or ThreadScraperStrategy()  # Default to thread-based
    
    def set_strategy(self, strategy: ScraperStrategy):
        """Change the scraping strategy at runtime"""
        self.strategy = strategy
        return self
    
    def scrape_all(self, urls: List[str]) -> List[Dict]:
        """Main entry point - delegates to the selected strategy"""
        n = min(self.config.num_processes, len(urls))
        indices = [len(urls) * i // n for i in range(n + 1)]
        url_chunks = [urls[indices[i]: indices[i + 1]] for i in range(n)]
        
        # Create scraping tasks
        tasks = self._create_tasks(url_chunks)
        
        # Execute using the selected strategy
        return asyncio.run(self.strategy.execute_tasks(
            tasks, urls, self.parsing_script, self.config
        ))
    
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
            )
            tasks.append(task)
        return tasks


# -----------------------------------------------------------------------------
# Factory Functions
# -----------------------------------------------------------------------------

def create_thread_scraper(**kwargs):
    """Create a scraper with thread-based strategy"""
    return BaseScraper(strategy=ThreadScraperStrategy(), **kwargs)

def create_process_scraper(**kwargs):
    """Create a scraper with process-based strategy"""
    return BaseScraper(strategy=ProcessScraperStrategy(), **kwargs)


