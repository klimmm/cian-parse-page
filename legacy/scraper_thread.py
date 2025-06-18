# -----------------------------------------------------------------------------
# Thread-Based Strategy Implementation
# -----------------------------------------------------------------------------



# Thread-safe counter for thread-based approach
class ThreadingValue:
    """Thread-safe value container similar to multiprocessing.Value"""
    
    def __init__(self, typecode, value=0):
        self.value = value
        self._lock = threading.Lock()
    
    def get_lock(self):
        return self._lock

class ScraperStrategy:
    """Interface for scraper execution strategies"""
    
    async def execute_tasks(self, tasks: List[ScrapingTask], urls: List[str], 
                           parsing_script: str, config: ScraperConfig) -> List[Dict]:
        """Execute scraping tasks and return results"""
        raise NotImplementedError("Strategies must implement this method")


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

