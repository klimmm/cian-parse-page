import logging
from dataclasses import dataclass
from typing import Dict, List, Optional
import random

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class BrowserConfig:
    """Browser launch configuration"""
    headless: bool = True
    browser_args: List[str] = None
    
    def __post_init__(self):
        if self.browser_args is None:
            self.browser_args = [
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ]
    
    def get_launch_options(self) -> Dict:
        """Get Playwright browser launch options"""
        return {
            "headless": self.headless,
            "args": self.browser_args,
        }


@dataclass 
class ContextConfig:
    """Browser context configuration"""
    viewport: Dict[str, int] = None
    locale: str = "ru-RU"
    timezone: str = "Europe/Moscow"
    user_agents: List[str] = None
    extra_headers: Dict[str, str] = None
    bypass_csp: bool = True
    
    def __post_init__(self):
        if self.viewport is None:
            self.viewport = {"width": 1920, "height": 1080}
            
        if self.user_agents is None:
            self.user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            ]
            
        if self.extra_headers is None:
            self.extra_headers = {
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"
            }
    
    def get_context_options(self) -> Dict:
        """Get Playwright context options"""
        return {
            "viewport": self.viewport,
            "user_agent": random.choice(self.user_agents),
            "locale": self.locale,
            "timezone_id": self.timezone,
            "bypass_csp": self.bypass_csp,
            "extra_http_headers": self.extra_headers,
        }


@dataclass
class TimingConfig:
    """Timing and delay configuration"""
    delay_base: float = 0.2
    delay_min: float = 0.2
    delay_max: float = 1.0
    timeout: int = 30000
    wait_until: str = "domcontentloaded"


@dataclass
class ProcessConfig:
    """Process and concurrency configuration"""
    concurrent_limit: int = 2
    num_processes: int = 1


@dataclass
class ScraperConfig:
    """Main scraper configuration composed of modular blocks"""
    
    browser: BrowserConfig = None
    context: ContextConfig = None
    timing: TimingConfig = None
    process: ProcessConfig = None
    
    def __post_init__(self):
        if self.browser is None:
            self.browser = BrowserConfig()
        if self.context is None:
            self.context = ContextConfig()
        if self.timing is None:
            self.timing = TimingConfig()
        if self.process is None:
            self.process = ProcessConfig()
    
    # Backward compatibility properties
    @property
    def headless(self):
        return self.browser.headless
    
    @property
    def browser_args(self):
        return self.browser.browser_args
        
    @property
    def viewport(self):
        return self.context.viewport
        
    @property
    def user_agents(self):
        return self.context.user_agents
        
    @property
    def locale(self):
        return self.context.locale
        
    @property
    def timezone(self):
        return self.context.timezone
        
    @property
    def bypass_csp(self):
        return self.context.bypass_csp
        
    @property
    def timeout(self):
        return self.timing.timeout
        
    @property
    def num_processes(self):
        return self.process.num_processes
        
    @property
    def concurrent_limit(self):
        return self.process.concurrent_limit
        
    @property
    def delay_base(self):
        return self.timing.delay_base
        
    @property
    def delay_min(self):
        return self.timing.delay_min
        
    @property
    def delay_max(self):
        return self.timing.delay_max
        
    @property
    def wait_until(self):
        return self.timing.wait_until
        
    @property
    def extra_headers(self):
        return self.context.extra_headers
