"""Proxy configuration and testing."""

import concurrent.futures
from typing import Dict, List
import time
import random
import requests
import logging
from collections import OrderedDict  # Import OrderedDict for sorted results

# Configure logger
logger = logging.getLogger(__name__)


class ServerPerformance:
    """Manages proxy configurations and testing."""

    def __init__(self, servers: List[Dict], proxy_mapping: Dict[str, int]):
        """Initialize ServerPerformance."""
        self.servers = servers
        self.proxy_mapping = proxy_mapping
        self.speed_results = {}

    def speed_test(self, servers, max_attempts=1, timeout=2) -> Dict[str, float]:
        """
        Test actual VPN proxy performance by routing requests through each VPN tunnel.

        Args:
            servers: List of server configs to test
            max_attempts: Maximum number of attempts per server (default: 1)
            timeout: Request timeout in seconds (default: 2)
        """
        logger.info("\n" + "=" * 60)
        logger.info("🚀 VPN Proxy Performance Test")
        logger.info("=" * 60)

        speed_results = {}

        # Use flexible test URLs - can be expanded later
        test_urls = [
            "https://www.cloudflare.com/cdn-cgi/trace",  # Primary test URL
            "https://ipinfo.io/json",  # Backup URLs below
            "https://api.ipify.org?format=json",
            "https://1.1.1.1/cdn-cgi/trace",
        ]

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            futures = []

            for server in servers:
                server_name = server["name"]

                # Get the SOCKS5 proxy for this server
                proxy_port = self.proxy_mapping.get(server_name)
                if not proxy_port:
                    speed_results[server_name] = float("inf")
                    continue

                # Submit the server testing task with parameters
                future = executor.submit(
                    lambda s=server_name, p=proxy_port: self._test_server(
                        s, p, test_urls, max_attempts, timeout
                    )
                )
                futures.append((future, server_name))

            # Process results as they complete
            for future, name in futures:
                try:
                    latency = future.result()
                    speed_results[name] = latency
                except Exception:
                    speed_results[name] = float("inf")

        # Display results
        logger.info("\n📊 VPN Servers by Real Proxy Performance:")
        working_servers = [
            (name, speed)
            for name, speed in speed_results.items()
            if speed != float("inf")
        ]

        working_servers.sort(key=lambda x: x[1])  # Sort by latency

        for name, latency in working_servers:
            logger.info(f"{name}: {latency:.0f} ms")

        failed_servers = [
            name for name, speed in speed_results.items() if speed == float("inf")
        ]
        if failed_servers:
            logger.warning(f"\n❌ Failed VPN connections: {', '.join(failed_servers)}")

        logger.info(
            f"\n✅ Found {len(working_servers)} working VPN proxies out of {len(servers)}"
        )

        # Return results with small variations to prevent exact ties
        final_results = {}
        for name, latency in speed_results.items():
            if latency == float("inf"):
                final_results[name] = float("inf")
            else:
                # Add tiny random variation to prevent exact ties
                final_results[name] = latency + random.uniform(0, 1.0)

        # Sort all servers by latency and create ordered result
        # First, separate working and failed servers
        working_items = [(k, v) for k, v in speed_results.items() if v != float("inf")]
        failed_items = [(k, float("inf")) for k in failed_servers]

        # Sort working servers by latency
        working_items.sort(key=lambda x: x[1])

        # Create OrderedDict with working servers first (sorted by speed), then failed servers
        final_results = OrderedDict(working_items)

        return final_results

    def _test_server(
        self, server_name, proxy_port, test_urls, max_attempts=1, timeout=2
    ):
        """
        Test a server with configurable attempts and URLs.

        Args:
            server_name: Name of the server being tested
            proxy_port: SOCKS5 proxy port
            test_urls: List of URLs to test with
            max_attempts: Maximum number of attempts (default: 1)
            timeout: Request timeout in seconds (default: 2)
        """
        proxy_url = f"socks5://127.0.0.1:{proxy_port}"
        logger.debug(f"🔄 Testing {server_name} via {proxy_url}...")

        measurements = []
        end_time = time.perf_counter() + (
            timeout * max_attempts
        )  # Dynamic timeout based on attempts

        # Only try up to the number of attempts specified
        for attempt in range(1, max_attempts + 1):
            if time.perf_counter() > end_time:
                logger.debug(f"  ⏱️ Time limit exceeded for {server_name}")
                break

            # Select URL from the list (cycling through if needed)
            test_url = test_urls[(attempt - 1) % len(test_urls)]
            logger.debug(
                f"  📡 Attempt {attempt}/{max_attempts} for {server_name}: {test_url}"
            )

            try:
                session = requests.Session()
                session.proxies = {"http": proxy_url, "https": proxy_url}

                start = time.perf_counter()
                response = session.get(
                    test_url,
                    timeout=timeout,
                    headers={"User-Agent": "VPN-Test/1.0"},
                )

                if response.status_code == 200:
                    latency = (time.perf_counter() - start) * 1000
                    measurements.append(latency)
                    logger.debug(f"  ✅ Success - {server_name}: {latency:.0f}ms")
                else:
                    logger.debug(
                        f"  ❌ Failed - {server_name}: Status {response.status_code}"
                    )

            except requests.exceptions.Timeout:
                logger.debug(f"  ⏱️ Timeout - {server_name}")
            except Exception as e:
                logger.debug(f"  ❌ Error - {server_name}: {type(e).__name__}")

            # Only do additional attempts if we haven't succeeded yet
            if measurements and max_attempts > 1:
                break

        if measurements:
            # If only one measurement, return it directly
            if len(measurements) == 1:
                result = measurements[0]
            else:
                # Otherwise average the measurements
                result = sum(measurements) / len(measurements)

            logger.debug(f"📊 Final result for {server_name}: {result:.0f}ms")
            return result
        else:
            logger.debug(
                f"📊 Final result for {server_name}: No successful measurements"
            )
            return float("inf")


def run_speed_test(servers: List[Dict], proxy_mapping: Dict[str, int], 
                   max_attempts=1, timeout=2) -> Dict[str, float]:
    """
    Standalone function to run VPN speed test without creating persistent class instance.
    
    Args:
        servers: List of server configs to test
        proxy_mapping: Mapping of server names to proxy ports
        max_attempts: Maximum number of attempts per server
        timeout: Request timeout in seconds
        
    Returns:
        Dictionary with speed test results
    """
    # Create temporary instance just for testing
    tester = ServerPerformance(servers, proxy_mapping)
    results = tester.speed_test(servers, max_attempts, timeout)
    
    # Delete the instance to avoid any threading references
    del tester
    
    return results