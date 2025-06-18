"""VPN Manager - Main module coordinating proxy and xray functionality."""

import json
from pathlib import Path
from typing import Dict, List, Optional
import time
import subprocess

try:
    from vpn_manager.xray_config import XrayConfig
    from vpn_manager.server_performance import run_speed_test
except ImportError:
    from xray_config import XrayConfig
    from server_performance import run_speed_test


class VPNManager:
    """Manages VPN connections for scraping through multiple servers."""

    def __init__(
        self,
        servers_dir: str = "vpn_manager/servers",
        config_path: str = "vpn_manager/xray_auto_generated.json",
    ):
        """Initialize VPN Manager."""
        self.servers_dir = Path(servers_dir)
        self.config_path = Path(config_path)
        self.vpn_servers = self._load_vpn_servers()
        self.proxy_mapping = self._create_proxy_mapping()
        self.speed_results = {}
        self.sorted_servers = None
        self.xray_config = XrayConfig(self.config_path)
        self.run_xray()
        self._run_speed_test_and_sort_servers()

    def _load_vpn_servers(self) -> List[Dict]:
        """Load all VPN server configurations from the servers directory."""
        servers = []
        for file_path in self.servers_dir.glob("*.json"):
            with open(file_path, "r") as f:
                server_config = json.load(f)
                server_config["name"] = file_path.stem
                servers.append(server_config)
        return servers

    def _create_proxy_mapping(self) -> Dict[str, int]:
        """Create proxy port mappings based on server configurations."""
        proxy_mapping = {}
        base_port = 10801
        alphabetical_servers = sorted(self.vpn_servers, key=lambda x: x["name"])
        for i, server in enumerate(alphabetical_servers):
            proxy_mapping[server["name"]] = base_port + i
        return proxy_mapping

    def _run_speed_test_and_sort_servers(self):
        """Run speed test on all servers and get pre-sorted results."""
        self.speed_results = run_speed_test(self.vpn_servers, self.proxy_mapping)
        self.sorted_servers = []
        for server_name, latency in self.speed_results.items():
            server = next(s for s in self.vpn_servers if s["name"] == server_name)
            self.sorted_servers.append(server)

    def _create_proxy_config(self, server_name: str) -> Optional[Dict]:
        """Create proxy configuration for a server."""
        port = self.proxy_mapping.get(server_name)
        if port:
            proxy = {"server": f"socks5://127.0.0.1:{port}", "server_name": server_name}
            print(f"{proxy['server_name']}: {proxy['server']}")
            return proxy
        return None

    def get_proxy(self, identifier, exclude_servers=["russia"]) -> Optional[Dict]:
        """Get proxy by index (returns best servers first) or by server name.

        Args:
            identifier: String server name or numeric index
            exclude_servers: Optional list/set of server names to exclude when using numeric index
        Returns:
            Dict with proxy config, None for direct connection, or raises exception
        """

        available_servers = [
            server for server in self.sorted_servers
            if not (exclude_servers and server["name"] in exclude_servers)
        ]

        if not available_servers:
            print("⚠️ No working VPN servers available, using direct connection")
            return None

        if isinstance(identifier, str):
            if not any(s["name"] == identifier for s in available_servers):
                raise Exception(f"VPN server '{identifier}' is not working")
            proxy = self._create_proxy_config(identifier)
            return proxy

        server = available_servers[identifier % len(available_servers)]
        server_name = server["name"]
        proxy = self._create_proxy_config(server_name)

        return proxy

    def run_xray(self) -> bool:
        """Ensure Xray is running with the provided configuration."""
        # Check and stop any existing processes
        try:
            result = subprocess.run(["pgrep", "xray"], capture_output=True, text=True)
            if result.returncode == 0:
                print("🔄 Stopping existing xray to start with new config...")
                pids = result.stdout.strip().split("\n")
                for pid in pids:
                    if pid:
                        print(f"🛑 Stopping existing xray process {pid}")
                        subprocess.run(["kill", pid], capture_output=True)
                time.sleep(1)  # Wait for processes to stop
        except Exception:
            # If we can't check or kill processes, just continue to start attempt
            pass

        print("⚠️ Starting xray...")
        config_path = self.xray_config.generate_config(
            self.vpn_servers, self.proxy_mapping
        )

        try:
            print(f"🚀 Starting xray with configuration: {config_path}")
            xray_process = subprocess.Popen(
                ["xray", "run", "-c", config_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time.sleep(2)  # Wait a moment for xray to start

            if xray_process.poll() is None:
                print("✅ Xray started successfully")
                return True
            else:
                print("❌ Xray failed to start")
                return False
        except Exception as e:
            print(f"❌ Failed to start xray: {e}")
            return False

    def __del__(self):
        """Clean up resources when object is destroyed."""
        # No longer storing xray_process, so no cleanup needed
        pass
