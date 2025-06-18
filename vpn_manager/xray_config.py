"""Xray process and configuration management."""

import json
import time
import subprocess
from pathlib import Path
from typing import Dict, List


class XrayConfig:
    """Manages the Xray process and configuration."""

    def __init__(self, config_path: str = "xray_auto_generated.json"):
        """Initialize XrayManager."""
        self.config_path = config_path
        self.xray_process = None

    def generate_config(
        self, servers: List[Dict], proxy_mapping: Dict[str, int]
    ) -> str:
        """Generate Xray configuration file."""
        config = {
            "log": {"loglevel": "warning"},
            "inbounds": [],
            "outbounds": [],
            "routing": {"rules": []},
        }

        # Create inbounds and outbounds for each server
        for server in sorted(servers, key=lambda x: x["name"]):
            server_name = server["name"]
            port = proxy_mapping[server_name]

            # SOCKS5 inbound
            inbound = {
                "tag": f"socks-{server_name}",
                "port": port,
                "listen": "127.0.0.1",
                "protocol": "socks",
                "settings": {"auth": "noauth", "udp": True},
            }
            config["inbounds"].append(inbound)

            # VLESS outbound
            outbound = {
                "tag": f"{server_name}-out",
                "protocol": "vless",
                "settings": {
                    "vnext": [
                        {
                            "address": server["server"],
                            "port": server["server_port"],
                            "users": [
                                {
                                    "id": server["uuid"],
                                    "encryption": "none",
                                    "flow": server["flow"],
                                }
                            ],
                        }
                    ]
                },
                "streamSettings": {
                    "network": "tcp",
                    "security": "reality",
                    "realitySettings": {
                        "serverName": server["tls"]["server_name"],
                        "fingerprint": "chrome",
                        "publicKey": server["tls"]["reality"]["public_key"],
                        "shortId": server["tls"]["reality"]["short_id"],
                    },
                },
            }
            config["outbounds"].append(outbound)

            # Routing rule
            rule = {
                "type": "field",
                "inboundTag": [f"socks-{server_name}"],
                "outboundTag": f"{server_name}-out",
            }
            config["routing"]["rules"].append(rule)

        # Add default direct outbound
        config["outbounds"].append({"tag": "direct", "protocol": "freedom"})

        # Ensure directory exists
        Path(self.config_path).parent.mkdir(exist_ok=True, parents=True)

        # Write config file
        with open(self.config_path, "w") as f:
            json.dump(config, f, indent=2)

        return self.config_path
