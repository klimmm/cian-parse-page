# memory_monitoring.py

import psutil
import time
import os
import logging
from typing import Dict, List, Optional, Callable
from threading import Thread, Event
from dataclasses import dataclass, field
import pandas as pd
from datetime import datetime
import threading
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)


@dataclass
class CPUStats:
    """Store CPU usage statistics"""

    timestamp: float
    process_cpu_percent: float
    system_cpu_percent: float
    memory_percent: float
    thread_count: int

    @property
    def formatted_time(self) -> str:
        """Return formatted timestamp"""
        return datetime.fromtimestamp(self.timestamp).strftime("%H:%M:%S")


@dataclass
class CPUMonitor:
    """Monitor CPU usage of the current process and system"""

    interval: float = 1.0  # Sampling interval in seconds
    history_size: int = 3600  # Number of samples to keep (1 hour at 1 second intervals)
    enabled: bool = True
    stats: List[CPUStats] = field(default_factory=list)
    process: psutil.Process = field(default_factory=lambda: psutil.Process(os.getpid()))
    _stop_event: threading.Event = field(default_factory=threading.Event)
    _monitor_thread: Optional[threading.Thread] = None
    on_threshold_exceeded: Optional[Callable[[CPUStats], None]] = None
    cpu_threshold: float = 80.0  # Threshold to trigger alerts (percentage)

    def start(self):
        """Start monitoring in a background thread"""
        if self._monitor_thread is not None and self._monitor_thread.is_alive():
            logger.warning("Monitor already running")
            return

        self._stop_event.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info(f"CPU monitoring started with {self.interval}s interval")

    def stop(self):
        """Stop the monitoring thread"""
        if self._monitor_thread is None:
            return

        self._stop_event.set()
        self._monitor_thread.join(timeout=2.0)
        logger.info("CPU monitoring stopped")

    def _monitor_loop(self):
        """Main monitoring loop that runs in a separate thread"""
        while not self._stop_event.is_set():
            try:
                # Collect stats
                self._collect_stats()

                # Sleep until next interval
                self._stop_event.wait(self.interval)
            except Exception as e:
                logger.error(f"Error in CPU monitoring: {str(e)}")
                time.sleep(self.interval)  # Sleep and try again

    def _collect_stats(self):
        """Collect current CPU and memory stats"""
        try:
            # Get process stats
            with self.process.oneshot():  # More efficient collection of multiple metrics
                process_cpu = self.process.cpu_percent()
                memory_percent = self.process.memory_percent()
                thread_count = len(self.process.threads())

            # Get system CPU usage
            system_cpu = psutil.cpu_percent()

            # Create stats object
            stats = CPUStats(
                timestamp=time.time(),
                process_cpu_percent=process_cpu,
                system_cpu_percent=system_cpu,
                memory_percent=memory_percent,
                thread_count=thread_count,
            )

            # Add to history, keeping the history size limit
            self.stats.append(stats)
            if len(self.stats) > self.history_size:
                self.stats = self.stats[-self.history_size :]

            # Check thresholds and trigger callback if needed
            if self.on_threshold_exceeded and (
                stats.process_cpu_percent > self.cpu_threshold
                or stats.system_cpu_percent > self.cpu_threshold
            ):
                self.on_threshold_exceeded(stats)

        except Exception as e:
            logger.error(f"Error collecting CPU stats: {str(e)}")

    def get_current_stats(self) -> Optional[CPUStats]:
        """Get the most recent stats"""
        if not self.stats:
            return None
        return self.stats[-1]

    def get_stats_summary(self) -> Dict[str, float]:
        """Get summary of stats (averages)"""
        if not self.stats:
            return {
                "avg_process_cpu": 0.0,
                "avg_system_cpu": 0.0,
                "avg_memory": 0.0,
                "avg_threads": 0,
                "max_process_cpu": 0.0,
                "max_system_cpu": 0.0,
            }

        avg_process_cpu = sum(s.process_cpu_percent for s in self.stats) / len(
            self.stats
        )
        avg_system_cpu = sum(s.system_cpu_percent for s in self.stats) / len(self.stats)
        avg_memory = sum(s.memory_percent for s in self.stats) / len(self.stats)
        avg_threads = sum(s.thread_count for s in self.stats) / len(self.stats)
        max_process_cpu = max(s.process_cpu_percent for s in self.stats)
        max_system_cpu = max(s.system_cpu_percent for s in self.stats)

        return {
            "avg_process_cpu": avg_process_cpu,
            "avg_system_cpu": avg_system_cpu,
            "avg_memory": avg_memory,
            "avg_threads": avg_threads,
            "max_process_cpu": max_process_cpu,
            "max_system_cpu": max_system_cpu,
        }

    def plot(self, save_path: Optional[str] = None):
        """Generate a plot of CPU usage over time"""
        if not self.stats:
            logger.warning("No stats to plot")
            return

        # Extract data for plotting
        timestamps = [s.timestamp for s in self.stats]
        formatted_times = [s.formatted_time for s in self.stats]
        process_cpu = [s.process_cpu_percent for s in self.stats]
        system_cpu = [s.system_cpu_percent for s in self.stats]
        memory = [s.memory_percent for s in self.stats]
        threads = [s.thread_count for s in self.stats]

        # Create plot with two subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

        # Plot CPU usage in first subplot
        ax1.plot(timestamps, process_cpu, "b-", label="Process CPU %")
        ax1.plot(timestamps, system_cpu, "r-", label="System CPU %")
        ax1.set_ylabel("CPU %")
        ax1.set_title("CPU Usage Over Time")
        ax1.legend()
        ax1.grid(True)

        # Plot memory and threads in second subplot
        ax2.plot(timestamps, memory, "g-", label="Memory %")

        # Create second y-axis for thread count
        ax3 = ax2.twinx()
        ax3.plot(timestamps, threads, "k-", label="Thread Count")
        ax3.set_ylabel("Thread Count")

        # Add legend for second subplot
        lines1, labels1 = ax2.get_legend_handles_labels()
        lines2, labels2 = ax3.get_legend_handles_labels()
        ax2.legend(lines1 + lines2, labels1 + labels2, loc="upper right")

        ax2.set_ylabel("Memory %")
        ax2.set_xlabel("Time")
        ax2.grid(True)

        # Set x-axis ticks to show time
        tick_indices = list(range(0, len(timestamps), max(1, len(timestamps) // 10)))
        plt.xticks(
            [timestamps[i] for i in tick_indices],
            [formatted_times[i] for i in tick_indices],
            rotation=45,
        )

        plt.tight_layout()

        # Save or show the plot
        if save_path:
            plt.savefig(save_path)
            logger.info(f"CPU usage plot saved to {save_path}")
        else:
            plt.show()

        plt.close()


# Integration with scraper
def create_cpu_monitor_for_scraper(
    alert_threshold: float = 80.0, interval: float = 1.0
) -> CPUMonitor:
    """Create and configure a CPU monitor for the scraper"""

    def on_high_cpu(stats: CPUStats):
        """Callback for high CPU usage"""
        logger.warning(
            f"High CPU usage detected: "
            f"Process: {stats.process_cpu_percent:.1f}%, "
            f"System: {stats.system_cpu_percent:.1f}%, "
            f"Memory: {stats.memory_percent:.1f}%, "
            f"Threads: {stats.thread_count}"
        )

    monitor = CPUMonitor(
        interval=interval,
        history_size=3600,  # 1 hour at 1-second intervals
        cpu_threshold=alert_threshold,
        on_threshold_exceeded=on_high_cpu,
    )

    return monitor


@dataclass
class SystemMonitor:
    """Monitor system resource usage including per-process metrics"""

    interval: float = 1.0  # seconds
    output_dir: str = "monitoring"
    file_prefix: str = "system_stats"
    max_processes: int = 10  # Number of top processes to track
    enabled: bool = True
    _stop_event: Event = field(default_factory=Event)
    _monitor_thread: Optional[Thread] = None

    def __post_init__(self):
        """Ensure output directory exists"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def start(self):
        """Start monitoring in a background thread"""
        if self._monitor_thread is not None and self._monitor_thread.is_alive():
            logger.warning("System monitor already running")
            return

        self._stop_event.clear()
        self._monitor_thread = Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info(f"System monitoring started with {self.interval}s interval")

    def stop(self):
        """Stop the monitoring thread"""
        if self._monitor_thread is None:
            return

        self._stop_event.set()
        self._monitor_thread.join(timeout=2.0)
        logger.info("System monitoring stopped")

    def _monitor_loop(self):
        """Main monitoring loop"""
        stats_file = os.path.join(
            self.output_dir,
            f"{self.file_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        )

        # Create empty DataFrame to store stats
        columns = [
            "timestamp",
            "cpu_percent",
            "memory_percent",
            "swap_percent",
            "disk_usage_percent",
            "network_sent_bytes",
            "network_recv_bytes",
        ]

        # Add columns for top processes
        for i in range(self.max_processes):
            columns.extend(
                [f"proc{i}_pid", f"proc{i}_name", f"proc{i}_cpu", f"proc{i}_memory"]
            )

        df = pd.DataFrame(columns=columns)
        last_save = time.time()

        # Get initial network counters
        net_io_counters = psutil.net_io_counters()
        last_bytes_sent, last_bytes_recv = (
            net_io_counters.bytes_sent,
            net_io_counters.bytes_recv,
        )

        while not self._stop_event.is_set():
            try:
                # Get current timestamp
                current_time = time.time()

                # Get system stats
                cpu_percent = psutil.cpu_percent(interval=None)
                memory = psutil.virtual_memory()
                swap = psutil.swap_memory()
                disk = psutil.disk_usage("/")

                # Calculate network bandwidth since last sample
                net_io_counters = psutil.net_io_counters()
                bytes_sent, bytes_recv = (
                    net_io_counters.bytes_sent,
                    net_io_counters.bytes_recv,
                )
                sent_rate = (bytes_sent - last_bytes_sent) / self.interval
                recv_rate = (bytes_recv - last_bytes_recv) / self.interval
                last_bytes_sent, last_bytes_recv = bytes_sent, bytes_recv

                # Get top processes by CPU
                processes = []
                for proc in sorted(
                    psutil.process_iter(
                        ["pid", "name", "cpu_percent", "memory_percent"]
                    ),
                    key=lambda p: p.info["cpu_percent"] or 0,
                    reverse=True,
                ):
                    try:
                        # Update CPU usage
                        if proc.info["cpu_percent"] is None:
                            proc.info["cpu_percent"] = proc.cpu_percent(interval=0)

                        processes.append(proc.info)
                        if len(processes) >= self.max_processes:
                            break
                    except (
                        psutil.NoSuchProcess,
                        psutil.AccessDenied,
                        psutil.ZombieProcess,
                    ):
                        pass

                # Pad the processes list if needed
                while len(processes) < self.max_processes:
                    processes.append(
                        {"pid": 0, "name": "", "cpu_percent": 0, "memory_percent": 0}
                    )

                # Create row data
                row = {
                    "timestamp": current_time,
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory.percent,
                    "swap_percent": swap.percent,
                    "disk_usage_percent": disk.percent,
                    "network_sent_bytes": sent_rate,
                    "network_recv_bytes": recv_rate,
                }

                # Add process data
                for i, proc in enumerate(processes):
                    row[f"proc{i}_pid"] = proc["pid"]
                    row[f"proc{i}_name"] = proc["name"]
                    row[f"proc{i}_cpu"] = proc["cpu_percent"]
                    row[f"proc{i}_memory"] = proc["memory_percent"]

                # Add row to DataFrame
                df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

                # Save periodically (every minute)
                if current_time - last_save > 60:
                    df.to_csv(stats_file, index=False)
                    last_save = current_time

                # Print current stats
                logger.debug(
                    f"System: CPU={cpu_percent:.1f}%, Mem={memory.percent:.1f}%, "
                    f"Top process: {processes[0]['name']} (CPU={processes[0]['cpu_percent']:.1f}%)"
                )

                # Sleep until next interval
                wait_time = max(0, self.interval - (time.time() - current_time))
                self._stop_event.wait(wait_time)

            except Exception as e:
                logger.error(f"Error in system monitoring: {str(e)}")
                time.sleep(self.interval)

        # Final save when stopping
        df.to_csv(stats_file, index=False)
        logger.info(f"System stats saved to {stats_file}")

    def get_stats_by_process_name(
        self, process_name: str, output_file: Optional[str] = None
    ):
        """Analyze collected data for a specific process"""
        # Find the most recent stats file
        stats_files = [
            f
            for f in os.listdir(self.output_dir)
            if f.startswith(self.file_prefix) and f.endswith(".csv")
        ]

        if not stats_files:
            logger.warning("No stats files found")
            return None

        latest_file = max(
            stats_files,
            key=lambda f: os.path.getmtime(os.path.join(self.output_dir, f)),
        )
        stats_path = os.path.join(self.output_dir, latest_file)

        # Load the stats
        df = pd.read_csv(stats_path)

        # Find columns for the target process
        process_stats = pd.DataFrame()
        process_stats["timestamp"] = df["timestamp"]

        # Extract process-specific data
        for i in range(self.max_processes):
            mask = df[f"proc{i}_name"] == process_name
            if mask.any():
                process_stats.loc[mask, "cpu_percent"] = df.loc[mask, f"proc{i}_cpu"]
                process_stats.loc[mask, "memory_percent"] = df.loc[
                    mask, f"proc{i}_memory"
                ]

        # Fill missing values with zeros
        process_stats = process_stats.fillna(0)

        # Convert timestamp to datetime for readability
        process_stats["datetime"] = pd.to_datetime(process_stats["timestamp"], unit="s")

        # Save if requested
        if output_file:
            process_stats.to_csv(output_file, index=False)
            logger.info(f"Process stats for '{process_name}' saved to {output_file}")

        return process_stats

    def plot_system_overview(self, output_file: Optional[str] = None):
        """Generate a plot of system resource usage"""
        import matplotlib.pyplot as plt

        # Find the most recent stats file
        stats_files = [
            f
            for f in os.listdir(self.output_dir)
            if f.startswith(self.file_prefix) and f.endswith(".csv")
        ]

        if not stats_files:
            logger.warning("No stats files found")
            return

        latest_file = max(
            stats_files,
            key=lambda f: os.path.getmtime(os.path.join(self.output_dir, f)),
        )
        stats_path = os.path.join(self.output_dir, latest_file)

        # Load the stats
        df = pd.read_csv(stats_path)
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")

        # Create plot with multiple subplots
        fig, axs = plt.subplots(3, 1, figsize=(12, 10), sharex=True)

        # Plot CPU usage
        axs[0].plot(df["datetime"], df["cpu_percent"], "b-", label="System CPU")
        axs[0].set_ylabel("CPU %")
        axs[0].set_title("System Resource Usage")
        axs[0].legend()
        axs[0].grid(True)

        # Plot memory usage
        axs[1].plot(df["datetime"], df["memory_percent"], "g-", label="Memory")
        axs[1].plot(df["datetime"], df["swap_percent"], "r-", label="Swap")
        axs[1].set_ylabel("Memory %")
        axs[1].legend()
        axs[1].grid(True)

        # Plot network usage
        axs[2].plot(
            df["datetime"], df["network_sent_bytes"] / 1024, "y-", label="Sent (KB/s)"
        )
        axs[2].plot(
            df["datetime"],
            df["network_recv_bytes"] / 1024,
            "m-",
            label="Received (KB/s)",
        )
        axs[2].set_ylabel("Network KB/s")
        axs[2].set_xlabel("Time")
        axs[2].legend()
        axs[2].grid(True)

        plt.tight_layout()

        # Save or show the plot
        if output_file:
            plt.savefig(output_file)
            logger.info(f"System overview plot saved to {output_file}")
        else:
            plt.show()

        plt.close()

    def plot_top_processes(self, top_n: int = 5, output_file: Optional[str] = None):
        """Plot CPU usage of top processes"""
        import matplotlib.pyplot as plt
        import numpy as np

        # Find the most recent stats file
        stats_files = [
            f
            for f in os.listdir(self.output_dir)
            if f.startswith(self.file_prefix) and f.endswith(".csv")
        ]

        if not stats_files:
            logger.warning("No stats files found")
            return

        latest_file = max(
            stats_files,
            key=lambda f: os.path.getmtime(os.path.join(self.output_dir, f)),
        )
        stats_path = os.path.join(self.output_dir, latest_file)

        # Load the stats
        df = pd.read_csv(stats_path)
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")

        # Collect process stats
        process_data = {}

        for i in range(self.max_processes):
            proc_names = df[f"proc{i}_name"].unique()
            for proc_name in proc_names:
                if not proc_name or proc_name == "":
                    continue

                mask = df[f"proc{i}_name"] == proc_name
                cpu_values = df.loc[mask, f"proc{i}_cpu"]

                if proc_name in process_data:
                    process_data[proc_name].extend(cpu_values.tolist())
                else:
                    process_data[proc_name] = cpu_values.tolist()

        # Calculate average CPU for each process
        process_avg_cpu = {}
        for proc_name, cpu_values in process_data.items():
            if cpu_values:
                process_avg_cpu[proc_name] = np.mean(cpu_values)

        # Get top processes by average CPU
        top_processes = sorted(
            process_avg_cpu.items(), key=lambda x: x[1], reverse=True
        )[:top_n]

        # Create plot
        plt.figure(figsize=(10, 6))

        # Create bar chart
        proc_names = [p[0] for p in top_processes]
        cpu_avgs = [p[1] for p in top_processes]

        plt.bar(proc_names, cpu_avgs)
        plt.ylabel("Average CPU %")
        plt.title(f"Top {top_n} Processes by CPU Usage")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        # Save or show the plot
        if output_file:
            plt.savefig(output_file)
            logger.info(f"Top processes plot saved to {output_file}")
        else:
            plt.show()

        plt.close()


# Integration with the scraper architecture
def setup_system_monitoring_for_scraper():
    """Setup system monitoring for the scraper application"""
    monitor = SystemMonitor(
        interval=2.0,  # Sample every 2 seconds
        output_dir="scraper_stats",
        file_prefix="system_stats",
    )

    return monitor


class MonitoredScraper:
    """Decorator class that adds monitoring to any scraper instance"""

    def __init__(
        self,
        scraper,
        cpu_monitoring=False,
        system_monitoring=False,
        cpu_threshold=80.0,
        monitoring_interval=1.0,
    ):
        self.scraper = scraper
        self.cpu_monitoring = cpu_monitoring
        self.system_monitoring = system_monitoring
        self.cpu_threshold = cpu_threshold
        self.monitoring_interval = monitoring_interval
        self.cpu_monitor = None
        self.system_monitor = None

    def scrape_all(self, urls):
        """Run scraping with CPU monitoring"""
        start_time = time.time()

        if self.cpu_monitoring:
            self.cpu_monitor = create_cpu_monitor_for_scraper(
                alert_threshold=self.cpu_threshold, interval=self.monitoring_interval
            )
            self.cpu_monitor.start()
        if self.system_monitoring:
            self.system_monitor = setup_system_monitoring_for_scraper()
            self.system_monitor.start()

        try:
            # Run the actual scraper
            results = self.scraper.scrape_all(urls)

            # Log performance summary
            elapsed = time.time() - start_time
            logger.info(
                f"Scraping completed in {elapsed:.2f}s for {len(urls)} URLs "
                f"({len(urls)/elapsed:.2f} URLs/s)"
            )

            # Log CPU stats if available
            if self.cpu_monitor:
                summary = self.cpu_monitor.get_stats_summary()
                logger.info(
                    f"CPU usage: Avg Process={summary['avg_process_cpu']:.1f}%, "
                    f"Max Process={summary['max_process_cpu']:.1f}%, "
                    f"Avg System={summary['avg_system_cpu']:.1f}%"
                )

                # Save CPU plot with timestamp
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                monitoring_dir = "scraper/monitoring"
                os.makedirs(monitoring_dir, exist_ok=True)
                self.cpu_monitor.plot(f"{monitoring_dir}/cpu_usage_{timestamp}.png")

            # Generate system monitor reports if available
            if self.system_monitor:
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                monitoring_dir = "scraper/monitoring"
                os.makedirs(monitoring_dir, exist_ok=True)
                self.system_monitor.plot_system_overview(
                    f"{monitoring_dir}/system_overview_{timestamp}.png"
                )
                self.system_monitor.plot_top_processes(
                    top_n=5, output_file=f"{monitoring_dir}/top_processes_{timestamp}.png"
                )

            return results

        finally:
            # Always stop monitors
            if self.cpu_monitor:
                self.cpu_monitor.stop()

            if self.system_monitor:
                self.system_monitor.stop()

    def set_strategy(self, strategy):
        """Pass through to the underlying scraper"""
        self.scraper.set_strategy(strategy)
        return self
