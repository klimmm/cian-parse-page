# cpu_monitoring.py

import time
import os
import threading
import psutil
import logging
from typing import Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field
import matplotlib.pyplot as plt
from datetime import datetime

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
        return datetime.fromtimestamp(self.timestamp).strftime('%H:%M:%S')


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
                thread_count=thread_count
            )
            
            # Add to history, keeping the history size limit
            self.stats.append(stats)
            if len(self.stats) > self.history_size:
                self.stats = self.stats[-self.history_size:]
            
            # Check thresholds and trigger callback if needed
            if (self.on_threshold_exceeded and 
                (stats.process_cpu_percent > self.cpu_threshold or 
                 stats.system_cpu_percent > self.cpu_threshold)):
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
        
        avg_process_cpu = sum(s.process_cpu_percent for s in self.stats) / len(self.stats)
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
        ax1.plot(timestamps, process_cpu, 'b-', label='Process CPU %')
        ax1.plot(timestamps, system_cpu, 'r-', label='System CPU %')
        ax1.set_ylabel('CPU %')
        ax1.set_title('CPU Usage Over Time')
        ax1.legend()
        ax1.grid(True)
        
        # Plot memory and threads in second subplot
        ax2.plot(timestamps, memory, 'g-', label='Memory %')
        
        # Create second y-axis for thread count
        ax3 = ax2.twinx()
        ax3.plot(timestamps, threads, 'k-', label='Thread Count')
        ax3.set_ylabel('Thread Count')
        
        # Add legend for second subplot
        lines1, labels1 = ax2.get_legend_handles_labels()
        lines2, labels2 = ax3.get_legend_handles_labels()
        ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
        
        ax2.set_ylabel('Memory %')
        ax2.set_xlabel('Time')
        ax2.grid(True)
        
        # Set x-axis ticks to show time
        tick_indices = list(range(0, len(timestamps), max(1, len(timestamps) // 10)))
        plt.xticks([timestamps[i] for i in tick_indices], 
                   [formatted_times[i] for i in tick_indices], 
                   rotation=45)
        
        plt.tight_layout()
        
        # Save or show the plot
        if save_path:
            plt.savefig(save_path)
            logger.info(f"CPU usage plot saved to {save_path}")
        else:
            plt.show()
        
        plt.close()


# Integration with scraper
def create_cpu_monitor_for_scraper(alert_threshold: float = 80.0, 
                                  interval: float = 1.0) -> CPUMonitor:
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
        on_threshold_exceeded=on_high_cpu
    )
    
    return monitor



# Per-process monitoring for multiprocessing approach
def monitor_scraper_process(process_id: int):
    """Monitor function to run in each scraper process"""
    process = psutil.Process()
    stats = []
    
    logger.info(f"[P{process_id}] Starting CPU monitoring")
    
    try:
        while True:
            cpu_percent = process.cpu_percent()
            memory_percent = process.memory_percent()
            
            stats.append({
                'timestamp': time.time(),
                'cpu': cpu_percent,
                'memory': memory_percent
            })
            
            if cpu_percent > 80:
                logger.warning(f"[P{process_id}] High CPU usage: {cpu_percent:.1f}%")
            
            time.sleep(1)
    except Exception as e:
        logger.error(f"[P{process_id}] Monitoring error: {str(e)}")
    finally:
        # Save stats to file before process exits
        with open(f"process_{process_id}_stats.txt", "w") as f:
            for stat in stats:
                f.write(f"{stat['timestamp']},{stat['cpu']},{stat['memory']}\n")


# Usage example
if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Simple demonstration
    monitor = create_cpu_monitor_for_scraper()
    monitor.start()
    
    try:
        # Simulate some work
        logger.info("Starting CPU-intensive work...")
        for i in range(10):
            # Generate some CPU load
            start = time.time()
            while time.time() - start < 0.5:
                _ = [i**2 for i in range(10000)]
            
            # Sleep to simulate I/O
            time.sleep(0.5)
            
            # Print current stats
            stats = monitor.get_current_stats()
            if stats:
                logger.info(
                    f"Current CPU: Process={stats.process_cpu_percent:.1f}%, "
                    f"System={stats.system_cpu_percent:.1f}%"
                )
    finally:
        monitor.stop()
        monitor.plot("cpu_usage_demo.png")
        logger.info("Demo completed, plot saved to cpu_usage_demo.png")