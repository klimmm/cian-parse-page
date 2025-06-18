# enhanced_cpu_monitoring.py

import time
import os
import threading
import psutil
import logging
from typing import Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)

@dataclass
class ProcessInfo:
    """Information about a process"""
    pid: int
    name: str
    cpu_percent: float
    memory_percent: float
    
@dataclass
class CoreBreakdown:
    """Breakdown of processes per core"""
    core_id: int
    core_usage: float
    top_processes: List[ProcessInfo] = field(default_factory=list)

@dataclass
class EnhancedCPUStats:
    """Store enhanced CPU usage statistics including per-core data"""
    timestamp: float
    process_cpu_percent: float
    system_cpu_percent: float
    memory_percent: float
    thread_count: int
    per_core_usage: List[float] = field(default_factory=list)  # Per-core CPU usage
    cpu_count: int = 0
    load_avg: tuple = field(default_factory=tuple)  # 1m, 5m, 15m load averages
    headless_shell_processes: List[ProcessInfo] = field(default_factory=list)  # All headless_shell processes
    top_processes_per_core: List[CoreBreakdown] = field(default_factory=list)  # Process breakdown per core
    bird_processes: List[ProcessInfo] = field(default_factory=list)  # Bird processes
    
    @property
    def formatted_time(self) -> str:
        """Return formatted timestamp"""
        return datetime.fromtimestamp(self.timestamp).strftime('%H:%M:%S')
    
    @property
    def per_core_summary(self) -> str:
        """Return formatted per-core usage"""
        if not self.per_core_usage:
            return "N/A"
        core_strs = [f"C{i}:{usage:.1f}%" for i, usage in enumerate(self.per_core_usage)]
        return " ".join(core_strs)
    
    @property
    def load_avg_str(self) -> str:
        """Return formatted load averages"""
        if not self.load_avg:
            return "N/A"
        return f"{self.load_avg[0]:.2f} {self.load_avg[1]:.2f} {self.load_avg[2]:.2f}"


@dataclass
class EnhancedCPUMonitor:
    """Enhanced CPU monitor with per-core monitoring"""
    interval: float = 1.0
    history_size: int = 3600
    enabled: bool = True
    stats: List[EnhancedCPUStats] = field(default_factory=list)
    process: psutil.Process = field(default_factory=lambda: psutil.Process(os.getpid()))
    _stop_event: threading.Event = field(default_factory=threading.Event)
    _monitor_thread: Optional[threading.Thread] = None
    on_threshold_exceeded: Optional[Callable[[EnhancedCPUStats], None]] = None
    cpu_threshold: float = 80.0
    
    def start(self):
        """Start monitoring in a background thread"""
        if self._monitor_thread is not None and self._monitor_thread.is_alive():
            logger.warning("Enhanced monitor already running")
            return
            
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info(f"Enhanced CPU monitoring started with {self.interval}s interval")
    
    def stop(self):
        """Stop the monitoring thread"""
        if self._monitor_thread is None:
            return
            
        self._stop_event.set()
        self._monitor_thread.join(timeout=2.0)
        logger.info("Enhanced CPU monitoring stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while not self._stop_event.is_set():
            try:
                self._collect_enhanced_stats()
                self._stop_event.wait(self.interval)
            except Exception as e:
                logger.error(f"Error in enhanced CPU monitoring: {str(e)}")
                time.sleep(self.interval)
    
    def _collect_enhanced_stats(self):
        """Collect enhanced CPU and memory stats including per-core data"""
        try:
            # Get process stats
            with self.process.oneshot():
                process_cpu = self.process.cpu_percent()
                memory_percent = self.process.memory_percent()
                thread_count = len(self.process.threads())
            
            # Get per-core CPU usage (with interval=None to get cached values)
            per_core_usage = psutil.cpu_percent(percpu=True, interval=None)
            
            # Get overall system CPU
            system_cpu = psutil.cpu_percent(interval=None)
            
            # Get load averages (macOS specific)
            try:
                load_avg = os.getloadavg()
            except (OSError, AttributeError):
                load_avg = (0.0, 0.0, 0.0)
            
            # Collect process information
            headless_shell_processes = []
            bird_processes = []
            all_processes = []
            
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent', 'cmdline']):
                try:
                    pinfo = proc.info
                    if pinfo['name'] and pinfo['cpu_percent'] is not None:
                        proc_info = ProcessInfo(
                            pid=pinfo['pid'],
                            name=pinfo['name'],
                            cpu_percent=pinfo['cpu_percent'],
                            memory_percent=pinfo['memory_percent'] or 0.0
                        )
                        
                        # Categorize processes
                        if 'headless_shell' in pinfo['name']:
                            headless_shell_processes.append(proc_info)
                        elif 'bird' in pinfo['name']:
                            bird_processes.append(proc_info)
                        
                        all_processes.append(proc_info)
                        
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
            
            # Sort by CPU usage
            headless_shell_processes.sort(key=lambda x: x.cpu_percent, reverse=True)
            bird_processes.sort(key=lambda x: x.cpu_percent, reverse=True)
            all_processes.sort(key=lambda x: x.cpu_percent, reverse=True)
            
            # Create realistic per-core breakdown using round-robin distribution
            top_processes_per_core = []
            
            # Get all active processes sorted by CPU usage  
            active_processes = [proc for proc in all_processes if proc.cpu_percent > 1.0]
            
            # Distribute processes across cores in round-robin fashion to avoid duplication
            process_assignments = {}  # pid -> list of (core_id, estimated_usage)
            
            for i, proc in enumerate(active_processes[:20]):  # Limit to top 20 processes
                # Determine how many cores this process likely uses
                if proc.cpu_percent > 100:
                    cores_used = min(8, int(proc.cpu_percent / 50))  # Multi-core process
                elif proc.cpu_percent > 50:
                    cores_used = 2  # Likely dual-core
                else:
                    cores_used = 1  # Single-core
                
                # Assign to specific cores based on process index and core usage
                assigned_cores = []
                for j in range(cores_used):
                    target_core = (i + j) % len(per_core_usage)
                    # Only assign to cores that have significant usage
                    if per_core_usage[target_core] > 20:
                        assigned_cores.append(target_core)
                
                # If no high-usage cores, assign to highest usage core
                if not assigned_cores:
                    target_core = max(range(len(per_core_usage)), key=lambda x: per_core_usage[x])
                    assigned_cores = [target_core]
                
                # Calculate usage per assigned core
                usage_per_core = proc.cpu_percent / len(assigned_cores)
                process_assignments[proc.pid] = [(core, min(usage_per_core, per_core_usage[core])) 
                                               for core in assigned_cores]
            
            # Build per-core breakdown
            for core_id, core_usage in enumerate(per_core_usage):
                core_processes = []
                
                # Find processes assigned to this core
                for pid, assignments in process_assignments.items():
                    for assigned_core, estimated_usage in assignments:
                        if assigned_core == core_id:
                            # Find the original process info
                            orig_proc = next((p for p in active_processes if p.pid == pid), None)
                            if orig_proc:
                                core_proc = ProcessInfo(
                                    pid=orig_proc.pid,
                                    name=orig_proc.name,
                                    cpu_percent=estimated_usage,
                                    memory_percent=orig_proc.memory_percent
                                )
                                core_processes.append(core_proc)
                
                # Sort by estimated core usage and limit to top 3
                core_processes.sort(key=lambda x: x.cpu_percent, reverse=True)
                core_processes = core_processes[:3]
                
                # Calculate accounted CPU on this core
                accounted_cpu = sum(proc.cpu_percent for proc in core_processes)
                unaccounted_cpu = max(0, core_usage - accounted_cpu)
                
                # Add system/kernel usage if significant
                if unaccounted_cpu > 5.0:
                    system_proc = ProcessInfo(
                        pid=0,
                        name="[system/kernel]",
                        cpu_percent=unaccounted_cpu,
                        memory_percent=0.0
                    )
                    core_processes.insert(0, system_proc)
                
                breakdown = CoreBreakdown(
                    core_id=core_id,
                    core_usage=core_usage,
                    top_processes=core_processes[:4]  # Show top 4 including system
                )
                top_processes_per_core.append(breakdown)
            
            # Create enhanced stats object
            stats = EnhancedCPUStats(
                timestamp=time.time(),
                process_cpu_percent=process_cpu,
                system_cpu_percent=system_cpu,
                memory_percent=memory_percent,
                thread_count=thread_count,
                per_core_usage=per_core_usage,
                cpu_count=len(per_core_usage),
                load_avg=load_avg,
                headless_shell_processes=headless_shell_processes,
                top_processes_per_core=top_processes_per_core,
                bird_processes=bird_processes
            )
            
            # Add to history
            self.stats.append(stats)
            if len(self.stats) > self.history_size:
                self.stats = self.stats[-self.history_size:]
            
            # Check thresholds and trigger callback
            if (self.on_threshold_exceeded and 
                (stats.process_cpu_percent > self.cpu_threshold or 
                 stats.system_cpu_percent > self.cpu_threshold)):
                self.on_threshold_exceeded(stats)
                
        except Exception as e:
            logger.error(f"Error collecting enhanced CPU stats: {str(e)}")
    
    def get_current_stats(self) -> Optional[EnhancedCPUStats]:
        """Get the most recent enhanced stats"""
        if not self.stats:
            return None
        return self.stats[-1]
    
    def print_current_status(self):
        """Print current system status with detailed per-core and process breakdown"""
        stats = self.get_current_stats()
        if not stats:
            print("No stats available yet")
            return
        
        print(f"\n📊 {stats.formatted_time} CPU Status:")
        print(f"   Process: {stats.process_cpu_percent:.1f}% | System: {stats.system_cpu_percent:.1f}% | Memory: {stats.memory_percent:.1f}%")
        print(f"   Load Avg: {stats.load_avg_str} | Threads: {stats.thread_count}")
        
        # Headless shell processes breakdown
        if stats.headless_shell_processes:
            print(f"\n🌐 Headless Shell Processes ({len(stats.headless_shell_processes)} total):")
            for i, proc in enumerate(stats.headless_shell_processes[:10]):  # Show top 10
                proc_type = "renderer" if "renderer" in proc.name else "main" if "disable-field-trial" in proc.name else "gpu" if "gpu-process" in proc.name else "utility"
                print(f"   {i+1:2d}. PID {proc.pid:5d} ({proc_type:8s}): {proc.cpu_percent:5.1f}% CPU, {proc.memory_percent:4.1f}% MEM")
            
            if len(stats.headless_shell_processes) > 10:
                print(f"   ... and {len(stats.headless_shell_processes) - 10} more")
            
            total_headless_cpu = sum(proc.cpu_percent for proc in stats.headless_shell_processes)
            print(f"   📈 Total headless_shell CPU: {total_headless_cpu:.1f}%")
        
        # Bird processes
        if stats.bird_processes:
            print(f"\n🦅 Bird Processes ({len(stats.bird_processes)} total):")
            for proc in stats.bird_processes:
                print(f"   PID {proc.pid:5d}: {proc.cpu_percent:5.1f}% CPU, {proc.memory_percent:4.1f}% MEM")
        
        # Per-core breakdown
        print(f"\n🔥 Per-Core Breakdown:")
        for core in stats.top_processes_per_core:
            core_type = "P" if core.core_id < 4 else "E"  # Performance vs Efficiency cores
            print(f"   Core {core.core_id} ({core_type}): {core.core_usage:5.1f}%", end="")
            
            if core.top_processes:
                top_proc = core.top_processes[0]
                proc_name = top_proc.name[:15] + "..." if len(top_proc.name) > 15 else top_proc.name
                print(f" │ Top: {proc_name} ({top_proc.cpu_percent:.1f}%)")
            else:
                print(" │ Top: idle")
        
        # Highlight high usage cores
        if stats.per_core_usage:
            high_cores = [f"Core {i}" for i, usage in enumerate(stats.per_core_usage) if usage > 80]
            if high_cores:
                print(f"\n⚠️  High usage cores (>80%): {', '.join(high_cores)}")
            
            if all(usage > 90 for usage in stats.per_core_usage):
                print("🚨 ALL CORES >90% - SYSTEM OVERLOAD!")
        
        print("─" * 80)


def create_enhanced_cpu_monitor_for_scraper(alert_threshold: float = 80.0, 
                                          interval: float = 1.0) -> EnhancedCPUMonitor:
    """Create enhanced CPU monitor for the scraper"""
    
    def on_high_cpu(stats: EnhancedCPUStats):
        """Enhanced callback for high CPU usage"""
        logger.warning(
            f"High CPU detected: Process: {stats.process_cpu_percent:.1f}%, "
            f"System: {stats.system_cpu_percent:.1f}%, Memory: {stats.memory_percent:.1f}%, "
            f"Threads: {stats.thread_count}"
        )
        logger.info(f"Per-core usage: {stats.per_core_summary}")
        logger.info(f"Load averages: {stats.load_avg_str}")
        
        # Show headless shell process breakdown
        if stats.headless_shell_processes:
            total_headless_cpu = sum(proc.cpu_percent for proc in stats.headless_shell_processes)
            active_headless = [proc for proc in stats.headless_shell_processes if proc.cpu_percent > 1.0]
            logger.info(f"Headless shell: {len(stats.headless_shell_processes)} processes, {total_headless_cpu:.1f}% total CPU")
            if active_headless:
                top_headless = ", ".join([f"PID{proc.pid}({proc.cpu_percent:.1f}%)" for proc in active_headless[:5]])
                logger.info(f"Active headless processes: {top_headless}")
            
            # Show estimated core distribution for headless processes
            if active_headless:
                logger.info("Estimated core distribution of headless processes:")
                for i, proc in enumerate(active_headless[:8]):  # Show up to 8 (one per core)
                    estimated_core = i % 8  # Distribute across cores
                    core_type = "P" if estimated_core < 4 else "E"
                    logger.info(f"  Core {estimated_core} ({core_type}): PID{proc.pid} ~{proc.cpu_percent:.1f}% CPU")
        
        # Show bird processes
        if stats.bird_processes:
            for proc in stats.bird_processes:
                logger.warning(f"Bird process PID {proc.pid}: {proc.cpu_percent:.1f}% CPU")
        
        # Enhanced per-core breakdown with accurate process breakdown
        if stats.per_core_usage and stats.top_processes_per_core:
            logger.info("Per-core CPU breakdown with process details:")
            
            total_accounted = 0
            total_system_kernel = 0
            
            for core in stats.top_processes_per_core:
                core_type = "P" if core.core_id < 4 else "E"
                logger.info(f"  Core {core.core_id} ({core_type}): {core.core_usage:.1f}% total")
                
                core_accounted = 0
                for proc in core.top_processes[:4]:  # Show top 4 per core
                    if proc.name == "[system/kernel]":
                        logger.info(f"    └─ {proc.name}: {proc.cpu_percent:.1f}% (unaccounted system load)")
                        total_system_kernel += proc.cpu_percent
                    else:
                        # Show the estimated usage on this specific core
                        logger.info(f"    └─ PID{proc.pid} ({proc.name[:20]}): ~{proc.cpu_percent:.1f}% on this core")
                        core_accounted += proc.cpu_percent
                
                total_accounted += core_accounted
                
                # Show any remaining unaccounted CPU
                unaccounted = max(0, core.core_usage - core_accounted)
                if unaccounted > 2.0:
                    logger.info(f"    └─ [other processes]: ~{unaccounted:.1f}%")
            
            # Summary of CPU accounting
            total_core_usage = sum(stats.per_core_usage)
            total_process_cpu = sum(proc.cpu_percent for proc in stats.headless_shell_processes + stats.bird_processes)
            
            logger.info(f"CPU Accounting Summary:")
            logger.info(f"  Total core usage: {total_core_usage:.1f}%")
            logger.info(f"  Tracked processes: {total_process_cpu:.1f}%")
            logger.info(f"  System/kernel: {total_system_kernel:.1f}%")
            logger.info(f"  Other processes: {max(0, total_core_usage - total_process_cpu - total_system_kernel):.1f}%")
            
            high_cores = [(i, usage) for i, usage in enumerate(stats.per_core_usage) if usage > 90]
            if high_cores:
                core_info = ", ".join([f"Core {i}: {usage:.1f}%" for i, usage in high_cores])
                logger.warning(f"Cores at >90%: {core_info}")
            
            # Special warning for all cores maxed
            if all(usage > 95 for usage in stats.per_core_usage):
                logger.critical("ALL CORES >95% - CRITICAL SYSTEM OVERLOAD!")
    
    monitor = EnhancedCPUMonitor(
        interval=interval,
        history_size=3600,
        cpu_threshold=alert_threshold,
        on_threshold_exceeded=on_high_cpu
    )
    
    return monitor


# Usage example
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    monitor = create_enhanced_cpu_monitor_for_scraper()
    monitor.start()
    
    try:
        # Print status every 5 seconds
        for _ in range(12):  # Run for 1 minute
            time.sleep(5)
            monitor.print_current_status()
    finally:
        monitor.stop()