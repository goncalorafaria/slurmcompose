"""
SlurmCompose Terminal UI Components

This module provides terminal UI components for SlurmCompose including:
- Welcome boxes and banners
- Live dashboard with cluster status and gateway logs
- Log capture utilities
"""

from collections import deque
from datetime import datetime
from termcolor import colored
import logging

try:
    from rich.console import Console
    from rich.live import Live
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


def print_welcome_box():
    """Print a welcome box similar to Claude Code's style."""
    box_width = 70
    border_color = "cyan"
    text_color = "white"
    
    # Box drawing characters
    top_line = "╭" + "─" * (box_width - 2) + "╮"
    bottom_line = "╰" + "─" * (box_width - 2) + "╯"
    
    def center_text(text):
        padding = (box_width - 2 - len(text)) // 2
        return "│ " + " " * padding + text + " " * (box_width - 2 - len(text) - padding) + " │"
    
    print(colored(top_line, border_color))
    print(colored(center_text(""), border_color))
    print(colored("│ ", border_color) + colored("✨ Welcome to SlurmCompose ✨", "cyan", attrs=["bold"]) + colored(" " * 21 + "│", border_color))
    print(colored(center_text(""), border_color))
    print(colored("│ ", border_color)  + colored("            │", border_color))
    print(colored(center_text(""), border_color))
    print(colored(bottom_line, border_color))
    print()


def print_mode_banner(mode):
    """Print a prominent mode banner."""
    mode_configs = {
        "apply": {"emoji": "🚀", "text": "APPLY", "color": "green", "desc": "Launching cluster"},
        "destroy": {"emoji": "🔥", "text": "DESTROY", "color": "red", "desc": "Terminating cluster"},
        "plan": {"emoji": "📊", "text": "PLAN", "color": "cyan", "desc": "Showing configuration"},
        "run": {"emoji": "▶️", "text": "RUN", "color": "magenta", "desc": "Launching single job"},
    }
    
    config = mode_configs.get(mode.lower(), {"emoji": "⚡", "text": mode.upper(), "color": "white", "desc": ""})
    
    # Create banner
    banner_width = 70
    top_line = "┌" + "─" * (banner_width - 2) + "┐"
    bottom_line = "└" + "─" * (banner_width - 2) + "┘"
    
    mode_text = f"{config['emoji']} MODE: {config['text']}"
    if config['desc']:
        mode_text += f" - {config['desc']}"
    
    padding = (banner_width - 2 - len(mode_text)) // 2
    centered = "│ " + " " * padding + mode_text + " " * (banner_width - 2 - len(mode_text) - padding) + " │"
    
    print(colored(top_line, config['color']))
    print(colored(centered, config['color'], attrs=["bold"]))
    print(colored(bottom_line, config['color']))
    print()


def print_section_header(title, emoji="", color="cyan"):
    """Print a section header."""
    print()
    print(colored(f"{'─' * 70}", color))
    print(colored(f"{emoji} {title}".center(70), color, attrs=["bold"]))
    print(colored(f"{'─' * 70}", color))
    print()


class GatewayLogCapture:
    """Captures stdout/stderr and adds to gateway logs."""
    
    def __init__(self, original_stream, log_deque, also_print=True):
        self.original_stream = original_stream
        self.log_deque = log_deque
        self.buffer = ""
        self.also_print = also_print  # Whether to also write to original stream
        
    def write(self, text):
        # Optionally write to original stream (for visibility during setup)
        if self.also_print and self.original_stream:
            self.original_stream.write(text)
            self.original_stream.flush()
        
        # Add to buffer
        self.buffer += text
        
        # Process complete lines
        while '\n' in self.buffer:
            line, self.buffer = self.buffer.split('\n', 1)
            line = line.strip()
            if line:
                timestamp = datetime.now().strftime("%H:%M:%S")
                self.log_deque.append(f"[{timestamp}] {line}")
    
    def flush(self):
        # Flush any remaining buffer
        if self.buffer.strip():
            timestamp = datetime.now().strftime("%H:%M:%S")
            self.log_deque.append(f"[{timestamp}] {self.buffer.strip()}")
            self.buffer = ""
        if self.also_print and self.original_stream:
            self.original_stream.flush()
    
    def isatty(self):
        return False


class LogCaptureContext:
    """Context manager to capture all stdout/stderr and logging to log deque."""
    
    def __init__(self, log_deque):
        self.log_deque = log_deque
        self.original_stdout = None
        self.original_stderr = None
        self.log_handler = None
        
    def __enter__(self):
        import sys
        import logging
        
        # Capture stdout and stderr
        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr
        sys.stdout = GatewayLogCapture(self.original_stdout, self.log_deque)
        sys.stderr = GatewayLogCapture(self.original_stderr, self.log_deque)
        
        # Also capture logging output
        class LogDequeHandler(logging.Handler):
            def __init__(self, log_deque):
                super().__init__()
                self.log_deque = log_deque
                
            def emit(self, record):
                try:
                    msg = self.format(record)
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    self.log_deque.append(f"[{timestamp}] {msg}")
                except Exception:
                    self.handleError(record)
        
        self.log_handler = LogDequeHandler(self.log_deque)
        self.log_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(levelname)s:%(name)s: %(message)s')
        self.log_handler.setFormatter(formatter)
        
        # Add handler to root logger
        logging.root.addHandler(self.log_handler)
        
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        import sys
        import logging
        
        # Flush and restore stdout/stderr
        sys.stdout.flush()
        sys.stderr.flush()
        sys.stdout = self.original_stdout
        sys.stderr = self.original_stderr
        
        # Remove logging handler
        if self.log_handler:
            logging.root.removeHandler(self.log_handler)
        
        return False


class DashboardManager:
    """Manages the live terminal dashboard with cluster status and gateway logs."""
    
    def __init__(self, configurations, gateway_logs, cluster_status):
        """
        Initialize the dashboard manager.
        
        Args:
            configurations: List of cluster configurations
            gateway_logs: Deque of gateway log messages
            cluster_status: Dict mapping status keys to status info
        """
        self.configurations = configurations
        self.gateway_logs = gateway_logs
        self.cluster_status = cluster_status
        self.console = Console() if RICH_AVAILABLE else None
        self.live = None
        
        # Get terminal width for dynamic sizing
        try:
            import shutil
            self.terminal_width = shutil.get_terminal_size().columns
        except:
            self.terminal_width = 120  # Default fallback
        
    def create_layout(self):
        """Create the dashboard layout."""
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="cluster", size=12),
            Layout(name="gateway", ratio=1)
        )
        return layout
    
    def create_header(self):
        """Create header panel."""
        header_text = Text("🚀 SlurmCompose Live Dashboard", justify="center", style="bold cyan")
        return Panel(header_text, box=box.DOUBLE, style="cyan")
    
    def create_cluster_panel(self):
        """Create cluster status panel."""
        table = Table(
            box=box.ROUNDED, 
            show_header=True, 
            header_style="bold magenta", 
            border_style="cyan",
            expand=True,  # Expand to fill width
            width=self.terminal_width
        )
        table.add_column("Device", style="cyan")
        table.add_column("Spec", style="yellow")
        table.add_column("Count", justify="center", style="green")
        table.add_column("Status", justify="center", style="white")
        table.add_column("Jobs", justify="center", style="blue")
        
        # Add configuration rows
        for config in self.configurations:
            device = config.get('device_name', 'unknown')
            spec = config.get('script_spec', 'unknown')
            count = str(config.get('count', 1))
            
            # Get status from cluster_status if available
            status_key = f"{device}:{spec}"
            status_info = self.cluster_status.get(status_key, {})
            status = status_info.get('status', '⏳ pending')
            jobs = status_info.get('jobs', '-')
            
            table.add_row(device, spec, count, status, str(jobs))
        
        return Panel(table, title="📋 Cluster Composition", border_style="magenta", box=box.ROUNDED)
    
    def create_gateway_panel(self):
        """Create gateway logs panel."""
        log_text = Text()
        
        if not self.gateway_logs:
            log_text.append("Waiting for gateway logs...\n", style="dim")
        else:
            for log_line in self.gateway_logs:
                log_text.append(log_line + "\n")
        
        return Panel(
            log_text, 
            title="🌐 Gateway Logs", 
            border_style="blue", 
            box=box.ROUNDED,
            height=None,
            width=self.terminal_width,
            expand=True
        )
    
    def update_display(self, layout):
        """Update the dashboard display."""
        layout["header"].update(self.create_header())
        layout["cluster"].update(self.create_cluster_panel())
        layout["gateway"].update(self.create_gateway_panel())
    
    def start_live_display(self):
        """Start the live dashboard."""
        if not RICH_AVAILABLE:
            print(colored("⚠️  Rich library not available, falling back to regular output", "yellow"))
            return None
        
        layout = self.create_layout()
        self.update_display(layout)
        # Use screen=False to avoid full screen mode which causes conflicts with other outputs
        self.live = Live(layout, console=self.console, refresh_per_second=2, screen=False)
        return self.live


def is_rich_available():
    """Check if Rich library is available."""
    return RICH_AVAILABLE

