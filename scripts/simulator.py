#!/usr/bin/env python3
"""
AGV RTLS Simulator
Generates realistic AGV movement data for testing.
"""

import os
import sys
import json
import time
import random
import asyncio
import threading
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional
from pathlib import Path
import numpy as np
import click
from rich import print
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
import paho.mqtt.client as mqtt
import yaml

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))


console = Console()


class AGVSimulator:
    """Simulates multiple AGVs with realistic movement patterns."""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config = self._load_config(config_file)
        self.agvs = []
        self.running = False
        self.mqtt_client = None
        self.stats = {
            'messages_sent': 0,
            'start_time': time.time(),
            'errors': 0
        }
        
    def _load_config(self, config_file: Optional[str]) -> Dict:
        """Load simulator configuration."""
        default_config = {
            'mqtt': {
                'broker': os.getenv('MQTT_BROKER', 'localhost'),
                'port': int(os.getenv('MQTT_PORT', 1883)),
                'topic_pattern': 'rtls/{agv_id}/position'
            },
            'simulation': {
                'num_agvs': 5,
                'sample_rate_hz': 3,
                'speed_variation': 0.2,
                'position_noise': 0.1,
                'battery_drain_rate': 0.001
            },
            'plant': {
                'xmin': 0, 'xmax': 200,
                'ymin': 0, 'ymax': 150
            }
        }
        
        if config_file and Path(config_file).exists():
            with open(config_file, 'r') as f:
                custom_config = yaml.safe_load(f)
                # Merge configs
                for key in custom_config:
                    if key in default_config:
                        default_config[key].update(custom_config[key])
                    else:
                        default_config[key] = custom_config[key]
        
        return default_config
    
    def _init_mqtt(self):
        """Initialize MQTT client."""
        self.mqtt_client = mqtt.Client(client_id=f"simulator_{os.getpid()}")
        
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                console.print(f"[green]Connected to MQTT broker[/green]")
            else:
                console.print(f"[red]Failed to connect: {rc}[/red]")
        
        self.mqtt_client.on_connect = on_connect
        
        try:
            self.mqtt_client.connect(
                self.config['mqtt']['broker'],
                self.config['mqtt']['port'],
                60
            )
            self.mqtt_client.loop_start()
            return True
        except Exception as e:
            console.print(f"[red]MQTT connection failed: {e}[/red]")
            return False
    
    def create_agvs(self, num_agvs: int):
        """Create simulated AGVs."""
        agv_types = ['TUGGER', 'FORKLIFT', 'PALLET_JACK', 'AMR']
        
        for i in range(num_agvs):
            agv = SimulatedAGV(
                agv_id=f"AGV_SIM_{i+1:02d}",
                agv_type=random.choice(agv_types),
                plant_bounds=self.config['plant'],
                config=self.config['simulation']
            )
            self.agvs.append(agv)
        
        console.print(f"[green]Created {num_agvs} simulated AGVs[/green]")
    
    def publish_positions(self):
        """Publish AGV positions via MQTT."""
        while self.running:
            try:
                for agv in self.agvs:
                    # Update AGV position
                    agv.update()
                    
                    # Create message
                    message = agv.get_message()
                    
                    # Publish to MQTT
                    topic = self.config['mqtt']['topic_pattern'].format(agv_id=agv.agv_id)
                    self.mqtt_client.publish(
                        topic,
                        json.dumps(message),
                        qos=1
                    )
                    
                    self.stats['messages_sent'] += 1
                
                # Sleep to maintain sample rate
                time.sleep(1.0 / self.config['simulation']['sample_rate_hz'])
                
            except Exception as e:
                console.print(f"[red]Error publishing: {e}[/red]")
                self.stats['errors'] += 1
    
    def display_status(self):
        """Display live status table."""
        def generate_table():
            table = Table(title="AGV Simulator Status")
            table.add_column("AGV ID", style="cyan")
            table.add_column("Type", style="green")
            table.add_column("Position", justify="right")
            table.add_column("Speed", justify="right")
            table.add_column("Heading", justify="right")
            table.add_column("Battery", justify="right")
            table.add_column("Status", justify="center")
            
            for agv in self.agvs:
                status_color = "green" if agv.status == "ACTIVE" else "yellow"
                table.add_row(
                    agv.agv_id,
                    agv.agv_type,
                    f"({agv.x:.1f}, {agv.y:.1f})",
                    f"{agv.speed:.2f} m/s",
                    f"{agv.heading:.0f}°",
                    f"{agv.battery:.0f}%",
                    f"[{status_color}]{agv.status}[/{status_color}]"
                )
            
            # Add statistics panel
            runtime = time.time() - self.stats['start_time']
            rate = self.stats['messages_sent'] / runtime if runtime > 0 else 0
            
            stats_text = (
                f"Messages Sent: {self.stats['messages_sent']}\n"
                f"Rate: {rate:.1f} msg/s\n"
                f"Errors: {self.stats['errors']}\n"
                f"Runtime: {runtime:.0f}s"
            )
            
            layout = Layout()
            layout.split_column(
                Layout(Panel(table)),
                Layout(Panel(stats_text, title="Statistics"), size=6)
            )
            
            return layout
        
        with Live(generate_table(), refresh_per_second=2) as live:
            while self.running:
                time.sleep(0.5)
                live.update(generate_table())
    
    def run(self, duration: Optional[int] = None):
        """Run the simulator."""
        if not self._init_mqtt():
            return
        
        self.running = True
        
        # Start publishing thread
        publish_thread = threading.Thread(target=self.publish_positions)
        publish_thread.daemon = True
        publish_thread.start()
        
        # Start display thread
        display_thread = threading.Thread(target=self.display_status)
        display_thread.daemon = True
        display_thread.start()
        
        try:
            if duration:
                console.print(f"[cyan]Running for {duration} seconds...[/cyan]")
                time.sleep(duration)
            else:
                console.print("[cyan]Running... Press Ctrl+C to stop[/cyan]")
                while True:
                    time.sleep(1)
                    
        except KeyboardInterrupt:
            console.print("\n[yellow]Stopping simulator...[/yellow]")
        
        finally:
            self.stop()
    
    def stop(self):
        """Stop the simulator."""
        self.running = False
        
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        
        console.print(f"[green]Simulator stopped. Sent {self.stats['messages_sent']} messages[/green]")


class SimulatedAGV:
    """Represents a simulated AGV with realistic movement."""
    
    def __init__(self, agv_id: str, agv_type: str, plant_bounds: Dict, config: Dict):
        self.agv_id = agv_id
        self.agv_type = agv_type
        self.config = config
        self.bounds = plant_bounds
        
        # Initialize position
        self.x = random.uniform(bounds['xmin'] + 10, bounds['xmax'] - 10)
        self.y = random.uniform(bounds['ymin'] + 10, bounds['ymax'] - 10)
        self.heading = random.uniform(0, 360)
        
        # Movement parameters
        self.base_speed = random.uniform(0.5, 2.0)
        self.speed = self.base_speed
        self.target_x = self.x
        self.target_y = self.y
        
        # Status
        self.battery = random.uniform(60, 100)
        self.status = "ACTIVE"
        self.quality = random.uniform(0.8, 1.0)
        
        # Path planning
        self.waypoints = self._generate_path()
        self.current_waypoint = 0
    
    def _generate_path(self) -> List[Tuple[float, float]]:
        """Generate a random path through the plant."""
        num_waypoints = random.randint(5, 15)
        waypoints = []
        
        for _ in range(num_waypoints):
            x = random.uniform(
                self.bounds['xmin'] + 5,
                self.bounds['xmax'] - 5
            )
            y = random.uniform(
                self.bounds['ymin'] + 5,
                self.bounds['ymax'] - 5
            )
            waypoints.append((x, y))
        
        return waypoints
    
    def update(self):
        """Update AGV position and status."""
        # Check if reached current waypoint
        if self.current_waypoint < len(self.waypoints):
            target = self.waypoints[self.current_waypoint]
            distance = np.sqrt((target[0] - self.x)**2 + (target[1] - self.y)**2)
            
            if distance < 2.0:  # Reached waypoint
                self.current_waypoint += 1
                if self.current_waypoint >= len(self.waypoints):
                    # Generate new path
                    self.waypoints = self._generate_path()
                    self.current_waypoint = 0
            
            # Move towards target
            if self.current_waypoint < len(self.waypoints):
                target = self.waypoints[self.current_waypoint]
                
                # Calculate heading
                dx = target[0] - self.x
                dy = target[1] - self.y
                self.heading = np.degrees(np.arctan2(dy, dx)) % 360
                
                # Add some noise to heading
                self.heading += random.gauss(0, 5)
                self.heading = self.heading % 360
                
                # Calculate speed with variation
                self.speed = self.base_speed * (1 + random.gauss(0, self.config['speed_variation']))
                self.speed = max(0.1, min(3.0, self.speed))
                
                # Update position
                dt = 1.0 / self.config['sample_rate_hz']
                self.x += self.speed * np.cos(np.radians(self.heading)) * dt
                self.y += self.speed * np.sin(np.radians(self.heading)) * dt
                
                # Add position noise
                self.x += random.gauss(0, self.config['position_noise'])
                self.y += random.gauss(0, self.config['position_noise'])
                
                # Keep within bounds
                self.x = max(self.bounds['xmin'], min(self.bounds['xmax'], self.x))
                self.y = max(self.bounds['ymin'], min(self.bounds['ymax'], self.y))
        
        # Update battery
        self.battery -= self.config['battery_drain_rate'] * self.speed
        self.battery = max(0, self.battery)
        
        # Update status based on battery
        if self.battery < 20:
            self.status = "CHARGING" if random.random() < 0.1 else "LOW_BATTERY"
        elif random.random() < 0.01:  # Occasional idle
            self.status = "IDLE"
        else:
            self.status = "ACTIVE"
        
        # Update quality (signal strength simulation)
        self.quality = min(1.0, max(0.3, self.quality + random.gauss(0, 0.05)))
    
    def get_message(self) -> Dict:
        """Get MQTT message for current state."""
        # Convert to WGS84 (fake conversion for simulation)
        lat = 49.0 + (self.y / 111000)  # Rough conversion
        lon = 12.0 + (self.x / 111000)
        
        return {
            'ts': datetime.now(timezone.utc).isoformat(),
            'agv_id': self.agv_id,
            'lat': lat,
            'lon': lon,
            'plant_x': self.x,
            'plant_y': self.y,
            'heading_deg': self.heading,
            'speed_mps': self.speed,
            'quality': self.quality,
            'battery_percent': self.battery,
            'status': self.status,
            'satellites': random.randint(8, 12),
            'hdop': random.uniform(0.5, 1.5)
        }


@click.command()
@click.option('--agvs', '-n', default=5, help='Number of AGVs to simulate')
@click.option('--duration', '-d', type=int, help='Duration in seconds (runs forever if not set)')
@click.option('--config', '-c', type=click.Path(exists=True), help='Configuration file')
@click.option('--rate', '-r', default=3, help='Sample rate in Hz')
@click.option('--broker', '-b', help='MQTT broker address')
def main(agvs, duration, config, rate, broker):
    """AGV RTLS Simulator
    
    Simulates multiple AGVs with realistic movement patterns
    for testing the RTLS dashboard.
    """
    
    console.print("[bold magenta]AGV RTLS Simulator[/bold magenta]")
    console.print("=" * 50)
    
    # Create simulator
    simulator = AGVSimulator(config)
    
    # Override settings if provided
    if rate:
        simulator.config['simulation']['sample_rate_hz'] = rate
    if broker:
        simulator.config['mqtt']['broker'] = broker
    
    # Create AGVs
    simulator.create_agvs(agvs)
    
    # Run simulation
    simulator.run(duration)


if __name__ == "__main__":
    main()