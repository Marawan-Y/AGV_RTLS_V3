#!/usr/bin/env python3
"""
Load zone definitions into the database from configuration files.
"""

import os
import sys
import json
import yaml
from pathlib import Path
from typing import Dict, List, Any
import click
from rich import print
from rich.console import Console
from rich.table import Table
from shapely.geometry import Polygon
import numpy as np

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.core.database import db_manager
from src.core.zone_manager import ZoneManager

console = Console()


class ZoneLoader:
    """Loads zone definitions into the database."""
    
    def __init__(self):
        self.zone_manager = ZoneManager()
        self.zones_loaded = 0
        self.zones_failed = 0
    
    def load_from_yaml(self, file_path: str) -> bool:
        """Load zones from YAML configuration file."""
        try:
            with open(file_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if 'zones' not in config:
                console.print("[red]No zones found in configuration file[/red]")
                return False
            
            zones = config['zones']
            console.print(f"[cyan]Found {len(zones)} zones to load[/cyan]")
            
            for zone_config in zones:
                self._load_zone(zone_config)
            
            console.print(f"\n[green]Successfully loaded {self.zones_loaded} zones[/green]")
            if self.zones_failed > 0:
                console.print(f"[red]Failed to load {self.zones_failed} zones[/red]")
            
            return True
            
        except Exception as e:
            console.print(f"[red]Error loading zones from YAML: {e}[/red]")
            return False
    
    def load_from_geojson(self, file_path: str) -> bool:
        """Load zones from GeoJSON file."""
        try:
            with open(file_path, 'r') as f:
                geojson = json.load(f)
            
            if 'features' not in geojson:
                console.print("[red]Invalid GeoJSON format[/red]")
                return False
            
            features = geojson['features']
            console.print(f"[cyan]Found {len(features)} features to load[/cyan]")
            
            for feature in features:
                self._load_geojson_feature(feature)
            
            console.print(f"\n[green]Successfully loaded {self.zones_loaded} zones[/green]")
            if self.zones_failed > 0:
                console.print(f"[red]Failed to load {self.zones_failed} zones[/red]")
            
            return True
            
        except Exception as e:
            console.print(f"[red]Error loading zones from GeoJSON: {e}[/red]")
            return False
    
    def _load_zone(self, zone_config: Dict) -> bool:
        """Load a single zone from configuration."""
        try:
            zone_id = zone_config.get('zone_id')
            if not zone_id:
                console.print("[yellow]Skipping zone without ID[/yellow]")
                return False
            
            # Calculate centroid and area if vertices provided
            centroid_x, centroid_y, area = None, None, None
            if 'vertices' in zone_config and zone_config['vertices']:
                vertices = zone_config['vertices']
                polygon = Polygon(vertices)
                
                if polygon.is_valid:
                    centroid = polygon.centroid
                    centroid_x = centroid.x
                    centroid_y = centroid.y
                    area = polygon.area
                else:
                    console.print(f"[yellow]Invalid polygon for zone {zone_id}[/yellow]")
            
            # Insert or update zone in database
            query = """
                INSERT INTO plant_zones (
                    zone_id, name, category, zone_type, 
                    max_speed_mps, max_agvs, priority,
                    vertices, centroid_x, centroid_y, area_sqm
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    category = VALUES(category),
                    zone_type = VALUES(zone_type),
                    max_speed_mps = VALUES(max_speed_mps),
                    max_agvs = VALUES(max_agvs),
                    priority = VALUES(priority),
                    vertices = VALUES(vertices),
                    centroid_x = VALUES(centroid_x),
                    centroid_y = VALUES(centroid_y),
                    area_sqm = VALUES(area_sqm),
                    updated_at = CURRENT_TIMESTAMP
            """
            
            params = (
                zone_id,
                zone_config.get('name', zone_id),
                zone_config.get('category', 'OPERATIONAL'),
                zone_config.get('type', zone_config.get('zone_type', 'OPERATIONAL')),
                zone_config.get('max_speed_mps', 2.0),
                zone_config.get('max_agvs', 5),
                zone_config.get('priority', 5),
                json.dumps(zone_config.get('vertices', [])),
                centroid_x,
                centroid_y,
                area
            )
            
            db_manager.execute_query(query, params)
            self.zones_loaded += 1
            console.print(f"[green]✓[/green] Loaded zone: {zone_id}")
            return True
            
        except Exception as e:
            console.print(f"[red]✗ Failed to load zone {zone_config.get('zone_id', 'unknown')}: {e}[/red]")
            self.zones_failed += 1
            return False
    
    def _load_geojson_feature(self, feature: Dict) -> bool:
        """Load a zone from GeoJSON feature."""
        try:
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            
            # Extract zone ID
            zone_id = (properties.get('zone_id') or 
                      properties.get('id') or 
                      properties.get('name', '').replace(' ', '_'))
            
            if not zone_id:
                console.print("[yellow]Skipping feature without ID[/yellow]")
                return False
            
            # Extract vertices from geometry
            vertices = []
            if geometry.get('type') == 'Polygon':
                coordinates = geometry.get('coordinates', [[]])[0]
                vertices = [[coord[0], coord[1]] for coord in coordinates]
            
            # Create zone configuration
            zone_config = {
                'zone_id': zone_id,
                'name': properties.get('name', zone_id),
                'category': properties.get('category', 'OPERATIONAL'),
                'zone_type': properties.get('zone_type', 'OPERATIONAL'),
                'max_speed_mps': properties.get('max_speed_mps', 2.0),
                'max_agvs': properties.get('max_agvs', 5),
                'priority': properties.get('priority', 5),
                'vertices': vertices
            }
            
            return self._load_zone(zone_config)
            
        except Exception as e:
            console.print(f"[red]✗ Failed to load GeoJSON feature: {e}[/red]")
            self.zones_failed += 1
            return False
    
    def list_zones(self):
        """List all zones in the database."""
        zones = db_manager.query_dataframe("""
            SELECT 
                zone_id, name, category, zone_type,
                max_agvs, max_speed_mps, priority,
                area_sqm, active
            FROM plant_zones
            ORDER BY category, zone_id
        """)
        
        if zones.empty:
            console.print("[yellow]No zones found in database[/yellow]")
            return
        
        table = Table(title="Plant Zones")
        table.add_column("Zone ID", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Category")
        table.add_column("Type")
        table.add_column("Max AGVs", justify="right")
        table.add_column("Max Speed", justify="right")
        table.add_column("Priority", justify="right")
        table.add_column("Area (m²)", justify="right")
        table.add_column("Active", justify="center")
        
        for _, zone in zones.iterrows():
            active_indicator = "✓" if zone['active'] else "✗"
            active_color = "green" if zone['active'] else "red"
            
            table.add_row(
                zone['zone_id'],
                zone['name'],
                zone['category'],
                zone['zone_type'],
                str(zone['max_agvs']),
                f"{zone['max_speed_mps']:.1f}",
                str(zone['priority']),
                f"{zone['area_sqm']:.1f}" if zone['area_sqm'] else "N/A",
                f"[{active_color}]{active_indicator}[/{active_color}]"
            )
        
        console.print(table)
        console.print(f"\nTotal zones: {len(zones)}")
    
    def validate_zones(self) -> bool:
        """Validate zone definitions for issues."""
        console.print("[cyan]Validating zones...[/cyan]")
        
        issues = []
        
        # Check for overlapping zones
        zones = self.zone_manager.get_all_zones()
        
        for zone_id, zone_info in zones.items():
            # Check if zone has vertices
            if 'vertices' not in zone_info or not zone_info['vertices']:
                issues.append(f"Zone {zone_id} has no vertices defined")
            
            # Check for reasonable limits
            if zone_info.get('max_agvs', 0) == 0:
                issues.append(f"Zone {zone_id} has max_agvs set to 0")
            
            if zone_info.get('max_speed_mps', 0) == 0:
                issues.append(f"Zone {zone_id} has max_speed_mps set to 0")
        
        # Check for zone connectivity
        if len(zones) > 1:
            # Build adjacency graph
            adjacent_zones = {}
            for zone_id in zones:
                adjacent = self.zone_manager.get_adjacent_zones(zone_id)
                if adjacent:
                    adjacent_zones[zone_id] = adjacent
            
            # Check for isolated zones
            for zone_id in zones:
                if zone_id not in adjacent_zones or not adjacent_zones[zone_id]:
                    issues.append(f"Zone {zone_id} appears to be isolated (no adjacent zones)")
        
        if issues:
            console.print("\n[yellow]Validation issues found:[/yellow]")
            for issue in issues:
                console.print(f"  • {issue}")
            return False
        else:
            console.print("[green]✓ All zones validated successfully[/green]")
            return True
    
    def clear_zones(self):
        """Clear all zones from database (with confirmation)."""
        console.print("[red]WARNING: This will delete all zone definitions![/red]")
        
        confirm = input("Type 'DELETE ALL ZONES' to confirm: ")
        if confirm != 'DELETE ALL ZONES':
            console.print("[yellow]Operation cancelled[/yellow]")
            return
        
        try:
            db_manager.execute_query("DELETE FROM plant_zones")
            console.print("[green]All zones deleted[/green]")
        except Exception as e:
            console.print(f"[red]Error deleting zones: {e}[/red]")


@click.command()
@click.option('--yaml', '-y', type=click.Path(exists=True), help='Load zones from YAML file')
@click.option('--geojson', '-g', type=click.Path(exists=True), help='Load zones from GeoJSON file')
@click.option('--default', '-d', is_flag=True, help='Load default zones from config')
@click.option('--list', '-l', is_flag=True, help='List all zones')
@click.option('--validate', '-v', is_flag=True, help='Validate zone definitions')
@click.option('--clear', is_flag=True, help='Clear all zones (requires confirmation)')
def main(yaml, geojson, default, list, validate, clear):
    """Zone management tool for AGV RTLS system."""
    
    loader = ZoneLoader()
    
    if clear:
        loader.clear_zones()
        return
    
    if list:
        loader.list_zones()
        return
    
    if validate:
        loader.validate_zones()
        return
    
    if yaml:
        console.print(f"[cyan]Loading zones from YAML: {yaml}[/cyan]")
        loader.load_from_yaml(yaml)
    
    elif geojson:
        console.print(f"[cyan]Loading zones from GeoJSON: {geojson}[/cyan]")
        loader.load_from_geojson(geojson)
    
    elif default:
        # Load from default configuration
        default_path = Path('config/zones_config.yaml')
        if default_path.exists():
            console.print(f"[cyan]Loading zones from default config: {default_path}[/cyan]")
            loader.load_from_yaml(str(default_path))
        else:
            console.print("[red]Default configuration not found[/red]")
    
    else:
        console.print("[yellow]No input specified. Use --help for options[/yellow]")


if __name__ == "__main__":
    main()