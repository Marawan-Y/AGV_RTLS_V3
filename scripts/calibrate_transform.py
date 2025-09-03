#!/usr/bin/env python3
"""
Calibration tool for coordinate transformation.
Maps real-world coordinates to plant coordinate system.
"""

import os
import sys
import json
import numpy as np
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import click
from rich import print
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, Confirm
import yaml

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.core.transforms import TransformManager
from src.core.database import db_manager


console = Console()


class CalibrationTool:
    """Interactive calibration tool for coordinate transformation."""
    
    def __init__(self):
        self.transform_manager = TransformManager()
        self.control_points = []
        self.calibration_file = Path('assets/calibration/control_points.json')
        self.matrix_file = Path('assets/calibration/affine_matrix.npy')
        
    def load_existing_points(self) -> List[Dict]:
        """Load existing control points if available."""
        if self.calibration_file.exists():
            with open(self.calibration_file, 'r') as f:
                data = json.load(f)
                return data.get('control_points', [])
        return []
    
    def save_control_points(self):
        """Save control points to file."""
        self.calibration_file.parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            'control_points': self.control_points,
            'metadata': {
                'utm_zone': os.getenv('UTM_EPSG', 32633),
                'plant_crs': 'LOCAL',
                'units': 'meters',
                'calibration_date': str(np.datetime64('today'))
            }
        }
        
        with open(self.calibration_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        console.print(f"[green]✓ Saved {len(self.control_points)} control points[/green]")
    
    def add_control_point(self):
        """Interactively add a control point."""
        console.print("\n[bold]Add Control Point[/bold]")
        
        name = Prompt.ask("Point name/description")
        
        console.print("\n[cyan]World Coordinates (WGS84 or UTM):[/cyan]")
        coord_type = Prompt.ask("Coordinate type", choices=["wgs84", "utm"])
        
        if coord_type == "wgs84":
            lat = float(Prompt.ask("Latitude"))
            lon = float(Prompt.ask("Longitude"))
            # Convert to UTM
            utm_x, utm_y = self.transform_manager.transformer.transform(lon, lat)
            world_x, world_y = utm_x, utm_y
        else:
            world_x = float(Prompt.ask("UTM X (meters)"))
            world_y = float(Prompt.ask("UTM Y (meters)"))
        
        console.print("\n[cyan]Plant Coordinates (from CAD):[/cyan]")
        plant_x = float(Prompt.ask("Plant X (meters)"))
        plant_y = float(Prompt.ask("Plant Y (meters)"))
        
        # Optional: Get from actual AGV position
        if Confirm.ask("\nGet current position from AGV?"):
            agv_id = Prompt.ask("AGV ID")
            position = self._get_agv_position(agv_id)
            if position:
                if Confirm.ask(f"Use position ({position['lat']:.6f}, {position['lon']:.6f})?"):
                    world_x, world_y = self.transform_manager.transformer.transform(
                        position['lon'], position['lat']
                    )
        
        control_point = {
            'name': name,
            'world_x': world_x,
            'world_y': world_y,
            'plant_x': plant_x,
            'plant_y': plant_y
        }
        
        self.control_points.append(control_point)
        console.print(f"[green]✓ Added control point '{name}'[/green]")
    
    def _get_agv_position(self, agv_id: str) -> Optional[Dict]:
        """Get current AGV position from database."""
        try:
            result = db_manager.execute_query("""
                SELECT lat, lon, plant_x, plant_y 
                FROM agv_positions 
                WHERE agv_id = %s 
                ORDER BY ts DESC 
                LIMIT 1
            """, (agv_id,))
            
            if result:
                return result[0]
        except Exception as e:
            console.print(f"[red]Error getting AGV position: {e}[/red]")
        
        return None
    
    def display_points(self):
        """Display current control points."""
        if not self.control_points:
            console.print("[yellow]No control points defined[/yellow]")
            return
        
        table = Table(title="Control Points")
        table.add_column("Name", style="cyan")
        table.add_column("World X", justify="right")
        table.add_column("World Y", justify="right")
        table.add_column("Plant X", justify="right")
        table.add_column("Plant Y", justify="right")
        
        for point in self.control_points:
            table.add_row(
                point['name'],
                f"{point['world_x']:.2f}",
                f"{point['world_y']:.2f}",
                f"{point['plant_x']:.2f}",
                f"{point['plant_y']:.2f}"
            )
        
        console.print(table)
    
    def calibrate(self):
        """Perform calibration with current control points."""
        if len(self.control_points) < 3:
            console.print("[red]Need at least 3 control points for calibration[/red]")
            return False
        
        console.print("\n[bold]Performing Calibration...[/bold]")
        
        try:
            # Extract coordinates
            world_coords = np.array([
                [p['world_x'], p['world_y'], 1.0] 
                for p in self.control_points
            ])
            plant_coords = np.array([
                [p['plant_x'], p['plant_y'], 1.0] 
                for p in self.control_points
            ])
            
            # Compute affine transformation
            affine_matrix, residuals, rank, s = np.linalg.lstsq(
                world_coords, plant_coords, rcond=None
            )
            
            # Validate transformation
            errors = []
            console.print("\n[cyan]Validation Results:[/cyan]")
            
            table = Table(title="Calibration Errors")
            table.add_column("Point", style="cyan")
            table.add_column("Error (m)", justify="right")
            table.add_column("Status", justify="center")
            
            for point, wc, pc in zip(self.control_points, world_coords, plant_coords):
                predicted = affine_matrix.T @ wc
                error = np.linalg.norm(predicted[:2] - pc[:2])
                errors.append(error)
                
                status = "✓" if error < 0.5 else "⚠" if error < 1.0 else "✗"
                status_color = "green" if error < 0.5 else "yellow" if error < 1.0 else "red"
                
                table.add_row(
                    point['name'],
                    f"{error:.3f}",
                    f"[{status_color}]{status}[/{status_color}]"
                )
            
            console.print(table)
            
            mean_error = np.mean(errors)
            max_error = np.max(errors)
            
            console.print(f"\n[bold]Calibration Statistics:[/bold]")
            console.print(f"Mean error: {mean_error:.3f} meters")
            console.print(f"Max error: {max_error:.3f} meters")
            console.print(f"Matrix rank: {rank}")
            
            if mean_error > 1.0:
                console.print("[yellow]⚠ Warning: High calibration error detected[/yellow]")
                if not Confirm.ask("Continue with this calibration?"):
                    return False
            
            # Save calibration matrix
            self.matrix_file.parent.mkdir(parents=True, exist_ok=True)
            np.save(self.matrix_file, affine_matrix.T)
            console.print(f"[green]✓ Saved calibration matrix to {self.matrix_file}[/green]")
            
            # Save to database
            if Confirm.ask("\nSave calibration points to database?"):
                self._save_to_database()
            
            return True
            
        except Exception as e:
            console.print(f"[red]Calibration failed: {e}[/red]")
            return False
    
    def _save_to_database(self):
        """Save calibration points to database."""
        try:
            for point in self.control_points:
                db_manager.execute_query("""
                    INSERT INTO calibration_points 
                    (name, world_x, world_y, world_crs, plant_x, plant_y, quality)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    world_x = VALUES(world_x),
                    world_y = VALUES(world_y),
                    plant_x = VALUES(plant_x),
                    plant_y = VALUES(plant_y)
                """, (
                    point['name'],
                    point['world_x'],
                    point['world_y'],
                    f"EPSG:{os.getenv('UTM_EPSG', 32633)}",
                    point['plant_x'],
                    point['plant_y'],
                    1.0
                ))
            
            console.print("[green]✓ Saved calibration points to database[/green]")
            
        except Exception as e:
            console.print(f"[red]Failed to save to database: {e}[/red]")
    
    def test_transformation(self):
        """Test the transformation with sample coordinates."""
        if not self.matrix_file.exists():
            console.print("[red]No calibration matrix found. Run calibration first.[/red]")
            return
        
        console.print("\n[bold]Test Transformation[/bold]")
        
        coord_type = Prompt.ask("Input coordinate type", choices=["wgs84", "utm", "plant"])
        
        if coord_type == "wgs84":
            lat = float(Prompt.ask("Latitude"))
            lon = float(Prompt.ask("Longitude"))
            
            # Transform through the pipeline
            utm_x, utm_y = self.transform_manager.transformer.transform(lon, lat)
            matrix = np.load(self.matrix_file)
            plant_coords = matrix @ np.array([utm_x, utm_y, 1.0])
            
            console.print(f"\n[cyan]Results:[/cyan]")
            console.print(f"WGS84: ({lat:.6f}, {lon:.6f})")
            console.print(f"UTM: ({utm_x:.2f}, {utm_y:.2f})")
            console.print(f"Plant: ({plant_coords[0]:.2f}, {plant_coords[1]:.2f})")
            
        elif coord_type == "utm":
            utm_x = float(Prompt.ask("UTM X"))
            utm_y = float(Prompt.ask("UTM Y"))
            
            matrix = np.load(self.matrix_file)
            plant_coords = matrix @ np.array([utm_x, utm_y, 1.0])
            
            console.print(f"\n[cyan]Results:[/cyan]")
            console.print(f"UTM: ({utm_x:.2f}, {utm_y:.2f})")
            console.print(f"Plant: ({plant_coords[0]:.2f}, {plant_coords[1]:.2f})")
            
        else:  # plant
            plant_x = float(Prompt.ask("Plant X"))
            plant_y = float(Prompt.ask("Plant Y"))
            
            # Inverse transform
            matrix = np.load(self.matrix_file)
            inv_matrix = np.linalg.inv(matrix)
            utm_coords = inv_matrix @ np.array([plant_x, plant_y, 1.0])
            
            console.print(f"\n[cyan]Results:[/cyan]")
            console.print(f"Plant: ({plant_x:.2f}, {plant_y:.2f})")
            console.print(f"UTM: ({utm_coords[0]:.2f}, {utm_coords[1]:.2f})")
    
    def run_interactive(self):
        """Run interactive calibration session."""
        console.print("[bold magenta]AGV RTLS Calibration Tool[/bold magenta]")
        console.print("=" * 50)
        
        # Load existing points
        existing = self.load_existing_points()
        if existing:
            console.print(f"[cyan]Found {len(existing)} existing control points[/cyan]")
            if Confirm.ask("Load existing points?"):
                self.control_points = existing
                self.display_points()
        
        while True:
            console.print("\n[bold]Options:[/bold]")
            console.print("1. Add control point")
            console.print("2. Display points")
            console.print("3. Remove point")
            console.print("4. Run calibration")
            console.print("5. Test transformation")
            console.print("6. Save and exit")
            console.print("7. Exit without saving")
            
            choice = Prompt.ask("Select option", choices=["1","2","3","4","5","6","7"])
            
            if choice == "1":
                self.add_control_point()
            elif choice == "2":
                self.display_points()
            elif choice == "3":
                self.display_points()
                if self.control_points:
                    idx = int(Prompt.ask("Point index to remove")) - 1
                    if 0 <= idx < len(self.control_points):
                        removed = self.control_points.pop(idx)
                        console.print(f"[green]Removed point '{removed['name']}'[/green]")
            elif choice == "4":
                if self.calibrate():
                    console.print("[green]✓ Calibration successful[/green]")
            elif choice == "5":
                self.test_transformation()
            elif choice == "6":
                self.save_control_points()
                console.print("[green]Goodbye![/green]")
                break
            elif choice == "7":
                if Confirm.ask("Exit without saving?"):
                    console.print("[yellow]Exiting without saving[/yellow]")
                    break


@click.command()
@click.option('--interactive', '-i', is_flag=True, help='Run interactive mode')
@click.option('--test', '-t', is_flag=True, help='Test existing calibration')
@click.option('--load', '-l', type=click.Path(exists=True), help='Load control points from file')
def main(interactive, test, load):
    """AGV RTLS Calibration Tool
    
    Calibrates the transformation between world coordinates (WGS84/UTM)
    and plant coordinates (CAD/local).
    """
    
    tool = CalibrationTool()
    
    if load:
        with open(load, 'r') as f:
            data = json.load(f)
            tool.control_points = data.get('control_points', [])
        console.print(f"[green]Loaded {len(tool.control_points)} points from {load}[/green]")
        tool.display_points()
        if tool.calibrate():
            tool.save_control_points()
    
    elif test:
        tool.test_transformation()
    
    else:
        tool.run_interactive()


if __name__ == "__main__":
    main()