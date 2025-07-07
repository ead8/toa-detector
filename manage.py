#!/usr/bin/env python3
"""
TAO-DETECTOR Management Script
Simple interface to manage the Docker-based TAO-DETECTOR system
"""

import subprocess
import sys
import time
import requests
import json

def run_command(cmd, check=True):
    """Run a shell command"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=check)
        return result.stdout.strip(), result.stderr.strip()
    except subprocess.CalledProcessError as e:
        return e.stdout.strip(), e.stderr.strip()

def check_docker():
    """Check if Docker is installed and running"""
    stdout, stderr = run_command("docker --version", check=False)
    if "Docker version" not in stdout:
        print("❌ Docker is not installed or not in PATH")
        return False
    
    stdout, stderr = run_command("docker compose --version", check=False)
    if "docker compose version" not in stdout:
        print("❌ Docker Compose is not installed or not in PATH")
        return False
    
    return True

def status():
    """Show system status"""
    print("📊 TAO-DETECTOR Status")
    print("=" * 50)
    
    # Container status
    stdout, stderr = run_command("docker compose ps", check=False)
    print("🐳 Container Status:")
    print(stdout if stdout else "No containers running")
    
    # Health checks
    print("\n🏥 Health Checks:")
    try:
        response = requests.get("http://localhost:8082/health", timeout=5)
        binance_status = response.json().get('status', 'unknown')
        print(f"Binance: {binance_status}")
    except:
        print("Binance: Not responding")
    
    try:
        response = requests.get("http://localhost:8083/health", timeout=5)
        okx_status = response.json().get('status', 'unknown')
        print(f"OKX: {okx_status}")
    except:
        print("OKX: Not responding")

def deploy():
    """Deploy the system"""
    if not check_docker():
        print("Please install Docker and Docker Compose first")
        return
    
    print("🚀 Deploying TAO-DETECTOR...")
    
    # Stop existing containers
    print("Stopping existing containers...")
    run_command("docker compose down", check=False)
    
    # Build and start
    print("Building and starting services...")
    stdout, stderr = run_command("docker compose up --build -d")
    
    if stderr and "ERROR" in stderr:
        print(f"❌ Error during deployment: {stderr}")
        return
    
    print("✅ Deployment started successfully!")
    print("Waiting for services to initialize...")
    time.sleep(30)
    
    status()

def logs():
    """Show logs"""
    print("📝 Recent Logs:")
    stdout, stderr = run_command("docker compose logs --tail=50")
    print(stdout)

def stop():
    """Stop all services"""
    print("🛑 Stopping TAO-DETECTOR...")
    run_command("docker compose down")
    print("✅ Services stopped")

def restart():
    """Restart services"""
    print("🔄 Restarting TAO-DETECTOR...")
    run_command("docker compose restart")
    print("✅ Services restarted")

def main():
    if len(sys.argv) < 2:
        print("TAO-DETECTOR Management Script")
        print("Usage: python manage.py [command]")
        print("\nCommands:")
        print("  deploy   - Deploy the system")
        print("  status   - Show system status")
        print("  logs     - Show recent logs")
        print("  stop     - Stop all services")
        print("  restart  - Restart services")
        print("\nHealth Check URLs:")
        print("  Binance: http://localhost:8082/health")
        print("  OKX:     http://localhost:8083/health")
        return
    
    command = sys.argv[1].lower()
    
    if command == "deploy":
        deploy()
    elif command == "status":
        status()
    elif command == "logs":
        logs()
    elif command == "stop":
        stop()
    elif command == "restart":
        restart()
    else:
        print(f"Unknown command: {command}")

if __name__ == "__main__":
    main()
