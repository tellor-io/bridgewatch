# bridgewatch - Bridge Data Monitor

A monitoring and validation system for Tellor data bridges that detects malicious behavior and ensures data integrity between Tellor Layer and EVM chains.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure environment:
   ```bash
   python3 setup_env.py
   ```

3. Validate configuration:
   ```bash
   ./bridgewatch config validate
   ```

## Bridgewatch Commands

### Monitoring
- `./bridgewatch start` - Start continuous monitoring (default 5min interval)
- `./bridgewatch start --once` - Run once and exit
- `./bridgewatch start --interval 600` - Custom monitoring interval in seconds
- `./bridgewatch start --verbose` - Enable detailed logging

### Status & Management
- `./bridgewatch status` - Check system health and statistics
- `./bridgewatch reset` - Reset all state files (keeps data)
- `./bridgewatch test-discord` - Test Discord webhook alerts

### Configuration
- `./bridgewatch config list` - List all available configurations
- `./bridgewatch config show` - Show active configuration details
- `./bridgewatch config switch <config-name>` - Switch to different configuration
- `./bridgewatch config validate` - Validate current configuration

## Simple Monitors

These standalone monitoring scripts can be run independently:

### Feed Staleness Monitor
```bash
python src/feed_stale_monitor.py [--once] [--verbose]
```
Monitors TellorDataBank contracts for stale data feeds and sends Discord alerts.

### Data Integrity Monitor
```bash
python src/data_integrity_monitor.py [--once] [--verbose]
```
Verifies data integrity between TellorDataBank contracts and Tellor Layer.

### Validator Set Update Monitor
```bash
python src/valset_update_monitor.py [--once] [--verbose]
```
Tracks validator set updates in TellorDataBridge contracts.

### Validator Set Integrity Monitor
```bash
python src/valset_integrity_monitor.py [--once] [--verbose]
```
Verifies validator set integrity between TellorDataBridge contracts and Tellor Layer.

### Common Options
- `--once` - Run once instead of continuously
- `--verbose` - Enable verbose debug logging
- `--no-discord` - Disable Discord alerts
- `--interval N` - Set monitoring interval in minutes

## Configuration

The system uses `config.json` for multi-bridge configurations. Each configuration has its own data directory and settings. Environment variables can be referenced using `${VAR_NAME}` syntax.

