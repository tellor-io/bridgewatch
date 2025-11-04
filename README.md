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
python src/feed_stale_monitor.py [--once] [--verbose] --config <config-name>
```
Monitors TellorDataBank contracts for stale data feeds and sends Discord alerts.

### Data Integrity Monitor
```bash
python src/data_integrity_monitor.py [--once] [--verbose] --config <config-name>
```
Verifies data integrity between TellorDataBank contracts and Tellor Layer.

### Validator Set Update Monitor
```bash
python src/valset_update_monitor.py [--once] [--verbose] --config <config-name>
```
Tracks validator set updates in TellorDataBridge contracts.

### Validator Set Integrity Monitor
```bash
python src/valset_integrity_monitor.py [--once] [--verbose] --config <config-name>
```
Verifies validator set integrity between TellorDataBridge contracts and Tellor Layer.

### Frequency Monitor
```bash
python src/frequency_monitor.py [--report-now] [--days N] [--verbose] --config <config-name>
```
Generates periodic reports on data feed activity. Runs weekly reports on Tuesdays at 9am ET by default.

### Adaptor Guardian Monitor
```bash
python src/adaptor_guardian_monitor.py [--once] [--verbose] --config <config-name>
```
Monitors GuardedLiquityV2OracleAdaptor contracts for guardian management and pause/unpause events.

### Token Bridge Withdraw Monitor
```bash
python src/token_bridge_monitor.py [--once] [--verbose] [--start-block N] [--config NAME]
```
Validates TokenBridge `Withdraw` events against Tellor Layer aggregate reports.

### Common Options
- `--once` - Run once instead of continuously
- `--verbose` - Enable verbose debug logging
- `--no-discord` - Disable Discord alerts
- `--interval N` - Set monitoring interval in minutes
- `--ping-frequency N` - Set ping frequency in days (default: 7 for weekly)
- `--ping-now` - Send ping immediately regardless of schedule

## Configuration

The system uses `config.json` for multi-bridge configurations. Each configuration has its own data directory and settings. Environment variables can be referenced using `${VAR_NAME}` syntax.

### Multiple RPC endpoints (failover)

You can provide one or many RPC endpoints for both EVM and Layer. When multiple are provided, the monitors try them in order on each request and automatically fail over if one is unavailable. The primary (first) is always preferred.

Example:

```json
{
  "configs": {
    "my-bridge": {
      "evm_rpc_urls": [
        "https://primary-evm",
        "https://backup-evm-1",
        "https://backup-evm-2"
      ],
      "layer_rpc_urls": [
        "https://primary-layer",
        "https://backup-layer-1"
      ],
      "rpc_preference_reset_minutes": 60
    }
  }
}
```

Notes:
- You may still use single `evm_rpc_url` and `layer_rpc_url`; they remain fully supported.
- When `*_rpc_urls` arrays are present, they override the single URL fields for selection, but the single URLs are treated as the primary if both are present.
- The monitor sticks to the first working endpoint it finds and keeps using it for all calls. If that endpoint fails on a call, it retries from the beginning of the list to select the most preferred working endpoint and then sticks to that.
- Every `rpc_preference_reset_minutes` (default 60), the preference resets so the monitor re-evaluates from the top of the list and automatically returns to the primary if it's healthy.
- Both EVM contract calls and Layer REST API calls support failover independently.
