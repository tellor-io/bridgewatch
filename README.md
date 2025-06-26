# bridgewatch - Bridge Data Monitor

Monitors and validates data relayed to the Tellor data bridge contract.

## Quick Start

```bash
# clone repo
git clone https://github.com/tellor-io/bridgewatch.git
cd bridgewatch

# create virtual environment
python3 -m venv venv
source venv/bin/activate

# install dependencies
pip install -r requirements.txt

# setup environment
python3 setup_env.py

# start monitoring
./bridgewatch start
```

## What It Does

- watches for validator set updates on EVM bridge
- monitors oracle data attestations  
- verifies data against Layer blockchain
- detects malicious behavior
- generates slashing evidence

## Commands

```bash
./bridgewatch start           # continuous monitoring
./bridgewatch start --once    # run once and exit
./bridgewatch status          # check status
./bridgewatch reset           # reset progress
```

## Configuration

Run `python3 setup_env.py` to configure your API keys.

You need:
- paid infura RPC url
- Layer RPC URL (default provided)

## Output

Data is saved to CSV files in `data/` directory:
- validator set updates
- attestations
- validation results
- slashing evidence

## Requirements

- Python 3.8+
- EVM RPC with `trace_filter` support (Infura recommended)
- Layer blockchain RPC access 