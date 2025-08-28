#!/usr/bin/env python3
"""
Ping Helper

Shared utility for monitoring script ping functionality. Provides scheduling,
timing, and Discord ping capabilities for all monitoring scripts.

Features:
- Configurable ping frequencies (weekly, daily, etc.)
- Tuesday 9am ET basis timing for all frequencies
- Manual ping triggers
- State persistence for ping tracking
- Eastern timezone handling
"""

import time
import requests
import logging
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import pytz
from pathlib import Path

logger = logging.getLogger(__name__)

class PingHelper:
    def __init__(self, script_name: str, data_dir: str, discord_webhook_url: Optional[str] = None):
        """Initialize ping helper for a monitoring script
        
        Args:
            script_name: Name of the monitoring script (used for state file)
            data_dir: Data directory for state file storage
            discord_webhook_url: Discord webhook URL for pings
        """
        self.script_name = script_name
        self.discord_webhook_url = discord_webhook_url
        
        # set up timezone
        self.eastern_tz = pytz.timezone('US/Eastern')
        self.utc_tz = pytz.timezone('UTC')
        
        # ping state management
        ping_dir = Path(data_dir) / "ping_state"
        ping_dir.mkdir(parents=True, exist_ok=True)
        self.ping_state_file = ping_dir / f"{script_name}_ping_state.json"
        
        logger.debug(f"Initialized PingHelper for {script_name}")
    
    def _get_tuesday_9am_basis(self) -> datetime:
        """Get the most recent Tuesday at 9am ET as basis for scheduling"""
        now_et = datetime.now(self.eastern_tz)
        
        # find the most recent Tuesday (including today if it's Tuesday and before 9am)
        days_since_tuesday = now_et.weekday() - 1  # Monday=0, Tuesday=1, etc.
        if days_since_tuesday < 0:
            days_since_tuesday += 7
        
        # if it's Tuesday and after 9am, use last Tuesday
        if now_et.weekday() == 1 and now_et.hour >= 9:
            days_since_tuesday = 7
        
        tuesday_date = now_et.date() - timedelta(days=days_since_tuesday)
        tuesday_9am = self.eastern_tz.localize(
            datetime.combine(tuesday_date, datetime.min.time().replace(hour=9))
        )
        
        return tuesday_9am
    
    def _get_next_ping_time(self, frequency_days: int) -> datetime:
        """Calculate next ping time based on frequency and Tuesday 9am basis
        
        Args:
            frequency_days: Ping frequency in days (7=weekly, 1=daily, etc.)
        """
        basis = self._get_tuesday_9am_basis()
        now_et = datetime.now(self.eastern_tz)
        
        # calculate how many intervals have passed since basis
        time_diff = now_et - basis
        intervals_passed = int(time_diff.total_seconds() / (frequency_days * 24 * 3600))
        
        # next ping is after the current interval
        next_ping = basis + timedelta(days=(intervals_passed + 1) * frequency_days)
        
        return next_ping
    
    def _load_ping_state(self) -> Dict[str, Any]:
        """Load ping state from file"""
        try:
            if self.ping_state_file.exists():
                with open(self.ping_state_file, 'r') as f:
                    state = json.load(f)
                logger.debug(f"Loaded ping state for {self.script_name}")
                return state
            else:
                logger.debug(f"No ping state file found for {self.script_name}")
                return {}
        except Exception as e:
            logger.warning(f"Failed to load ping state for {self.script_name}: {e}")
            return {}
    
    def _save_ping_state(self, state: Dict[str, Any]):
        """Save ping state to file"""
        try:
            with open(self.ping_state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.debug(f"Saved ping state for {self.script_name}")
        except Exception as e:
            logger.error(f"Failed to save ping state for {self.script_name}: {e}")
    
    def should_send_ping(self, frequency_days: int = 7) -> bool:
        """Check if it's time to send a ping based on frequency
        
        Args:
            frequency_days: Ping frequency in days (7=weekly, 1=daily, etc.)
        """
        if not self.discord_webhook_url:
            return False
        
        state = self._load_ping_state()
        last_ping_timestamp = state.get('last_ping_timestamp', 0)
        
        # convert to ET timezone for comparison
        now_et = datetime.now(self.eastern_tz)
        
        if last_ping_timestamp == 0:
            # first time - check if it's past 9am ET today
            if now_et.hour >= 9:
                logger.debug(f"First ping for {self.script_name} - sending now")
                return True
            else:
                logger.debug(f"First ping for {self.script_name} - waiting for 9am ET")
                return False
        
        # calculate next expected ping time
        last_ping_dt = datetime.fromtimestamp(last_ping_timestamp, tz=self.eastern_tz)
        next_ping_time = self._get_next_ping_time(frequency_days)
        
        # check if we've passed the next ping time
        if now_et >= next_ping_time:
            logger.debug(f"Time for scheduled ping: {next_ping_time} (now: {now_et})")
            return True
        
        logger.debug(f"Next ping scheduled for: {next_ping_time} (now: {now_et})")
        return False
    
    def send_ping(self, ping_content: str, frequency_days: int = 7, force: bool = False):
        """Send ping to Discord
        
        Args:
            ping_content: The content of the ping message
            frequency_days: Ping frequency for logging
            force: If True, send ping regardless of schedule
        """
        if not self.discord_webhook_url:
            logger.warning(f"No Discord webhook configured for {self.script_name} ping")
            return False
        
        try:
            # create frequency description
            if frequency_days == 7:
                freq_desc = "Weekly"
            elif frequency_days == 1:
                freq_desc = "Daily"
            else:
                freq_desc = f"{frequency_days}-Day"
            
            # format timestamp
            now_et = datetime.now(self.eastern_tz)
            time_str = now_et.strftime('%Y-%m-%d %H:%M:%S %Z')
            
            # create ping message
            ping_message = (
                f"📍 **{freq_desc} Ping** - {self.script_name.replace('_', ' ').title()}\n"
                f"**Time:** {time_str}\n\n"
                f"{ping_content}"
            )
            
            payload = {
                "content": ping_message,
                "username": f"{self.script_name.replace('_', ' ').title()} Ping"
            }
            
            response = requests.post(self.discord_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            # update ping state
            state = self._load_ping_state()
            state['last_ping_timestamp'] = time.time()
            state['last_ping_frequency'] = frequency_days
            self._save_ping_state(state)
            
            logger.info(f"Sent {freq_desc.lower()} ping for {self.script_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send ping for {self.script_name}: {e}")
            return False
    
    def format_timestamp_et(self, timestamp_ms: int) -> str:
        """Format timestamp in milliseconds to human-readable ET string"""
        if timestamp_ms == 0:
            return "N/A"
        
        timestamp_s = timestamp_ms / 1000
        dt_utc = datetime.fromtimestamp(timestamp_s, tz=self.utc_tz)
        dt_et = dt_utc.astimezone(self.eastern_tz)
        
        return dt_et.strftime('%Y-%m-%d %H:%M:%S %Z')
    
    def get_ping_status(self, frequency_days: int = 7) -> Dict[str, Any]:
        """Get current ping status information"""
        state = self._load_ping_state()
        last_ping_timestamp = state.get('last_ping_timestamp', 0)
        
        if last_ping_timestamp > 0:
            last_ping_dt = datetime.fromtimestamp(last_ping_timestamp, tz=self.eastern_tz)
            last_ping_str = last_ping_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
            next_ping_time = self._get_next_ping_time(frequency_days)
            next_ping_str = next_ping_time.strftime('%Y-%m-%d %H:%M:%S %Z')
        else:
            last_ping_str = "Never"
            next_ping_str = "Next 9am ET"
        
        return {
            'last_ping': last_ping_str,
            'next_ping': next_ping_str,
            'should_ping_now': self.should_send_ping(frequency_days),
            'webhook_configured': self.discord_webhook_url is not None
        } 