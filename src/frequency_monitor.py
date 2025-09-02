 #!/usr/bin/env python3
"""
Frequency Monitor

This component generates periodic reports on TellorDataBank data feed activity,
showing the number of relayed reports per queryId over specified time periods.

Features:
- Generates weekly reports by default (Tuesdays at 9am ET)
- Counts relayed reports per queryId from TellorDataBank contract
- Sends consolidated Discord reports for all feeds
- Supports custom time periods and immediate report generation
- Uses query_ids.json for feed configuration
- Simple and self-contained reporting
"""

import time
import requests
import logging
import json
import argparse
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from web3 import Web3
import pytz
from pathlib import Path
from config_manager import get_config_manager
from logger_utils import setup_logging

# logging will be configured in main() based on --verbose flag
logger = logging.getLogger(__name__)

class FrequencyMonitor:
    def __init__(self, disable_discord: bool = False, databank_contract_address: Optional[str] = None):
        """Initialize the frequency monitor
        
        Args:
            disable_discord: If True, skip sending Discord alerts
            databank_contract_address: Override TellorDataBank contract address
        """
        try:
            self.config_manager = get_config_manager()
            
            # get configuration
            self.databank_contract_address = databank_contract_address or self.config_manager.get_databank_contract()
            self.evm_rpc_url = self.config_manager.get_evm_rpc_url()
            self.layer_chain = self.config_manager.get_layer_chain()
            self.evm_chain = self.config_manager.get_evm_chain()
            
            # store settings
            self.disable_discord = disable_discord
            
            # initialize Web3
            self.w3 = Web3(Web3.HTTPProvider(self.evm_rpc_url))
            
            # test connection
            try:
                latest_block = self.w3.eth.block_number
                logger.debug(f"Connected to EVM RPC. Latest block: {latest_block}")
            except Exception as e:
                raise ConnectionError(f"Failed to connect to EVM RPC: {e}")
            
            # load databank contract
            self.databank_contract = self._load_databank_contract()
            
            # load query IDs configuration
            self.query_feeds = self._load_query_ids()
            
            # get Discord webhook URL
            self.discord_webhook_url = None if disable_discord else self._get_data_frequency_discord_webhook()
            
            # set up timezones
            self.utc_tz = pytz.timezone('UTC')
            self.eastern_tz = pytz.timezone('US/Eastern')
            
            logger.info(f"Initialized FrequencyMonitor for {self.layer_chain} → {self.evm_chain}")
            logger.info(f"DataBank contract: {self.databank_contract_address}")
            logger.info(f"Monitoring {len(self.query_feeds)} feeds")
            
            if self.disable_discord:
                logger.warning("Discord reports: DISABLED via --no-discord flag")
            else:
                logger.info(f"Discord reports: {'enabled' if self.discord_webhook_url else 'disabled (no webhook configured)'}")
            
        except Exception as e:
            logger.error(f"Failed to initialize FrequencyMonitor: {e}")
            raise
    
    def _load_databank_contract(self):
        """Load the TellorDataBank contract ABI and create Web3 contract instance"""
        try:
            abi_path = "abis/TellorDataBank.json"
            with open(abi_path, 'r') as f:
                contract_data = json.load(f)
            
            contract = self.w3.eth.contract(
                address=self.databank_contract_address,
                abi=contract_data['abi']
            )
            
            logger.debug(f"Loaded TellorDataBank contract from {abi_path}")
            return contract
            
        except Exception as e:
            logger.error(f"Failed to load TellorDataBank contract: {e}")
            raise
    
    def _load_query_ids(self) -> List[Dict[str, str]]:
        """Load query IDs from query_ids.json"""
        try:
            with open('query_ids.json', 'r') as f:
                data = json.load(f)
            
            feeds = data.get('feeds', [])
            logger.debug(f"Loaded {len(feeds)} query IDs from query_ids.json")
            return feeds
            
        except Exception as e:
            logger.error(f"Failed to load query IDs: {e}")
            raise
    
    def _get_data_frequency_discord_webhook(self) -> Optional[str]:
        """Get Discord webhook URL for data frequency reports"""
        try:
            # try to get specific data_frequency webhook first
            webhook_url = self.config_manager.get_discord_webhook('data_frequency')
            if webhook_url and webhook_url != "N/A":
                return webhook_url
            
            # fallback to general discord webhook
            return self.config_manager.get_discord_webhook_url()
            
        except Exception as e:
            logger.warning(f"Could not get Discord webhook URL: {e}")
            return None
    
    def _format_timestamp(self, timestamp_ms: int) -> str:
        """Format timestamp in milliseconds to human-readable string"""
        if timestamp_ms == 0:
            return "N/A"
        
        timestamp_s = timestamp_ms / 1000
        dt = datetime.fromtimestamp(timestamp_s, tz=self.utc_tz)
        utc_str = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        eastern_dt = dt.astimezone(self.eastern_tz)
        eastern_str = eastern_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
        
        return f"{utc_str} ({eastern_str})"
    
    def _is_report_time(self) -> bool:
        """Check if it's time to send the weekly report (Tuesday 9am ET)"""
        now_et = datetime.now(self.eastern_tz)
        
        # Check if it's Tuesday (weekday 1) and 9am ET
        if now_et.weekday() == 1 and now_et.hour == 9:
            return True
        
        return False
    
    def _get_time_period(self, days: int) -> tuple[int, int]:
        """Get start and end timestamps for the reporting period
        
        Args:
            days: Number of days to look back
            
        Returns:
            Tuple of (start_timestamp_ms, end_timestamp_ms)
        """
        now = datetime.now(self.utc_tz)
        end_time = now
        start_time = now - timedelta(days=days)
        
        start_timestamp_ms = int(start_time.timestamp() * 1000)
        end_timestamp_ms = int(end_time.timestamp() * 1000)
        
        return start_timestamp_ms, end_timestamp_ms
    
    def _count_reports_for_feed(self, query_id: str, start_timestamp_ms: int, end_timestamp_ms: int) -> int:
        """Count the number of reports for a specific queryId in the time period
        
        Args:
            query_id: The queryId to count reports for
            start_timestamp_ms: Start of time period in milliseconds
            end_timestamp_ms: End of time period in milliseconds
            
        Returns:
            Number of reports in the time period
        """
        try:
            # get total number of reports for this queryId
            total_count = self.databank_contract.functions.getAggregateValueCount(query_id).call()
            
            if total_count == 0:
                logger.debug(f"No reports found for {query_id}")
                return 0
            
            count_in_period = 0
            
            # iterate through all reports and count those in our time period
            for index in range(total_count):
                try:
                    aggregate_data = self.databank_contract.functions.getAggregateByIndex(query_id, index).call()
                    aggregate_timestamp = aggregate_data[2]  # aggregateTimestamp
                    
                    if aggregate_timestamp == 0:
                        continue
                    
                    # check if this report is in our time period
                    if start_timestamp_ms <= aggregate_timestamp <= end_timestamp_ms:
                        count_in_period += 1
                        
                except Exception as e:
                    logger.warning(f"Error checking index {index} for {query_id}: {e}")
                    continue
            
            logger.debug(f"Found {count_in_period} reports for {query_id} in time period")
            return count_in_period
            
        except Exception as e:
            logger.error(f"Failed to count reports for {query_id}: {e}")
            return 0
    
    def generate_frequency_report(self, days: int = 7) -> Dict[str, Any]:
        """Generate frequency report for all feeds
        
        Args:
            days: Number of days to look back (default: 7 for weekly)
            
        Returns:
            Dictionary with report data
        """
        try:
            start_timestamp_ms, end_timestamp_ms = self._get_time_period(days)
            
            logger.info(f"Generating {days}-day frequency report...")
            logger.info(f"Period: {self._format_timestamp(start_timestamp_ms)} to {self._format_timestamp(end_timestamp_ms)}")
            
            feed_counts = []
            total_reports = 0
            
            for feed in self.query_feeds:
                query_id = feed['queryId']
                feed_name = feed['name']
                
                logger.debug(f"Counting reports for {feed_name} ({query_id})")
                count = self._count_reports_for_feed(query_id, start_timestamp_ms, end_timestamp_ms)
                
                feed_counts.append({
                    'query_id': query_id,
                    'name': feed_name,
                    'description': feed.get('description', ''),
                    'count': count
                })
                
                total_reports += count
            
            # sort by count descending
            feed_counts.sort(key=lambda x: x['count'], reverse=True)
            
            report = {
                'period_days': days,
                'start_timestamp_ms': start_timestamp_ms,
                'end_timestamp_ms': end_timestamp_ms,
                'start_time_str': self._format_timestamp(start_timestamp_ms),
                'end_time_str': self._format_timestamp(end_timestamp_ms),
                'total_reports': total_reports,
                'feed_counts': feed_counts,
                'feeds_monitored': len(self.query_feeds),
                'generated_at': int(time.time() * 1000)
            }
            
            logger.info(f"Report generated: {total_reports} total reports across {len(self.query_feeds)} feeds")
            
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate frequency report: {e}")
            return None
    
    def send_discord_report(self, report: Dict[str, Any]):
        """Send frequency report to Discord"""
        if self.disable_discord or not self.discord_webhook_url:
            return
        
        try:
            period_days = report['period_days']
            start_time = report['start_time_str']
            end_time = report['end_time_str']
            total_reports = report['total_reports']
            feed_counts = report['feed_counts']
            
            # create period description
            if period_days == 7:
                period_desc = "Weekly Report"
            elif period_days == 1:
                period_desc = "Daily Report"
            else:
                period_desc = f"{period_days}-Day Report"
            
            # build feed summary
            feed_summary = ""
            for feed in feed_counts:
                name = feed['name']
                count = feed['count']
                feed_summary += f"**{name}:** {count} reports\n"
            
            # create report message
            report_message = (
                f"**Data Feed Frequency {period_desc}** - {self.layer_chain} → {self.evm_chain}\n\n"
                f"**Period:** {start_time.split('(')[0].strip()}\n"
                f"**To:** {end_time.split('(')[0].strip()}\n"
                f"**Total Reports:** {total_reports}\n"
                f"**Feeds Monitored:** {len(feed_counts)}\n\n"
                f"**Reports per Feed:**\n{feed_summary}\n"
                f"**DataBank Contract:** `{self.databank_contract_address}`"
            )
            
            payload = {
                "content": report_message,
                "username": "Frequency Monitor"
            }
            
            response = requests.post(self.discord_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info(f"Sent Discord frequency report ({period_days}-day period)")
            
        except Exception as e:
            logger.error(f"Failed to send Discord report: {e}")
    
    def run_once(self, days: int = 7, force_report: bool = False) -> Optional[Dict[str, Any]]:
        """Run frequency check once and optionally send report
        
        Args:
            days: Number of days for the report period
            force_report: If True, generate and send report immediately
        """
        logger.info("Checking if frequency report should be generated...")
        
        # check if we should send a report
        should_send_report = force_report or self._is_report_time()
        
        if should_send_report:
            logger.info(f"Generating frequency report for {days}-day period...")
            
            report = self.generate_frequency_report(days)
            
            if report:
                logger.info(f"Report generated successfully - {report['total_reports']} total reports")
                self.send_discord_report(report)
                return report
            else:
                logger.error("Failed to generate frequency report")
                return None
        else:
            logger.debug("Not time for scheduled report, skipping")
            return None
    
    def run_continuous(self, interval_minutes: int = 60, days: int = 7):
        """Run continuous monitoring with specified interval
        
        Args:
            interval_minutes: How often to check if it's report time
            days: Number of days for the report period
        """
        interval_seconds = interval_minutes * 60
        
        logger.info(f"Starting continuous frequency monitoring (interval: {interval_minutes}m, report period: {days}d)")
        logger.info("Weekly reports scheduled for Tuesdays at 9am ET")
        
        try:
            while True:
                try:
                    self.run_once(days=days, force_report=False)
                except Exception as e:
                    logger.error(f"Error during monitoring cycle: {e}")
                
                logger.debug(f"Sleeping for {interval_minutes} minutes...")
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Monitoring stopped due to error: {e}")
            raise

def main():
    """Main entry point for the frequency monitor"""
    parser = argparse.ArgumentParser(description='Generate frequency reports for TellorDataBank data feeds')
    parser.add_argument('--once', action='store_true', help='Run once instead of continuously')
    parser.add_argument('--no-discord', action='store_true', help='Disable Discord reports')
    parser.add_argument('--databank-address', type=str, help='Override TellorDataBank contract address')
    parser.add_argument('--interval', type=int, default=60, help='Monitoring interval in minutes (default: 60)')
    parser.add_argument('--days', type=int, default=7, help='Report period in days (default: 7)')
    parser.add_argument('--report-now', action='store_true', help='Generate and send report immediately')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose debug logging')
    parser.add_argument('--config', type=str, help='Specify which configuration profile to use (overrides ACTIVE_CONFIG)')
    
    args = parser.parse_args()
    
    # set config override if --config flag was provided
    if args.config:
        from config import set_global_config_override
        set_global_config_override(args.config)
    
    # setup logging based on verbose flag
    setup_logging(verbose=args.verbose)
    
    # log which config we're using if override was provided
    if args.config:
        logger.info(f"🔧 Using configuration: {args.config}")
    
    try:
        # initialize monitor
        monitor = FrequencyMonitor(
            disable_discord=args.no_discord,
            databank_contract_address=args.databank_address
        )
        
        if args.once or args.report_now:
            # run once or generate immediate report
            report = monitor.run_once(days=args.days, force_report=args.report_now)
            if report:
                # print summary to console
                print(f"\nFrequency Report ({args.days}-day period):")
                print(f"Period: {report['start_time_str']} to {report['end_time_str']}")
                print(f"Total Reports: {report['total_reports']}")
                print("\nReports per Feed:")
                for feed in report['feed_counts']:
                    print(f"  {feed['name']}: {feed['count']} reports")
        else:
            # run continuously
            monitor.run_continuous(interval_minutes=args.interval, days=args.days)
            
    except Exception as e:
        logger.error(f"Monitor failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()