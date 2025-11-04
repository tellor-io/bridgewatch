#!/usr/bin/env python3
"""
Token Bridge Withdraw Monitor

Watches `Withdraw` events emitted by the TokenBridge contract and verifies that
the event payload matches the latest aggregate report recorded on Tellor Layer.

If any discrepancies are detected between on-chain events and Layer data, the
monitor will emit Discord alerts (unless disabled) with contextual details so
operators can investigate immediately.
"""

import argparse
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from eth_abi import decode, encode
from web3 import Web3
from web3._utils.events import get_event_data

from config_manager import get_config_manager
from logger_utils import setup_logging
from ping_helper import PingHelper
from rpc_failover import EVMProviderPool, HttpEndpointPool


logger = logging.getLogger(__name__)


TOKEN_PRECISION_MULTIPLIER = 10 ** 12  # matches TokenBridge.TOKEN_DECIMAL_PRECISION_MULTIPLIER
DEFAULT_INITIAL_LOOKBACK_BLOCKS = 5_000
DEFAULT_MAX_ALERT_HISTORY = 200
RECENT_WINDOW_SECONDS = 7 * 24 * 60 * 60


def strip_0x(value: str) -> str:
    if value.startswith("0x"):
        return value[2:]
    return value


@dataclass
class WithdrawEvent:
    deposit_id: int
    layer_sender: str
    recipient: str
    amount_wei: int
    tx_hash: str
    log_index: int
    block_number: int
    block_timestamp: Optional[int]


class TokenBridgeWithdrawMonitor:
    """Monitor TokenBridge withdraw events against Tellor Layer aggregate data."""

    def __init__(
        self,
        disable_discord: bool = False,
        start_block: Optional[int] = None,
        max_block_range: Optional[int] = None,
        block_batch_size_override: Optional[int] = None,
        initial_lookback_blocks: int = DEFAULT_INITIAL_LOOKBACK_BLOCKS,
        ping_frequency_days: int = 7,
    ):
        try:
            self.config_manager = get_config_manager()
        except RuntimeError as exc:
            logger.error(f"Failed to load configuration: {exc}")
            raise

        self.disable_discord = disable_discord
        self.start_block_override = start_block
        self.max_block_range = max_block_range
        self.initial_lookback_blocks = max(1, initial_lookback_blocks)
        self.ping_frequency_days = max(1, int(ping_frequency_days))

        # Load configuration values
        self.layer_chain = self.config_manager.get_layer_chain()
        self.evm_chain = self.config_manager.get_evm_chain()
        token_bridge_address = self.config_manager.get_token_bridge_contract()
        if not token_bridge_address:
            raise ValueError(
                "token_bridge_contract not configured. Set TOKEN_BRIDGE_ADDRESS env var "
                "or `token_bridge_contract` in config.json."
            )
        self.token_bridge_address = Web3.to_checksum_address(token_bridge_address)

        self.evm_rpc_urls = self.config_manager.get_evm_rpc_urls()
        self.layer_rpc_urls = self.config_manager.get_layer_rpc_urls()
        reset_minutes = self.config_manager.get_rpc_preference_reset_minutes()

        self.block_batch_size = (
            block_batch_size_override
            if block_batch_size_override is not None
            else self.config_manager.get_block_batch_size()
        )
        self.block_batch_size = max(10, int(self.block_batch_size))
        self.block_buffer = max(0, int(self.config_manager.get_block_buffer()))

        # Provider pools
        self.evm_pool = EVMProviderPool(
            self.evm_rpc_urls, request_timeout_s=15, preference_reset_minutes=reset_minutes
        )
        self.layer_pool = HttpEndpointPool(
            self.layer_rpc_urls, timeout_s=10, preference_reset_minutes=reset_minutes
        )

        # Preload ABI and event metadata
        self.token_bridge_abi = self._load_token_bridge_abi()
        self.withdraw_topic = Web3.keccak(text="Withdraw(uint256,string,address,uint256)").hex()

        # Data directories & state handling
        data_dir = Path(self.config_manager.get_data_dir())
        self.token_bridge_dir = data_dir / "token_bridge"
        self.token_bridge_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.token_bridge_dir / "withdraw_monitor_state.json"
        self.state = self._load_state()
        self.alert_history = set(self.state.get("alerted_events", []))
        now_ts = int(time.time())
        self.recent_withdrawals_map = self._load_recent_withdrawals_map(now_ts)
        self.processed_withdraw_ids = {
            int(withdraw_id) for withdraw_id in self.state.get("processed_withdraw_ids", [])
        }
        self.total_withdrawals_seen = max(
            len(self.processed_withdraw_ids),
            int(self.state.get("total_withdrawals_seen", len(self.processed_withdraw_ids))),
        )
        self._prune_recent_withdrawals_map(now_ts)

        # Discord configuration
        self.discord_webhook_url = None if disable_discord else self._get_discord_webhook()
        self.ping_helper = None
        if not disable_discord:
            try:
                self.ping_helper = PingHelper(
                    script_name="token_bridge_monitor",
                    data_dir=str(data_dir),
                    discord_webhook_url=self.discord_webhook_url,
                )
            except Exception as exc:
                logger.warning(f"Failed to initialize PingHelper: {exc}")

        logger.info(
            "Initialized TokenBridgeWithdrawMonitor for %s → %s",
            self.layer_chain,
            self.evm_chain,
        )
        logger.info("TokenBridge address: %s", self.token_bridge_address)
        logger.info("Block batch size: %s", self.block_batch_size)
        logger.info("Block buffer: %s", self.block_buffer)
        if self.max_block_range:
            logger.info("Max block range per cycle: %s", self.max_block_range)
        if self.disable_discord:
            logger.warning("Discord alerts disabled via --no-discord")
        elif not self.discord_webhook_url:
            logger.warning("Discord webhook not configured; alerts will be skipped")

    # ------------------------------------------------------------------
    # Initialization helpers
    # ------------------------------------------------------------------

    def _load_token_bridge_abi(self) -> List[Dict[str, Any]]:
        try:
            with open(Path("abis") / "TokenBridge.json", "r", encoding="utf-8") as abi_file:
                abi_data = json.load(abi_file)
            return abi_data["abi"]
        except Exception as exc:
            raise RuntimeError(f"Failed to load TokenBridge ABI: {exc}")

    def _get_discord_webhook(self) -> Optional[str]:
        try:
            webhook = self.config_manager.get_discord_webhook("malicious_activity")
            if webhook and webhook != "N/A":
                return webhook
            return self.config_manager.get_discord_webhook_url()
        except Exception as exc:
            logger.warning(f"Could not load Discord webhook URL: {exc}")
            return None

    # ------------------------------------------------------------------
    # State helpers
    # ------------------------------------------------------------------

    def _load_state(self) -> Dict[str, Any]:
        if not self.state_file.exists():
            return {}
        try:
            with open(self.state_file, "r", encoding="utf-8") as state_fp:
                return json.load(state_fp)
        except Exception as exc:
            logger.warning(f"Failed to load state file %s: %s", self.state_file, exc)
            return {}

    def _save_state(self, state: Dict[str, Any]) -> None:
        try:
            with open(self.state_file, "w", encoding="utf-8") as state_fp:
                json.dump(state, state_fp, indent=2)
        except Exception as exc:
            logger.error(f"Failed to save state to %s: %s", self.state_file, exc)

    def _load_recent_withdrawals_map(self, now_ts: int) -> Dict[int, int]:
        entries = self.state.get("recent_withdrawals", [])
        result: Dict[int, int] = {}
        for entry in entries:
            try:
                withdraw_id = int(entry.get("id"))
                timestamp = int(entry.get("timestamp"))
            except Exception:
                continue
            if now_ts - timestamp <= RECENT_WINDOW_SECONDS:
                result[withdraw_id] = timestamp
        return result

    def _prune_recent_withdrawals_map(self, now_ts: Optional[int] = None) -> None:
        if now_ts is None:
            now_ts = int(time.time())
        cutoff = now_ts - RECENT_WINDOW_SECONDS
        self.recent_withdrawals_map = {
            withdraw_id: ts
            for withdraw_id, ts in self.recent_withdrawals_map.items()
            if ts >= cutoff
        }

    def _record_withdrawal(self, event: WithdrawEvent) -> None:
        review_timestamp = int(time.time())

        self.recent_withdrawals_map[event.deposit_id] = review_timestamp
        self._prune_recent_withdrawals_map(review_timestamp)

        if event.deposit_id not in self.processed_withdraw_ids:
            self.processed_withdraw_ids.add(event.deposit_id)
        self.total_withdrawals_seen = len(self.processed_withdraw_ids)

    def _maybe_send_ping(self, force: bool = False) -> None:
        if not self.ping_helper:
            return
        try:
            if force or self.ping_helper.should_send_ping(self.ping_frequency_days):
                content = self.generate_ping_content()
                self.ping_helper.send_ping(
                    content,
                    self.ping_frequency_days,
                    force=force,
                )
        except Exception as exc:
            logger.warning(f"Failed to send ping: {exc}")

    def generate_ping_content(self) -> str:
        self._prune_recent_withdrawals_map()

        last_block = self.state.get("last_processed_block")
        last_block_display = str(last_block) if last_block is not None else "N/A"

        recent_ids = sorted(self.recent_withdrawals_map.keys())
        recent_ids_str = ", ".join(str(withdraw_id) for withdraw_id in recent_ids) if recent_ids else "None"

        total_reviewed = self.total_withdrawals_seen

        return (
            f"**Latest EVM block reviewed:** {last_block_display}\n"
            f"**Withdraw IDs reviewed (last 7d):** {recent_ids_str}\n"
            f"**Total withdraw IDs reviewed:** {total_reviewed}"
        )

    # ------------------------------------------------------------------
    # Monitoring entry points
    # ------------------------------------------------------------------

    def run_continuous(self, interval_seconds: int) -> None:
        logger.info("Starting continuous monitoring with interval %ss", interval_seconds)

        self._maybe_send_ping()

        while True:
            try:
                stats = self.run_monitoring_cycle()
                logger.info(
                    "Cycle complete | withdraw events: %s | discrepancies: %s | last block: %s",
                    stats["withdraw_events"],
                    stats["discrepancies"],
                    stats["last_processed_block"],
                )
                self._maybe_send_ping()
            except KeyboardInterrupt:
                logger.info("Received interrupt; stopping monitor")
                break
            except Exception as exc:
                logger.exception(f"Monitoring cycle failed: {exc}")

            logger.info("Sleeping %ss before next cycle", interval_seconds)
            for _ in range(interval_seconds):
                time.sleep(1)

    def run_monitoring_cycle(self) -> Dict[str, Any]:
        try:
            latest_block = self.evm_pool.with_web3(lambda web3: web3.eth.block_number)
        except Exception as exc:
            raise RuntimeError(f"Failed to connect to EVM RPCs: {exc}")
        if latest_block is None:
            raise RuntimeError("Could not determine latest block number")

        safe_target_block = max(0, latest_block - self.block_buffer) if self.block_buffer else latest_block
        if safe_target_block < 0:
            safe_target_block = 0

        last_processed_block = self.state.get("last_processed_block")

        if last_processed_block is None:
            if self.start_block_override is not None:
                from_block = max(0, self.start_block_override)
            else:
                from_block = max(0, safe_target_block - self.initial_lookback_blocks + 1)
        else:
            from_block = last_processed_block - self.block_buffer if self.block_buffer else last_processed_block + 1
            from_block = max(0, from_block)
            if self.start_block_override is not None:
                from_block = max(from_block, self.start_block_override)

        if safe_target_block < from_block:
            logger.debug(
                "No new blocks to process (from_block=%s, target=%s)", from_block, safe_target_block
            )
            return {
                "withdraw_events": 0,
                "discrepancies": 0,
                "last_processed_block": last_processed_block,
            }

        if self.max_block_range is not None:
            range_size = safe_target_block - from_block + 1
            if range_size > self.max_block_range:
                from_block = safe_target_block - self.max_block_range + 1
                logger.info(
                    "Constraining processing window to last %s blocks (new from_block=%s)",
                    self.max_block_range,
                    from_block,
                )

        withdraw_events = 0
        discrepancies = 0

        last_successful_block = (
            last_processed_block if last_processed_block is not None else from_block - 1
        )
        current_from = from_block
        while current_from <= safe_target_block:
            current_to = min(current_from + self.block_batch_size - 1, safe_target_block)
            logger.info("Scanning blocks %s → %s", current_from, current_to)
            try:
                events_in_batch, mismatches = self._process_block_range(current_from, current_to)
            except Exception as exc:
                logger.exception(f"Failed processing block range {current_from}-{current_to}: {exc}")
                break

            withdraw_events += events_in_batch
            discrepancies += mismatches
            last_successful_block = current_to
            current_from = current_to + 1

        # Update state with safe target block and alert history (bounded size)
        trimmed_history = sorted(self.alert_history)
        if len(trimmed_history) > DEFAULT_MAX_ALERT_HISTORY:
            trimmed_history = trimmed_history[-DEFAULT_MAX_ALERT_HISTORY:]

        persisted_block = max(-1, last_successful_block)

        recent_withdrawals_serialized = [
            {"id": int(withdraw_id), "timestamp": int(ts)}
            for withdraw_id, ts in sorted(
                self.recent_withdrawals_map.items(), key=lambda item: item[1]
            )
        ]
        processed_ids_serialized = sorted(int(withdraw_id) for withdraw_id in self.processed_withdraw_ids)

        new_state = {
            "last_processed_block": persisted_block,
            "last_run": datetime.now(timezone.utc).isoformat(),
            "alerted_events": trimmed_history,
            "recent_withdrawals": recent_withdrawals_serialized,
            "processed_withdraw_ids": processed_ids_serialized,
            "total_withdrawals_seen": self.total_withdrawals_seen,
        }
        self._save_state(new_state)
        self.state = new_state

        return {
            "withdraw_events": withdraw_events,
            "discrepancies": discrepancies,
            "last_processed_block": persisted_block if persisted_block >= 0 else None,
        }

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------

    def _process_block_range(
        self, from_block: int, to_block: int
    ) -> Tuple[int, int]:
        if from_block > to_block:
            return 0, 0

        filter_params = {
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": self.token_bridge_address,
            "topics": [self.withdraw_topic],
        }

        try:
            logs, withdraw_event = self.evm_pool.with_web3(
                lambda web3: (
                    web3.eth.get_logs(filter_params),
                    web3.eth.contract(address=self.token_bridge_address, abi=self.token_bridge_abi).events.Withdraw(),
                )
            )
        except Exception as exc:
            logger.error(
                "Failed to fetch logs for blocks %s-%s from any EVM RPC: %s",
                from_block,
                to_block,
                exc,
            )
            return 0, 0

        if not logs:
            return 0, 0

        processed = 0
        mismatches = 0

        for raw_log in logs:
            try:
                decoded = withdraw_event.process_log(raw_log)
            except Exception as exc:
                logger.warning(f"Failed to decode withdraw log: {exc}")
                continue

            event = self._build_withdraw_event(decoded)

            if event is None:
                continue

            processed += 1
            self._record_withdrawal(event)

            issues, layer_payload = self._verify_event_against_layer(event)

            if issues:
                mismatches += 1
                self._handle_discrepancy(event, layer_payload, issues)
            else:
                logger.info(
                    "✅ Verified withdraw | depositId=%s | tx=%s | amount=%s",
                    event.deposit_id,
                    event.tx_hash,
                    event.amount_wei,
                )

        return processed, mismatches

    def _build_withdraw_event(self, decoded_event: Any) -> Optional[WithdrawEvent]:
        args = decoded_event.get("args", {})
        try:
            deposit_id = int(args.get("_depositId"))
            layer_sender = args.get("_sender")
            recipient = Web3.to_checksum_address(args.get("_recipient"))
            amount_wei = int(args.get("_amount"))
            tx_hash = decoded_event.get("transactionHash").hex()
            log_index = int(decoded_event.get("logIndex"))
            block_number = int(decoded_event.get("blockNumber"))
        except Exception as exc:
            logger.warning(f"Malformed withdraw event; skipping. Error: {exc}")
            return None

        try:
            block = self.evm_pool.with_web3(lambda web3: web3.eth.get_block(block_number))
            block_timestamp = block.timestamp if block is not None else None
        except Exception:
            block_timestamp = None

        return WithdrawEvent(
            deposit_id=deposit_id,
            layer_sender=layer_sender,
            recipient=recipient,
            amount_wei=amount_wei,
            tx_hash=tx_hash,
            log_index=log_index,
            block_number=block_number,
            block_timestamp=block_timestamp,
        )

    def _verify_event_against_layer(
        self, event: WithdrawEvent
    ) -> Tuple[List[str], Optional[Dict[str, Any]]]:
        query_id = self._build_withdraw_query_id(event.deposit_id)

        try:
            response = self.layer_pool.get_json(
                f"/tellor-io/layer/oracle/get_current_aggregate_report/{strip_0x(query_id)}"
            )
        except Exception as exc:
            logger.error(
                "Layer API error for depositId=%s: %s",
                event.deposit_id,
                exc,
            )
            return [f"Layer API error: {exc}"], None

        aggregate = response.get("aggregate") if isinstance(response, dict) else None
        issues: List[str] = []

        if not aggregate:
            issues.append("Layer returned no aggregate data")
            return issues, None

        layer_payload: Dict[str, Any] = {
            "aggregate": aggregate,
            "timestamp": response.get("timestamp"),
        }

        layer_query_id = aggregate.get("query_id")
        if layer_query_id and strip_0x(layer_query_id).lower() != strip_0x(query_id).lower():
            issues.append(
                f"Layer queryId mismatch (layer={layer_query_id}, expected={query_id})"
            )

        aggregate_value = aggregate.get("aggregate_value")
        if not aggregate_value:
            issues.append("Aggregate value missing on Layer")
            return issues, layer_payload

        try:
            decoded_value = decode(
                ["address", "string", "uint256", "uint256"],
                bytes.fromhex(strip_0x(str(aggregate_value))),
            )
        except Exception as exc:
            issues.append(f"Failed to decode aggregate_value: {exc}")
            return issues, layer_payload

        layer_recipient = Web3.to_checksum_address(decoded_value[0])
        layer_layer_sender = decoded_value[1]
        layer_amount_loya = int(decoded_value[2])
        layer_tip_amount = int(decoded_value[3])

        if layer_recipient != event.recipient:
            issues.append(
                f"Recipient mismatch (event={event.recipient}, layer={layer_recipient})"
            )

        if layer_layer_sender != event.layer_sender:
            issues.append(
                f"Layer sender mismatch (event={event.layer_sender}, layer={layer_layer_sender})"
            )

        converted_amount = layer_amount_loya * TOKEN_PRECISION_MULTIPLIER
        if converted_amount != event.amount_wei:
            issues.append(
                "Amount mismatch (event={} wei, layer={} loya)".format(
                    event.amount_wei, layer_amount_loya
                )
            )

        aggregate_reporter = aggregate.get("aggregate_reporter")
        if aggregate_reporter and aggregate_reporter != event.layer_sender:
            issues.append(
                f"Reporter mismatch (aggregate_reporter={aggregate_reporter}, expected={event.layer_sender})"
            )

        # enrich payload with decoded values for alert context
        layer_payload["decoded_value"] = {
            "recipient": layer_recipient,
            "layer_sender": layer_layer_sender,
            "amount_loya": layer_amount_loya,
            "tip_amount": layer_tip_amount,
        }

        return issues, layer_payload

    def _handle_discrepancy(
        self,
        event: WithdrawEvent,
        layer_payload: Optional[Dict[str, Any]],
        issues: List[str],
    ) -> None:
        alert_key = f"{event.tx_hash}:{event.log_index}"
        if alert_key in self.alert_history:
            logger.warning(
                "Discrepancy re-detected (already alerted) | depositId=%s | tx=%s | issues=%s",
                event.deposit_id,
                event.tx_hash,
                "; ".join(issues),
            )
            return

        logger.error(
            "🚨 Withdraw mismatch detected | depositId=%s | tx=%s | issues=%s",
            event.deposit_id,
            event.tx_hash,
            "; ".join(issues),
        )

        self.alert_history.add(alert_key)

        if self.disable_discord or not self.discord_webhook_url:
            return

        embed_fields = [
            {
                "name": "Issues",
                "value": "\n".join(f"• {issue}" for issue in issues)[:1024],
                "inline": False,
            },
            {
                "name": "Event",
                "value": (
                    f"Deposit ID: `{event.deposit_id}`\n"
                    f"Sender: `{event.layer_sender}`\n"
                    f"Recipient: `{event.recipient}`\n"
                    f"Amount (wei): `{event.amount_wei}`"
                )[:1024],
                "inline": True,
            },
        ]

        if layer_payload:
            decoded = layer_payload.get("decoded_value", {})
            embed_fields.append(
                {
                    "name": "Layer Aggregate",
                    "value": (
                        f"Recipient: `{decoded.get('recipient', 'n/a')}`\n"
                        f"Layer Sender: `{decoded.get('layer_sender', 'n/a')}`\n"
                        f"Amount (loya): `{decoded.get('amount_loya', 'n/a')}`\n"
                        f"Tip (loya): `{decoded.get('tip_amount', 'n/a')}`\n"
                        f"Reporter: `{layer_payload.get('aggregate', {}).get('aggregate_reporter', 'n/a')}`\n"
                        f"Timestamp: `{layer_payload.get('timestamp', 'n/a')}`"
                    )[:1024],
                    "inline": True,
                }
            )

        embed = {
            "title": "🚨 Token Bridge Withdraw Mismatch",
            "color": 0xFF0000,
            "timestamp": datetime.utcnow().isoformat(),
            "fields": embed_fields,
            "footer": {
                "text": f"tx: {event.tx_hash} (log {event.log_index}) | block: {event.block_number}",
            },
        }

        payload = {
            "content": None,
            "embeds": [embed],
        }

        try:
            response = requests.post(self.discord_webhook_url, json=payload, timeout=10)
            if response.status_code >= 400:
                logger.error(
                    "Failed to send Discord alert (status %s): %s",
                    response.status_code,
                    response.text,
                )
        except Exception as exc:
            logger.error(f"Failed to send Discord alert: {exc}")

    # ------------------------------------------------------------------
    # Utility methods
    # ------------------------------------------------------------------

    @staticmethod
    def _build_withdraw_query_id(deposit_id: int) -> str:
        query_data_args = encode(["bool", "uint256"], [False, deposit_id])
        query_data = encode(["string", "bytes"], ["TRBBridge", query_data_args])
        return Web3.keccak(query_data).hex()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor TokenBridge withdraw events")
    parser.add_argument("--once", action="store_true", help="Run a single monitoring cycle")
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Interval (in seconds) between monitoring cycles in continuous mode",
    )
    parser.add_argument("--start-block", type=int, help="Override starting block for first run")
    parser.add_argument(
        "--max-block-range",
        type=int,
        help="Limit number of blocks processed per cycle (prevents huge catch-up runs)",
    )
    parser.add_argument(
        "--block-batch-size",
        type=int,
        help="Override block batch size (defaults to config monitoring.block_batch_size)",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("--no-color", action="store_true", help="Disable colored logs")
    parser.add_argument("--no-discord", action="store_true", help="Disable Discord alerts")
    parser.add_argument(
        "--config",
        type=str,
        help="Configuration profile to use (overrides ACTIVE_CONFIG)",
    )
    parser.add_argument(
        "--initial-lookback-blocks",
        type=int,
        default=DEFAULT_INITIAL_LOOKBACK_BLOCKS,
        help="Number of blocks to look back on first run when no state is available",
    )
    parser.add_argument(
        "--ping-frequency-days",
        type=int,
        default=7,
        help="Heartbeat ping frequency in days (used only when Discord alerts enabled)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.config:
        from config import set_global_config_override

        set_global_config_override(args.config)

    # ensure logging respects new config before other setup
    interval = args.interval
    if interval is None:
        try:
            interval = get_config_manager().get_monitoring_interval()
        except Exception:
            interval = 300

    setup_logging(verbose=args.verbose, no_color=args.no_color)

    if args.config:
        logger.info("🔧 Using configuration: %s", args.config)

    monitor = TokenBridgeWithdrawMonitor(
        disable_discord=args.no_discord,
        start_block=args.start_block,
        max_block_range=args.max_block_range,
        block_batch_size_override=args.block_batch_size,
        initial_lookback_blocks=args.initial_lookback_blocks,
        ping_frequency_days=args.ping_frequency_days,
    )

    if args.once:
        stats = monitor.run_monitoring_cycle()
        logger.info(
            "Run complete | withdraw events: %s | discrepancies: %s | last block: %s",
            stats["withdraw_events"],
            stats["discrepancies"],
            stats["last_processed_block"],
        )
    else:
        monitor.run_continuous(interval)


if __name__ == "__main__":
    main()

