#!/usr/bin/env python3
from typing import List, Callable, Any, Optional, Dict
import time
from web3 import Web3
import requests


class EVMProviderPool:
    def __init__(self, urls: List[str], request_timeout_s: int = 15, preference_reset_minutes: int = 60):
        if not urls:
            raise ValueError("EVMProviderPool requires at least one URL")
        self.urls = urls
        self.request_timeout_s = request_timeout_s
        self.preference_reset_sec = max(1, int(preference_reset_minutes) * 60)
        self._last_reset_ts = 0.0
        self._sticky_index: Optional[int] = None

    def _should_reset_preferences(self) -> bool:
        now = time.time()
        if self._last_reset_ts == 0.0:
            self._last_reset_ts = now
            return False
        return (now - self._last_reset_ts) >= self.preference_reset_sec

    def _iter_indices_in_preference_order(self):
        return range(len(self.urls))

    def _build_web3(self, index: int) -> Web3:
        return Web3(Web3.HTTPProvider(self.urls[index]))

    def ensure_connected(self) -> Web3:
        # Reset preference timer if needed
        if self._should_reset_preferences():
            self._last_reset_ts = time.time()
            self._sticky_index = None

        # Try sticky provider first if set
        if self._sticky_index is not None:
            try:
                w3 = self._build_web3(self._sticky_index)
                _ = w3.eth.block_number
                return w3
            except Exception:
                # sticky failed; clear and fall back to full scan
                self._sticky_index = None

        # Scan from start to find first working provider
        for i in self._iter_indices_in_preference_order():
            try:
                w3 = self._build_web3(i)
                _ = w3.eth.block_number
                self._sticky_index = i
                return w3
            except Exception:
                continue
        raise ConnectionError("No EVM RPC endpoints are reachable")

    def with_contract_call(self, address: str, abi: Any, fn_builder: Callable[[Any], Any], max_attempts: Optional[int] = None):
        # Reset preference on schedule
        if self._should_reset_preferences():
            self._last_reset_ts = time.time()
            self._sticky_index = None

        attempts = 0
        last_error: Optional[Exception] = None

        # 1) Try sticky provider first if available
        if self._sticky_index is not None:
            try:
                w3 = self._build_web3(self._sticky_index)
                contract = w3.eth.contract(address=address, abi=abi)
                result = fn_builder(contract)
                return result
            except Exception as e:
                last_error = e
                # sticky failed; clear it and proceed to full scan
                self._sticky_index = None

        # 2) Scan from beginning to pick the most preferred working provider
        for i in self._iter_indices_in_preference_order():
            if max_attempts is not None and attempts >= max_attempts:
                break
            attempts += 1
            try:
                w3 = self._build_web3(i)
                contract = w3.eth.contract(address=address, abi=abi)
                result = fn_builder(contract)
                self._sticky_index = i
                return result
            except Exception as e:
                last_error = e
                continue

        if last_error:
            raise ConnectionError(f"All EVM RPC endpoints failed for contract call: {last_error}")
        raise ConnectionError("All EVM RPC endpoints failed for contract call")


class HttpEndpointPool:
    def __init__(self, base_urls: List[str], timeout_s: int = 10, preference_reset_minutes: int = 60):
        if not base_urls:
            raise ValueError("HttpEndpointPool requires at least one base URL")
        self.base_urls = [u.rstrip('/') for u in base_urls]
        self.timeout_s = timeout_s
        self.preference_reset_sec = max(1, int(preference_reset_minutes) * 60)
        self._last_reset_ts = 0.0
        self._sticky_index: Optional[int] = None

    def _should_reset_preferences(self) -> bool:
        now = time.time()
        if self._last_reset_ts == 0.0:
            self._last_reset_ts = now
            return False
        return (now - self._last_reset_ts) >= self.preference_reset_sec

    def _full_url(self, index: int, path: str) -> str:
        path = path.lstrip('/')
        return f"{self.base_urls[index]}/{path}"

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        # Reset preference if needed
        if self._should_reset_preferences():
            self._last_reset_ts = time.time()
            self._sticky_index = None

        last_error: Optional[Exception] = None

        # 1) Try sticky first
        if self._sticky_index is not None:
            try:
                url = self._full_url(self._sticky_index, path)
                resp = requests.get(url, params=params, headers=headers, timeout=self.timeout_s)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                last_error = e
                self._sticky_index = None

        # 2) Scan from beginning to find most preferred working endpoint
        for i in range(len(self.base_urls)):
            try:
                url = self._full_url(i, path)
                resp = requests.get(url, params=params, headers=headers, timeout=self.timeout_s)
                resp.raise_for_status()
                data = resp.json()
                self._sticky_index = i
                return data
            except Exception as e:
                last_error = e
                continue

        if last_error:
            raise ConnectionError(f"All Layer HTTP endpoints failed for GET {path}: {last_error}")
        raise ConnectionError(f"All Layer HTTP endpoints failed for GET {path}")


