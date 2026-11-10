#!/usr/bin/env python3
"""
NetSapiens Message Events Subscription Management Script

This script creates and maintains NetSapiens API messaging events subscriptions
to enable message history synchronization into the Acrobits Linkup Messaging platform.

Use cases:
    - Review existing Message Event subscriptions
    - Create new subscriptions for messaging events
    - Maintain and update subscription configurations
    - Report subscription status and health
    - Purge all subscriptions of the callback host that follow the script's
      post-url pattern, for any cloud id (e.g. when a customer migrates to another app)

Usage:
    subscription_manager.py                                   regular run
    subscription_manager.py --purge-subscriptions             list subscriptions that would be purged
    subscription_manager.py --purge-subscriptions --execute   delete them
"""

import argparse
import json
import os
import re
import sys
import time
import traceback
import urllib.request
import urllib.parse
import urllib.error
import ssl
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Any, TypeVar, Callable


# ===============================================
# Version Constants
# ===============================================

SCRIPT_VERSION = "2.0"
REPORT_VERSION = 2


# ===============================================
# Processing Context (for error/script-log reporting)
# ===============================================

class ProcessingStage(Enum):
    """Stages of script execution for error tracking."""
    LOAD_CONFIG = "LOAD_CONFIG"
    FETCH_ACTIVE_DOMAINS = "FETCH_ACTIVE_DOMAINS"
    AUTH_FETCH_OAUTH_TOKEN = "AUTH_FETCH_OAUTH_TOKEN"
    FETCH_DOMAINS = "FETCH_DOMAINS"
    FETCH_SUBSCRIPTIONS = "FETCH_SUBSCRIPTIONS"
    APPLY_SUBSCRIPTION_CHANGES = "APPLY_SUBSCRIPTION_CHANGES"
    REFETCH_SUBSCRIPTIONS = "REFETCH_SUBSCRIPTIONS"
    BUILD_STATUS_REPORT = "BUILD_STATUS_REPORT"
    SEND_STATUS_REPORT = "SEND_STATUS_REPORT"
    RECREATE_FAILING_SUBSCRIPTIONS = "RECREATE_FAILING_SUBSCRIPTIONS"
    PURGE_SUBSCRIPTIONS = "PURGE_SUBSCRIPTIONS"


@dataclass
class ProcessingContext:
    """Mutable context tracking the current processing stage."""
    stage: ProcessingStage = ProcessingStage.LOAD_CONFIG


def set_stage(context: Optional[ProcessingContext], stage: ProcessingStage) -> None:
    """
    Update stage and log it to stdout (so we have useful logs even if remote logging fails).
    """
    if context is None:
        return
    context.stage = stage
    print(f"[STAGE] {stage.value}")


def _get_utc_timestamp() -> str:
    """Get current UTC timestamp in ISO 8601 format for reports."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_subscription_list(subscriptions: list, max_items: int = 10) -> str:
    """Format subscriptions showing count and truncated IDs."""
    ids = [s.id for s in subscriptions]
    preview = ids[:max_items]
    suffix = f" ... and {len(ids) - max_items} more" if len(ids) > max_items else ""
    return f"count={len(ids)} ids={preview}{suffix}"


def _format_list_preview(items: list, max_items: int = 10) -> str:
    """Format a list showing first N items with count if truncated."""
    if len(items) <= max_items:
        return str(items)
    return f"{items[:max_items]} ... and {len(items) - max_items} more"


# ===============================================
# Configuration
# ===============================================

@dataclass
class Config:
    """Typed configuration container for easier dependency injection/testing."""
    ns_api_host: str
    callback_host: str
    callback_password: str
    cloud_id: str


@dataclass
class OAuthConfig:
    username: str
    password: str
    client_id: str
    client_secret: str


@dataclass
class CallbackConfig:
    """
    Minimal config needed to send logs/reports to callback host.

    Allows sending error logs even when full OAuth/NS config is missing.
    """
    callback_host: str
    callback_password: str
    cloud_id: str


def _normalize_base_url(value: str, default_scheme: str = "https") -> str:
    """
    Ensure a base URL has a scheme and no trailing slash.
    If no scheme is provided, default_scheme is prepended.
    """
    if value is None:
        raise ValueError("Base URL cannot be empty")
    normalized = value.strip()
    if not normalized:
        raise ValueError("Base URL cannot be empty")

    parsed = urllib.parse.urlparse(normalized)
    if not parsed.scheme:
        normalized = f"{default_scheme}://{normalized}"
        parsed = urllib.parse.urlparse(normalized)

    if not parsed.netloc:
        raise ValueError(f"Invalid base URL: {value}")

    return normalized.rstrip("/")


def load_config_from_env() -> Config:
    """
    Load configuration from environment variables.
    
    Required environment variables:
        NS_API_HOST: NetSapiens API host with scheme (e.g., "https://sipns.example.net")
        CALLBACK_HOST: Callback URL host with scheme (e.g., "https://api.example.com")
        CALLBACK_PASSWORD: Password for callback authentication
        CLOUD_ID: Cloud identifier

    Active and editable domains are not configured here; they are always
    loaded from the Messaging active-domains endpoint (see fetch_active_domains).

    Schemes default to https if omitted; trailing slashes are removed.
    """
    ns_api_host = os.environ.get("NS_API_HOST")
    callback_host = os.environ.get("CALLBACK_HOST")
    callback_password = os.environ.get("CALLBACK_PASSWORD")
    cloud_id = os.environ.get("CLOUD_ID")
    
    missing = []
    if not ns_api_host:
        missing.append("NS_API_HOST")
    if not callback_host:
        missing.append("CALLBACK_HOST")
    if not callback_password:
        missing.append("CALLBACK_PASSWORD")
    if not cloud_id:
        missing.append("CLOUD_ID")
    
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    return Config(
        ns_api_host=_normalize_base_url(ns_api_host),
        callback_host=_normalize_base_url(callback_host),
        callback_password=callback_password,
        cloud_id=cloud_id,
    )


def load_oauth_config_from_env() -> OAuthConfig:
    """
    Load OAuth configuration from environment variables.
    
    Required environment variables:
        NS_USERNAME: NetSapiens username (e.g., "user@domain")
        NS_PASSWORD: NetSapiens password
        NS_CLIENT_ID: OAuth client ID
        NS_CLIENT_SECRET: OAuth client secret
    """
    username = os.environ.get("NS_USERNAME")
    password = os.environ.get("NS_PASSWORD")
    client_id = os.environ.get("NS_CLIENT_ID")
    client_secret = os.environ.get("NS_CLIENT_SECRET")
    
    missing = []
    if not username:
        missing.append("NS_USERNAME")
    if not password:
        missing.append("NS_PASSWORD")
    if not client_id:
        missing.append("NS_CLIENT_ID")
    if not client_secret:
        missing.append("NS_CLIENT_SECRET")
    
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
    
    return OAuthConfig(
        username=username,
        password=password,
        client_id=client_id,
        client_secret=client_secret,
    )


def load_all_config_from_env() -> tuple[Config, OAuthConfig]:
    """
    Load all configuration from environment variables.
    
    Returns a tuple of (Config, OAuthConfig) by delegating to load_config_from_env()
    and load_oauth_config_from_env().
    
    Validates all required environment variables upfront to report all missing
    variables at once rather than failing on the first missing variable.
    
    See load_config_from_env() and load_oauth_config_from_env() for required
    environment variables.
    """
    # Check all required env vars upfront to report all missing at once
    required_vars = [
        "NS_API_HOST",
        "CALLBACK_HOST", 
        "CALLBACK_PASSWORD",
        "CLOUD_ID",
        "NS_USERNAME",
        "NS_PASSWORD",
        "NS_CLIENT_ID",
        "NS_CLIENT_SECRET",
    ]
    missing = [var for var in required_vars if not os.environ.get(var)]
    
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
    
    return load_config_from_env(), load_oauth_config_from_env()


def load_callback_config_from_env() -> CallbackConfig:
    """
    Load minimal callback configuration from environment variables.

    Required environment variables:
        CALLBACK_HOST: Callback URL host with scheme
        CALLBACK_PASSWORD: Password for callback authentication
        CLOUD_ID: Cloud identifier
    """
    callback_host = os.environ.get("CALLBACK_HOST")
    callback_password = os.environ.get("CALLBACK_PASSWORD")
    cloud_id = os.environ.get("CLOUD_ID")

    missing = []
    if not callback_host:
        missing.append("CALLBACK_HOST")
    if not callback_password:
        missing.append("CALLBACK_PASSWORD")
    if not cloud_id:
        missing.append("CLOUD_ID")

    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    return CallbackConfig(
        callback_host=_normalize_base_url(callback_host),
        callback_password=callback_password,
        cloud_id=cloud_id,
    )


def callback_config_from_config(config: Config) -> CallbackConfig:
    """Create CallbackConfig from full Config to reuse already-loaded values."""
    return CallbackConfig(
        callback_host=config.callback_host,
        callback_password=config.callback_password,
        cloud_id=config.cloud_id,
    )


# ===============================================
# Helper Functions
# ===============================================

class ErrorCode(Enum):
    """Error codes for unrecoverable script failures."""
    CONFIG_LOAD_FAILURE = "CONFIG_LOAD_FAILURE"
    ACTIVE_DOMAINS_RETRIEVAL_FAILURE = "ACTIVE_DOMAINS_RETRIEVAL_FAILURE"
    AUTH_TOKEN_RETRIEVAL_FAILURE = "AUTH_TOKEN_RETRIEVAL_FAILURE"
    DOMAIN_RETRIEVAL_FAILURE = "DOMAIN_RETRIEVAL_FAILURE"
    SUBSCRIPTIONS_RETRIEVAL_FAILURE = "SUBSCRIPTIONS_RETRIEVAL_FAILURE"
    SUBSCRIPTION_MUTATION_FAILURE = "SUBSCRIPTION_MUTATION_FAILURE"
    STATUS_REPORT_SEND_FAILURE = "STATUS_REPORT_SEND_FAILURE"
    FAILING_SUBSCRIPTIONS_RECREATION_FAILURE = "FAILING_SUBSCRIPTIONS_RECREATION_FAILURE"
    UNHANDLED_EXCEPTION = "UNHANDLED_EXCEPTION"


def _infer_error_code(stage: ProcessingStage) -> ErrorCode:
    """Map processing stage to appropriate error code."""
    if stage == ProcessingStage.LOAD_CONFIG:
        return ErrorCode.CONFIG_LOAD_FAILURE
    if stage == ProcessingStage.FETCH_ACTIVE_DOMAINS:
        return ErrorCode.ACTIVE_DOMAINS_RETRIEVAL_FAILURE
    if stage == ProcessingStage.AUTH_FETCH_OAUTH_TOKEN:
        return ErrorCode.AUTH_TOKEN_RETRIEVAL_FAILURE
    if stage == ProcessingStage.FETCH_DOMAINS:
        return ErrorCode.DOMAIN_RETRIEVAL_FAILURE
    if stage in (ProcessingStage.FETCH_SUBSCRIPTIONS, ProcessingStage.REFETCH_SUBSCRIPTIONS):
        return ErrorCode.SUBSCRIPTIONS_RETRIEVAL_FAILURE
    if stage in (ProcessingStage.APPLY_SUBSCRIPTION_CHANGES, ProcessingStage.PURGE_SUBSCRIPTIONS):
        return ErrorCode.SUBSCRIPTION_MUTATION_FAILURE
    if stage == ProcessingStage.SEND_STATUS_REPORT:
        return ErrorCode.STATUS_REPORT_SEND_FAILURE
    if stage == ProcessingStage.RECREATE_FAILING_SUBSCRIPTIONS:
        return ErrorCode.FAILING_SUBSCRIPTIONS_RECREATION_FAILURE
    return ErrorCode.UNHANDLED_EXCEPTION


_SCRIPT_LOG_ALLOWED_LEVELS = {"INFO", "WARN", "WARNING", "ERROR", "DEBUG"}


def send_script_log(
    callback_config: CallbackConfig,
    level: str,
    message: str,
    callstack: Optional[str] = None,
) -> bool:
    """
    Send a script log to the callback host.

    POST to: {callback_host}/netsapiens/callbacks/{cloud_id}/subscriptions-script-log?password={callback_password}
    Body:
      - level (optional): INFO/WARN/WARNING/ERROR/DEBUG
      - message (required)
      - callstack (optional)

    Always logs locally to stdout; remote send is best-effort.
    """
    normalized_level = (level or "").strip().upper()
    if normalized_level not in _SCRIPT_LOG_ALLOWED_LEVELS:
        normalized_level = "INFO"

    payload: dict = {
        "level": normalized_level,
        "message": message,
    }
    if callstack:
        payload["callstack"] = callstack

    encoded_password = urllib.parse.quote(callback_config.callback_password, safe="")
    path = (
        f"/netsapiens/callbacks/{callback_config.cloud_id}/subscriptions-script-log"
        f"?password={encoded_password}"
    )

    callback_client = ApiClient(callback_config.callback_host)

    # Never log password (even encoded) to stdout.
    print(
        f"[SCRIPT_LOG] Sending {normalized_level} log to "
        f"{callback_config.callback_host}/netsapiens/callbacks/{callback_config.cloud_id}/subscriptions-script-log..."
    )

    success, _ = callback_client.post_json_with_status(path, payload)
    if success:
        print("[SCRIPT_LOG] Log sent successfully")
        return True

    print("[SCRIPT_LOG] Failed to send log")
    return False


def _format_exception_stack_trace(exception: Exception) -> str:
    return "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))


def json_value(data: Any, key: str) -> Optional[str]:
    """Extract a value from a dictionary by key."""
    if not isinstance(data, dict):
        raise TypeError(f"Expected dict, got {type(data).__name__}")
    return data.get(key)

@dataclass
class DomainInfo:
    domain: str
    reseller: str

@dataclass
class AuthResult:
    client: "ApiClient"
    user_scope: str

def extract_domain_list(data: Any) -> list[DomainInfo]:
    """Extract all domain and reseller values from a JSON array."""
    if not isinstance(data, list):
        raise TypeError(f"Expected list of domains, got {type(data).__name__}")
    
    result: list[DomainInfo] = []
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            raise TypeError(f"Expected dict at index {index}, got {type(item).__name__}")
        if "domain" not in item:
            raise ValueError(f"Missing 'domain' field at index {index}")
        if "reseller" not in item:
            raise ValueError(f"Missing 'reseller' field at index {index}")
        result.append(DomainInfo(domain=item["domain"], reseller=item["reseller"]))
    return result

# ===============================================
# API Client
# ===============================================

class ApiClient:
    """Simple HTTP client with automatic Bearer token authentication."""
    
    def __init__(
        self,
        base_url: str,
        access_token: Optional[str] = None,
        token_refresher: Optional[Callable[[], str]] = None,
        timeout_seconds: Optional[float] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.access_token = access_token
        self.ssl_ctx = ssl.create_default_context()
        self._token_refresher = token_refresher
        self._timeout_seconds = timeout_seconds
    
    def set_token(self, access_token: str):
        """Set the access token for authenticated requests."""
        self.access_token = access_token
    
    def _make_request(
        self,
        path: str,
        method: str = "GET",
        data: Optional[bytes] = None,
        content_type: Optional[str] = None,
        extra_headers: Optional[dict] = None,
        auth_token: Optional[str] = None
    ) -> Optional[dict]:
        """Internal method to make HTTP requests."""
        success, result = self._make_request_with_status(
            path, method, data, content_type, extra_headers, auth_token
        )
        return result

    _TOKEN_EXPIRED_MARKER = "The access token provided has expired"
    _MAX_TOKEN_REFRESH_RETRIES = 5

    def _make_request_with_status(
        self,
        path: str,
        method: str = "GET",
        data: Optional[bytes] = None,
        content_type: Optional[str] = None,
        extra_headers: Optional[dict] = None,
        auth_token: Optional[str] = None,
    ) -> tuple[bool, Optional[dict]]:
        """
        Internal method to make HTTP requests, returning success status.
        
        Returns:
            tuple of (success: bool, response_data: Optional[dict])
            - success is True for any 2XX response
            - response_data is the parsed JSON body, or None if empty/unparseable
        """
        token_refresh_attempts = 0

        while True:
            url = f"{self.base_url}{path}"
            headers = {}

            token = auth_token or self.access_token
            if token:
                headers["Authorization"] = f"Bearer {token}"

            if content_type:
                headers["Content-Type"] = content_type

            if extra_headers:
                headers.update(extra_headers)

            request = urllib.request.Request(url, data=data, headers=headers, method=method)

            try:
                with self._open(request) as response:
                    response_body = response.read().decode("utf-8")
                    if response_body:
                        return (True, json.loads(response_body))
                    return (True, None)
            except urllib.error.HTTPError as e:
                error_body = ""
                try:
                    error_body = e.read().decode("utf-8")
                except Exception:
                    pass

                if (
                    e.code == 401
                    and self._token_refresher is not None
                    and auth_token is None
                    and self._TOKEN_EXPIRED_MARKER in error_body
                    and token_refresh_attempts < self._MAX_TOKEN_REFRESH_RETRIES
                ):
                    token_refresh_attempts += 1
                    print(f"Access token expired, refreshing (attempt {token_refresh_attempts}/{self._MAX_TOKEN_REFRESH_RETRIES})...")
                    try:
                        self.access_token = self._token_refresher()
                        print("Access token refreshed successfully, retrying request...")
                        continue
                    except Exception as refresh_error:
                        print(f"Token refresh failed: {refresh_error}")
                        return (False, None)

                print(f"HTTP Error {e.code}: {e.reason}")
                if error_body:
                    print(f"Response: {error_body}")
                return (False, None)
            except urllib.error.URLError as e:
                print(f"URL Error: {e.reason}")
                return (False, None)
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")
                return (True, None)  # Request succeeded but response wasn't valid JSON
    
    def _open(self, request: urllib.request.Request):
        if self._timeout_seconds is None:
            return urllib.request.urlopen(request, context=self.ssl_ctx)
        return urllib.request.urlopen(request, context=self.ssl_ctx, timeout=self._timeout_seconds)

    def get(self, path: str) -> Optional[dict]:
        """Make a GET request with automatic authentication."""
        return self._make_request(path, method="GET")

    def get_with_status(self, path: str) -> tuple[bool, Optional[dict]]:
        """
        Make a GET request, returning success status.

        Returns:
            tuple of (success: bool, response_data: Optional[dict])
        """
        return self._make_request_with_status(path, method="GET")
    
    def post_json(self, path: str, body: dict) -> Optional[dict]:
        """Make a POST request with JSON body and automatic authentication."""
        return self._make_request(
            path,
            method="POST",
            data=json.dumps(body).encode("utf-8"),
            content_type="application/json"
        )

    def post_json_with_status(self, path: str, body: dict) -> tuple[bool, Optional[dict]]:
        """
        Make a POST request with JSON body, returning success status.
        
        Returns:
            tuple of (success: bool, response_data: Optional[dict])
        """
        return self._make_request_with_status(
            path,
            method="POST",
            data=json.dumps(body).encode("utf-8"),
            content_type="application/json"
        )

    def put_json(self, path: str, body: dict) -> Optional[dict]:
        """Make a PUT request with JSON body and automatic authentication."""
        return self._make_request(
            path,
            method="PUT",
            data=json.dumps(body).encode("utf-8"),
            content_type="application/json"
        )

    def put_json_with_status(self, path: str, body: dict) -> tuple[bool, Optional[dict]]:
        """
        Make a PUT request with JSON body, returning success status.

        Returns:
            tuple of (success: bool, response_data: Optional[dict])
        """
        return self._make_request_with_status(
            path,
            method="PUT",
            data=json.dumps(body).encode("utf-8"),
            content_type="application/json"
        )

    def delete(self, path: str) -> Optional[dict]:
        """Make a DELETE request with automatic authentication."""
        return self._make_request(path, method="DELETE")

    def delete_with_status(self, path: str) -> tuple[bool, Optional[dict]]:
        """
        Make a DELETE request, returning success status.

        Returns:
            tuple of (success: bool, response_data: Optional[dict])
        """
        return self._make_request_with_status(path, method="DELETE")
    
    def post_form(
        self,
        path: str,
        form_data: dict,
        auth_token: Optional[str] = None,
        extra_headers: Optional[dict] = None
    ) -> Optional[dict]:
        """Make a POST request with form-urlencoded body."""
        return self._make_request(
            path,
            method="POST",
            data=urllib.parse.urlencode(form_data).encode("utf-8"),
            content_type="application/x-www-form-urlencoded",
            auth_token=auth_token,
            extra_headers=extra_headers
        )

# ===============================================
# Subscription Data Classes
# ===============================================

# Example:
# {
#     "id": "7097db6a6dda6e5ca2a99225abfbe356",
#     "subscription-geo-support": "yes",
#     "post-url": "https:\/\/your-callback-host.com\/netsapiens\/callbacks\/your-cloud-id\/message-session?password=your-callback-password",
#     "model": "messagesession",
#     "user-scope": "Super User",
#     "reseller": "Acme_Networks",
#     "domain": "Acme",
#     "user": "1000",
#     "preferred-server": "sipns.acme.net",
#     "current-active-server": "",
#     "status": "pending",
#     "error-count": 0,
#     "posts-count": 0,
#     "subscription-creation-datetime": "2025-12-11T16:28:17+00:00",
#     "subscription-expires-datetime": "2045-12-11T17:28:17+00:00"
# }

# Subscription Model Enum
class SubscriptionModel(Enum):
    MESSAGE_SESSION = "messagesession"
    MESSAGE = "message"
    OTHER = "other_model"

    @classmethod
    def from_string(cls, value: str) -> "SubscriptionModel":
        for member in cls:
            if member.value == value:
                return member
        return cls.OTHER

    def to_path_suffix(self) -> str:
        if self == SubscriptionModel.MESSAGE_SESSION:
            return "message-session"
        elif self == SubscriptionModel.MESSAGE:
            return "message"
        else:
            raise ValueError(f"Invalid model: {self}")

@dataclass
class Subscription:
    id: str
    domain: str
    model: SubscriptionModel
    current_active_server: str
    subscription_expires: str
    post_url: str
    subscription_geo_support: str
    user_scope: str
    reseller: str
    user: str
    preferred_server: str
    status: str
    error_count: int
    posts_count: int
    subscription_creation: str

    _REQUIRED_KEYS = {
        "id", "domain", "model", "current-active-server", "subscription-expires-datetime",
        "post-url", "subscription-geo-support", "user-scope", "reseller", "user",
        "preferred-server", "status", "error-count", "posts-count", "subscription-creation-datetime"
    }

    @classmethod
    def from_dict(cls, data: dict) -> "Subscription":
        if not cls._is_valid_dict(data):
            raise ValueError(f"Invalid subscription data: {data}")
        return cls(
            id=data["id"],
            domain=data["domain"],
            model=SubscriptionModel.from_string(data["model"]),
            current_active_server=data["current-active-server"],
            subscription_expires=data["subscription-expires-datetime"],
            post_url=data["post-url"],
            subscription_geo_support=data["subscription-geo-support"],
            user_scope=data["user-scope"],
            reseller=data["reseller"],
            user=data["user"],
            preferred_server=data["preferred-server"],
            status=data["status"],
            error_count=data["error-count"],
            posts_count=data["posts-count"],
            subscription_creation=data["subscription-creation-datetime"]
        )

    @staticmethod
    def _is_valid_dict(data: dict) -> bool:
        return Subscription._REQUIRED_KEYS.issubset(data.keys())

@dataclass
class SubscriptionReviewResult:
    subscriptions_in_inactive_domains: list[Subscription]
    subscriptions_in_unknown_domains: list[Subscription]
    subscriptions_with_invalid_active_server: list[Subscription]
    subscriptions_with_outdated_post_url: list[Subscription]
    domains_without_message_subscription: list[str]
    domains_without_messagesession_subscription: list[str]
    missing_active_domains: list[str]


# Mirrors NetsapiensFailingSubscriptionDto returned by the callback host
# GET /netsapiens/callbacks/{cloudId}/failing-subscriptions endpoint.
# Only the fields actually consumed by the recreation flow are mapped.
@dataclass
class FailingSubscription:
    cloud_id: str
    domain: str
    subscription_id: str

    @classmethod
    def from_dict(cls, data: dict) -> "FailingSubscription":
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data).__name__}")
        for required_key in ("cloudId", "domain", "subscriptionId"):
            if required_key not in data:
                raise ValueError(f"Missing '{required_key}' in failing subscription payload: {data}")
        return cls(
            cloud_id=data["cloudId"],
            domain=data["domain"],
            subscription_id=data["subscriptionId"],
        )


# Mirrors NetsapiensActiveDomainsDto returned by the callback host
# GET /netsapiens/callbacks/{cloudId}/active-domains endpoint.
@dataclass
class ActiveDomains:
    active_domains: list[str]
    editable_domains: list[str]


@dataclass
class DomainSelection:
    """
    Domain selection evaluated once per run from the Messaging active domains
    and the NetSapiens domain list. Shared by failing subscription recreation,
    review, routing and the status report so all of them describe the same decision.
    """
    active_domains: list[str]
    editable_domains: list[str]
    selected_domain_names: list[str]
    missing_active_domains: list[str]

    def is_selected(self, domain: str) -> bool:
        return domain in self.selected_domain_names

    def is_editable(self, domain: str) -> bool:
        # Case-insensitive like ChatConnectorCallbackService.isCallbackAllowed on the server.
        return any(editable.casefold() == domain.casefold() for editable in self.editable_domains)

# ===============================================
# Status Report Data Classes
# ===============================================

class CoverageStatus(Enum):
    """
    Domain subscription coverage status.
    
    - FULLY_COVERED: Domain has both 'message' and 'messagesession' subscriptions,
      and neither has any issues.
    
    - COVERED_WITH_ISSUES: Domain has at least one subscription, but one or more
      subscriptions have issues (e.g., INVALID_ACTIVE_SERVER). This includes:
      - Both subscriptions exist but one or both have issues
      - Only one subscription exists and it has issues
    
    - PARTIALLY_COVERED: Domain has only one subscription type (either 'message' or 
      'messagesession', but not both), and that subscription has no issues.
      Issues will include MISSING_MESSAGE_SUBSCRIPTION or MISSING_MESSAGESESSION_SUBSCRIPTION.
    
    - NOT_COVERED: Domain has no subscriptions at all. Issues will include both 
      MISSING_MESSAGE_SUBSCRIPTION and MISSING_MESSAGESESSION_SUBSCRIPTION.
    
    - EXCLUDED: Domain is not in the active domains returned by the Messaging
      active-domains endpoint. No issues are tracked for excluded domains.
    """
    FULLY_COVERED = "FULLY_COVERED"
    COVERED_WITH_ISSUES = "COVERED_WITH_ISSUES"
    PARTIALLY_COVERED = "PARTIALLY_COVERED"
    NOT_COVERED = "NOT_COVERED"
    EXCLUDED = "EXCLUDED"


class ExclusionReason(Enum):
    """Reason why a domain was excluded from coverage tracking."""
    NOT_IN_ALLOWED_LIST = "NOT_IN_ALLOWED_LIST"  # Domain is not in activeDomains returned by the Messaging endpoint


class SubscriptionIssue(Enum):
    """
    Issues detected for a domain's subscriptions.
    
    Coverage-related issues:
    - MISSING_MESSAGE_SUBSCRIPTION: No 'message' subscription exists for the domain.
      Present when coverage is PARTIALLY_COVERED, COVERED_WITH_ISSUES (partial), or NOT_COVERED.
    - MISSING_MESSAGESESSION_SUBSCRIPTION: No 'messagesession' subscription exists.
      Present when coverage is PARTIALLY_COVERED, COVERED_WITH_ISSUES (partial), or NOT_COVERED.
    
    Subscription health issues (triggers COVERED_WITH_ISSUES status):
    - INVALID_ACTIVE_SERVER: A subscription exists but its current_active_server
      is in an abnormal state — either empty (no server resolved yet) or
      scheme-prefixed like "https://host" (stale data from an older script
      version that wrongly sent preferred-server with a scheme). Either
      condition indicates the subscription is not in a clean state and will
      be re-posted via update_subscription on the next run so NS can
      re-resolve current_active_server in the canonical host-only format.
    """
    MISSING_MESSAGE_SUBSCRIPTION = "MISSING_MESSAGE_SUBSCRIPTION"
    MISSING_MESSAGESESSION_SUBSCRIPTION = "MISSING_MESSAGESESSION_SUBSCRIPTION"
    INVALID_ACTIVE_SERVER = "INVALID_ACTIVE_SERVER"


@dataclass
class SubscriptionReport:
    id: str
    status: str
    current_active_server: str
    expires: str
    post_url: str
    error_count: int
    posts_count: int

    @classmethod
    def from_subscription(cls, subscription: Subscription) -> "SubscriptionReport":
        return cls(
            id=subscription.id,
            status=subscription.status,
            current_active_server=subscription.current_active_server,
            expires=subscription.subscription_expires,
            post_url=subscription.post_url,
            error_count=subscription.error_count,
            posts_count=subscription.posts_count
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "status": self.status,
            "current_active_server": self.current_active_server,
            "expires": self.expires,
            "post_url": self.post_url,
            "error_count": self.error_count,
            "posts_count": self.posts_count
        }


@dataclass
class DomainReport:
    domain: str
    reseller: str
    coverage_status: CoverageStatus
    exclusion_reason: Optional[ExclusionReason]
    message_subscription: Optional[SubscriptionReport]
    messagesession_subscription: Optional[SubscriptionReport]
    issues: list[SubscriptionIssue]

    def to_dict(self) -> dict:
        result = {
            "domain": self.domain,
            "reseller": self.reseller,
            "coverage_status": self.coverage_status.value,
            "subscriptions": {
                "message": self.message_subscription.to_dict() if self.message_subscription else None,
                "messagesession": self.messagesession_subscription.to_dict() if self.messagesession_subscription else None
            },
            "issues": [issue.value for issue in self.issues]
        }
        if self.exclusion_reason:
            result["exclusion_reason"] = self.exclusion_reason.value
        return result


@dataclass
class OrphanedSubscriptionReport:
    id: str
    domain: str
    model: str
    reason: str
    post_url: str

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "domain": self.domain,
            "model": self.model,
            "reason": self.reason,
            "post_url": self.post_url
        }


@dataclass
class StatusReportSummary:
    total_domains_from_api: int
    effective_domains_count: int
    fully_covered_domains_count: int
    covered_with_issues_domains_count: int
    partially_covered_domains_count: int
    uncovered_domains_count: int

    def to_dict(self) -> dict:
        return {
            "total_domains_from_api": self.total_domains_from_api,
            "effective_domains_count": self.effective_domains_count,
            "fully_covered_domains_count": self.fully_covered_domains_count,
            "covered_with_issues_domains_count": self.covered_with_issues_domains_count,
            "partially_covered_domains_count": self.partially_covered_domains_count,
            "uncovered_domains_count": self.uncovered_domains_count
        }


@dataclass
class StatusReportConfiguration:
    ns_api_host: str
    callback_host: str
    allowed_domains: list[str]
    disallowed_domains: list[str]
    resellers: list[str]
    editable_version_domain: str

    def to_dict(self) -> dict:
        return {
            "ns_api_host": self.ns_api_host,
            "callback_host": self.callback_host,
            "allowed_domains": self.allowed_domains,
            "disallowed_domains": self.disallowed_domains,
            "resellers": self.resellers,
            "editable_version_domain": self.editable_version_domain
        }


@dataclass
class StatusReport:
    script_version: str
    report_version: int
    report_timestamp: str
    cloud_id: str
    configuration: StatusReportConfiguration
    summary: StatusReportSummary
    domains: list[DomainReport]
    orphaned_subscriptions: list[OrphanedSubscriptionReport]

    def to_dict(self) -> dict:
        return {
            "script_version": self.script_version,
            "report_version": self.report_version,
            "report_timestamp": self.report_timestamp,
            "cloud_id": self.cloud_id,
            "configuration": self.configuration.to_dict(),
            "summary": self.summary.to_dict(),
            "domains": [d.to_dict() for d in self.domains],
            "orphaned_subscriptions": [o.to_dict() for o in self.orphaned_subscriptions]
        }


# ===============================================
# Subscription Management
# ===============================================

def extract_subscriptions_from_api_response(data: Any) -> list[Subscription]:
    """Extract all subscription values from a JSON array."""
    if not isinstance(data, list):
        raise TypeError(f"Expected list of subscriptions, got {type(data).__name__}")
    
    subscriptions: list[Subscription] = []
    for item in data:
        if isinstance(item, dict):
            subscriptions.append(Subscription.from_dict(item))
        else:
            raise TypeError(f"Expected dict in subscription list, got {type(item).__name__}")
    return subscriptions


def _post_url_matches_callback_host(post_url: str, callback_host: str) -> bool:
    """Return True if the subscription post_url matches our callback host."""
    if not post_url:
        return False
    parsed_post_url = urllib.parse.urlparse(post_url)
    parsed_callback = urllib.parse.urlparse(callback_host)
    return (
        parsed_post_url.scheme == parsed_callback.scheme and
        parsed_post_url.netloc == parsed_callback.netloc
    )


def extract_subscriptions_for_callback_host(data: Any, callback_host: str) -> list[Subscription]:
    """Extract subscriptions whose post-url points to the configured callback host."""
    subscriptions = extract_subscriptions_from_api_response(data)
    return [
        subscription
        for subscription in subscriptions
        if _post_url_matches_callback_host(subscription.post_url, callback_host)
    ]

def compute_domain_selection(active_domains: ActiveDomains, domain_infos: list[DomainInfo]) -> DomainSelection:
    """
    Evaluate which domains are managed in this run.

    selected = activeDomains from Messaging that are also known to NetSapiens
    missing  = activeDomains from Messaging that NetSapiens does not know

    Domain names are compared exactly. An empty activeDomains list selects nothing.
    """
    available_domain_names = [info.domain for info in domain_infos]
    available_domain_names_set = set(available_domain_names)
    active_domain_names_set = set(active_domains.active_domains)
    return DomainSelection(
        active_domains=list(active_domains.active_domains),
        editable_domains=list(active_domains.editable_domains),
        selected_domain_names=[domain for domain in available_domain_names if domain in active_domain_names_set],
        missing_active_domains=[
            domain for domain in active_domains.active_domains if domain not in available_domain_names_set
        ],
    )


def _find_subscription_by_domain_and_model(
    subscriptions: list[Subscription],
    domain: str,
    model: SubscriptionModel
) -> Optional[Subscription]:
    """Find a subscription matching the given domain and model."""
    return next(
        (s for s in subscriptions if s.domain == domain and s.model == model),
        None
    )


def review_subscriptions(
    subscriptions: list[Subscription],
    domain_infos: list[DomainInfo],
    config: Config,
    selection: DomainSelection
) -> SubscriptionReviewResult:
    """
    Analyze existing subscriptions against domain metadata and configuration.
    
    - Subscriptions not targeting our callback host (config.callback_host) are filtered out.
    - Subscriptions in domains not returned by the API are marked as unknown.
    - Domain selection comes from `selection` (Messaging activeDomains known to NetSapiens);
      subscriptions for known domains that are not selected are surfaced for removal.
    - For the remaining subscriptions, abnormal `current_active_server` values are flagged
      (empty, or scheme-prefixed such as "https://host" — see _is_active_server_invalid).
    - For each selected domain, presence of both `message` and `messagesession` subscriptions is verified.
    - Post-url routing (live vs editable cloud id) follows `selection.editable_domains`.
    """
    
    # Filter out subscriptions that do not target our callback host to avoid touching foreign subscriptions.
    # NS API is fragile, in theory, we can still break foreign subscriptions by subscribing to the same domain,
    # but we just assume that that will not be the case.
    subscriptions = [
        subscription
        for subscription in subscriptions
        if _post_url_matches_callback_host(subscription.post_url, config.callback_host)
    ]
    
    domain_names = [info.domain for info in domain_infos]

    # Filter out subscriptions that are in unknown domains, not sure if this can happen, but just in case.
    subscriptions_in_unknown_domains = [
        subscription for subscription in subscriptions if subscription.domain not in domain_names
    ]

    # Subscriptions whose domain is known but not active in Messaging are
    # out of scope and must be removed.
    subscriptions_in_inactive_domains = [
        subscription
        for subscription in subscriptions
        if subscription.domain in domain_names and not selection.is_selected(subscription.domain)
    ]

    excluded_subscription_ids = {
        subscription.id for subscription in (subscriptions_in_inactive_domains + subscriptions_in_unknown_domains)
    }
    allowed_subscriptions = [
        subscription for subscription in subscriptions if subscription.id not in excluded_subscription_ids
    ]
    subscriptions_with_invalid_active_server = [
        subscription for subscription in allowed_subscriptions
        if _is_active_server_invalid(subscription.current_active_server)
    ]
    # Detect subscriptions whose post-url path no longer matches what the current
    # config would produce (e.g. created with a wrong cloud_id in a previous run).
    # These need their body re-posted via update_subscription so the path is healed.
    subscriptions_with_outdated_post_url = [
        subscription for subscription in allowed_subscriptions
        if _post_url_path_is_outdated(subscription, config, selection)
    ]
    domains_without_message_subscription: list[str] = []
    domains_without_messagesession_subscription: list[str] = []

    for domain in selection.selected_domain_names:
        # find the messagesession subscription for the domain
        messagesession_subscription = _find_subscription_by_domain_and_model(
            allowed_subscriptions, domain, SubscriptionModel.MESSAGE_SESSION
        )
        if not messagesession_subscription:
            domains_without_messagesession_subscription.append(domain)
        # find the message subscription for the domain
        message_subscription = _find_subscription_by_domain_and_model(
            allowed_subscriptions, domain, SubscriptionModel.MESSAGE
        )
        if not message_subscription:
            domains_without_message_subscription.append(domain)

    return SubscriptionReviewResult(
        subscriptions_in_inactive_domains=subscriptions_in_inactive_domains,
        subscriptions_in_unknown_domains=subscriptions_in_unknown_domains,
        subscriptions_with_invalid_active_server=subscriptions_with_invalid_active_server,
        subscriptions_with_outdated_post_url=subscriptions_with_outdated_post_url,
        domains_without_message_subscription=domains_without_message_subscription,
        domains_without_messagesession_subscription=domains_without_messagesession_subscription,
        missing_active_domains=selection.missing_active_domains
    )


# ===============================================
# Status Report Builder
# ===============================================

def build_status_report(
    subscriptions: list[Subscription],
    domain_infos: list[DomainInfo],
    config: Config,
    selection: DomainSelection,
    report_timestamp: str
) -> StatusReport:
    """
    Build a status report of subscription coverage across all domains.
    
    The report includes:
    - Full info about subscriptions for each domain
    - Domains that are fully covered, partially covered, covered with issues, not covered, or excluded
    - Orphaned subscriptions (subscriptions for domains not in the API)
    - Subscriptions with issues (e.g., invalid active server)
    """
    # Filter subscriptions to only those targeting our callback host
    our_subscriptions = [
        sub for sub in subscriptions
        if _post_url_matches_callback_host(sub.post_url, config.callback_host)
    ]
    
    domain_names = [info.domain for info in domain_infos]

    # Build domain reports
    domain_reports: list[DomainReport] = []
    orphaned_subscriptions: list[OrphanedSubscriptionReport] = []
    
    # Track counts for summary
    fully_covered_count = 0
    covered_with_issues_count = 0
    partially_covered_count = 0
    uncovered_count = 0

    # Process each domain from the API
    for domain_info in domain_infos:
        domain = domain_info.domain
        reseller = domain_info.reseller
        
        is_excluded = not selection.is_selected(domain)
        exclusion_reason = ExclusionReason.NOT_IN_ALLOWED_LIST if is_excluded else None
        
        # Find subscriptions for this domain
        message_sub = _find_subscription_by_domain_and_model(our_subscriptions, domain, SubscriptionModel.MESSAGE)
        messagesession_sub = _find_subscription_by_domain_and_model(our_subscriptions, domain, SubscriptionModel.MESSAGE_SESSION)
        
        # Build subscription reports
        message_report = SubscriptionReport.from_subscription(message_sub) if message_sub else None
        messagesession_report = SubscriptionReport.from_subscription(messagesession_sub) if messagesession_sub else None
        
        # Determine coverage status and issues
        issues: list[SubscriptionIssue] = []
        
        if is_excluded:
            coverage_status = CoverageStatus.EXCLUDED
        else:
            has_message = message_sub is not None
            has_messagesession = messagesession_sub is not None
            has_any_sub = has_message or has_messagesession
            
            # Check for missing subscriptions
            if not has_message:
                issues.append(SubscriptionIssue.MISSING_MESSAGE_SUBSCRIPTION)
            if not has_messagesession:
                issues.append(SubscriptionIssue.MISSING_MESSAGESESSION_SUBSCRIPTION)
            
            # Check for invalid active server on existing subscriptions
            # (empty or scheme-prefixed; see _is_active_server_invalid)
            for sub in [message_sub, messagesession_sub]:
                if sub and _is_active_server_invalid(sub.current_active_server):
                    if SubscriptionIssue.INVALID_ACTIVE_SERVER not in issues:
                        issues.append(SubscriptionIssue.INVALID_ACTIVE_SERVER)
            
            # Determine coverage status based on subscriptions and issues
            has_health_issue = SubscriptionIssue.INVALID_ACTIVE_SERVER in issues
            
            if not has_any_sub:
                coverage_status = CoverageStatus.NOT_COVERED
                uncovered_count += 1
            elif has_health_issue:
                coverage_status = CoverageStatus.COVERED_WITH_ISSUES
                covered_with_issues_count += 1
            elif has_message and has_messagesession:
                coverage_status = CoverageStatus.FULLY_COVERED
                fully_covered_count += 1
            else:
                coverage_status = CoverageStatus.PARTIALLY_COVERED
                partially_covered_count += 1
        
        domain_reports.append(DomainReport(
            domain=domain,
            reseller=reseller,
            coverage_status=coverage_status,
            exclusion_reason=exclusion_reason,
            message_subscription=message_report,
            messagesession_subscription=messagesession_report,
            issues=issues
        ))
    
    # Find orphaned subscriptions (subscriptions for domains not in the API)
    for sub in our_subscriptions:
        if sub.domain not in domain_names:
            orphaned_subscriptions.append(OrphanedSubscriptionReport(
                id=sub.id,
                domain=sub.domain,
                model=sub.model.value,
                reason="DOMAIN_NOT_IN_API",
                post_url=sub.post_url
            ))
    
    # Keeps the existing report contract: allowed_domains carries the Messaging activeDomains,
    # the removed configuration filters are reported empty. Editable routing is visible in post_url.
    configuration = StatusReportConfiguration(
        ns_api_host=config.ns_api_host,
        callback_host=config.callback_host,
        allowed_domains=selection.active_domains,
        disallowed_domains=[],
        resellers=[],
        editable_version_domain=""
    )
    
    # Build summary
    summary = StatusReportSummary(
        total_domains_from_api=len(domain_infos),
        effective_domains_count=len(selection.selected_domain_names),
        fully_covered_domains_count=fully_covered_count,
        covered_with_issues_domains_count=covered_with_issues_count,
        partially_covered_domains_count=partially_covered_count,
        uncovered_domains_count=uncovered_count
    )
    
    return StatusReport(
        script_version=SCRIPT_VERSION,
        report_version=REPORT_VERSION,
        report_timestamp=report_timestamp,
        cloud_id=config.cloud_id,
        configuration=configuration,
        summary=summary,
        domains=domain_reports,
        orphaned_subscriptions=orphaned_subscriptions
    )


def send_status_report(report: StatusReport, config: Config) -> bool:
    """
    Send the status report to the callback host.
    
    POST to: {callback_host}/netsapiens/callbacks/{cloud_id}/subscriptions-status?password={callback_password}
    
    Returns True if the report was sent successfully, False otherwise.
    """
    encoded_password = urllib.parse.quote(config.callback_password, safe='')
    path = f"/netsapiens/callbacks/{config.cloud_id}/subscriptions-status?password={encoded_password}"
    
    callback_client = ApiClient(config.callback_host)
    
    url_for_logging = f"{config.callback_host}{path}"
    url_for_logging = url_for_logging.replace(encoded_password, "****")
    print(f"Sending status report to {url_for_logging}...")
    
    success, response = callback_client.post_json_with_status(path, report.to_dict())
    
    if success:
        print("Status report sent successfully")
        return True
    else:
        print("Failed to send status report")
        return False


# Suffix appended to cloud_id for editable domains to route callbacks to the editable app version
EDITABLE_VERSION_CLOUD_ID_SUFFIX = "*"


def _build_post_url(model: SubscriptionModel, domain: str, config: Config, selection: DomainSelection) -> str:
    """
    Build the canonical post-url for a (model, domain) pair under the current config
    and the editable routing loaded from Messaging.

    Single source of truth for both creating new subscriptions and validating
    existing ones (see _post_url_path_is_outdated).
    """
    path_suffix = model.to_path_suffix()
    encoded_password = urllib.parse.quote(config.callback_password, safe='')
    needs_suffix = selection.is_editable(domain) and not config.cloud_id.endswith(EDITABLE_VERSION_CLOUD_ID_SUFFIX)
    cloud_id = f"{config.cloud_id}{EDITABLE_VERSION_CLOUD_ID_SUFFIX}" if needs_suffix else config.cloud_id
    return f"{config.callback_host}/netsapiens/callbacks/{cloud_id}/{path_suffix}?password={encoded_password}"


def _post_url_path_is_outdated(subscription: Subscription, config: Config, selection: DomainSelection) -> bool:
    """
    Return True if the subscription's post_url path no longer matches the canonical
    path computed from the current config (e.g. because cloud_id has changed since
    the subscription was created).

    Compares only the URL path, not the query string. This deliberately ignores
    incidental differences in the password query parameter (encoding, rotation)
    and focuses on the cloud_id / model path segment, which is what we want
    to detect and heal here.

    Subscriptions whose model is not MESSAGE or MESSAGE_SESSION have no canonical
    path and are treated as not outdated.
    """
    if subscription.model not in (SubscriptionModel.MESSAGE, SubscriptionModel.MESSAGE_SESSION):
        return False
    expected_path = urllib.parse.urlparse(
        _build_post_url(subscription.model, subscription.domain, config, selection)
    ).path
    actual_path = urllib.parse.urlparse(subscription.post_url).path
    return actual_path != expected_path


def _is_active_server_invalid(active_server: str) -> bool:
    """
    Return True if current_active_server is in an abnormal state that warrants
    re-posting the subscription via update_subscription.

    The expected canonical format reported by NetSapiens is host-only
    (e.g. "sipns.acme.net"). Two conditions are treated as invalid:

    1. Empty: the subscription has no active server resolved yet
       (newly created, or NS lost the binding).
    2. Scheme-prefixed (e.g. "https://sipns.acme.net"): stale data left over
       from older script versions that incorrectly sent preferred-server with
       a scheme. Re-posting sends preferred-server as the host-only form of this
       same value (scheme stripped; see _resolve_preferred_server_host), letting
       NS re-resolve current-active-server in the canonical format.

    Detection of (2) relies on the literal "://" substring rather than
    urlparse's scheme field, because a bare "host:port" string can be
    misclassified as having a scheme by some URL parsers.
    """
    if not active_server:
        return True
    return "://" in active_server


def _strip_scheme_to_host(value: str) -> str:
    """
    Return the host-only form of a server value, stripping any scheme prefix.

    "https://sipns.acme.net" -> "sipns.acme.net"; a value that is already
    host-only ("sipns.acme.net") is returned unchanged. Scheme detection relies
    on the literal "://" substring (consistent with _is_active_server_invalid),
    because urlparse can misclassify a bare "host:port" string as having a scheme.
    """
    if "://" in value:
        return urllib.parse.urlparse(value).netloc
    return value


def _resolve_preferred_server_host(current_active_server: str, config: Config) -> str:
    """
    Determine the host-only preferred-server value to send on a create/update.

    Resolution:
    - If current_active_server is empty/None, fall back to the host of
      config.ns_api_host (ns_api_host is normalized with a scheme by
      _normalize_base_url, so .netloc is always populated).
    - Otherwise use current_active_server, stripping any scheme prefix so the
      value stays host-only (e.g. a stale "https://sipns.acme.net" becomes
      "sipns.acme.net"). If stripping leaves an empty host, fall back to config.
    """
    config_host = urllib.parse.urlparse(config.ns_api_host).netloc
    if not current_active_server:
        return config_host
    return _strip_scheme_to_host(current_active_server) or config_host


def _build_subscription_body(
    model: SubscriptionModel,
    domain: str,
    reseller: str,
    user_scope: str,
    subscription_expires: str,
    config: Config,
    selection: DomainSelection,
    current_active_server: str = ""
) -> dict:
    """
    Build the JSON body for subscription create/update requests.

    preferred-server is derived from current_active_server (host-only) when
    provided, falling back to config.ns_api_host when it is empty. New creates
    that have no existing subscription pass an empty current_active_server and
    therefore always take preferred-server from config.
    """
    post_url = _build_post_url(model, domain, config, selection)

    preferred_server_host = _resolve_preferred_server_host(current_active_server, config)

    return {
        "post-url": post_url,
        "model": model.value,
        "domain": domain,
        "user": "*",
        "reseller": reseller,
        "user-scope": user_scope,
        "subscription-geo-support": "yes",
        "preferred-server": preferred_server_host,
        "subscription-expires-datetime": subscription_expires
    }


def _sanitize_subscription_body_for_log(json_body: dict, config: Config) -> dict:
    """
    Return a shallow copy of the subscription body suitable for logging.

    The `post-url` field embeds the callback password as a query parameter,
    so we mask both the URL-encoded and raw forms before sending the body
    to any log destination (stdout or remote script-log endpoint).
    """
    sanitized = dict(json_body)
    post_url = sanitized.get("post-url")
    if isinstance(post_url, str) and config.callback_password:
        encoded_password = urllib.parse.quote(config.callback_password, safe="")
        sanitized["post-url"] = (
            post_url.replace(encoded_password, "****").replace(config.callback_password, "****")
        )
    return sanitized


def _format_subscription_body_for_log(json_body: dict, config: Config) -> str:
    """Format a subscription body as a sanitized single-line JSON string for logs."""
    return json.dumps(_sanitize_subscription_body_for_log(json_body, config), sort_keys=True)


_SUBSCRIPTION_CREATE_MONO_TIMES: list[float] = []
_SUBSCRIPTION_CREATE_MAX_PER_SECOND = 3


def _throttle_subscription_creates() -> None:
    """Block until a new subscription POST is allowed (max _SUBSCRIPTION_CREATE_MAX_PER_SECOND per 1s sliding window)."""
    window_sec = 1.0
    times = _SUBSCRIPTION_CREATE_MONO_TIMES
    while True:
        now = time.monotonic()
        cutoff = now - window_sec
        while times and times[0] <= cutoff:
            times.pop(0)
        if len(times) < _SUBSCRIPTION_CREATE_MAX_PER_SECOND:
            times.append(time.monotonic())
            return
        sleep_for = window_sec - (now - times[0])
        if sleep_for > 0:
            time.sleep(sleep_for)


def create_subscription(
    client: ApiClient,
    model: SubscriptionModel,
    domain: str,
    reseller: str,
    user_scope: str,
    subscription_expires: str,
    config: Config,
    selection: DomainSelection,
    current_active_server: str = ""
) -> Optional[str]:
    """Create a subscription and return the subscription ID."""
    json_body = _build_subscription_body(
        model, domain, reseller, user_scope, subscription_expires, config, selection, current_active_server
    )

    _throttle_subscription_creates()

    print(f"Creating {model.value} subscription...")
    
    response = client.post_json("/ns-api/v2/subscriptions", json_body)
    
    if response:
        subscription_id = json_value(response, "id")
        if subscription_id:
            print(f"{model.value} subscription created successfully with id: {subscription_id}")
            return subscription_id
        else:
            print(f"Error: subscription id not found in response")
            encoded_password = urllib.parse.quote(config.callback_password, safe="")
            response_for_log = str(response).replace(encoded_password, "****").replace(config.callback_password, "****")
            print(f"Response: {response_for_log}")
            failure_reason = f"subscription id not found in response: {response_for_log}"
    else:
        print(f"Error: Empty or failed response from {model.value} subscription request")
        failure_reason = "empty or failed response from create request"

    # Per-failure remote log: include the full attempted state so the server-side
    # log captures every field value the script tried to apply for this domain.
    sanitized_body = _format_subscription_body_for_log(json_body, config)
    print(f"[CREATE_FAILURE] Failed state for domain={domain} model={model.value}: {sanitized_body}")
    send_script_log(
        callback_config=callback_config_from_config(config),
        level="ERROR",
        message=(
            f"Failed to create {model.value} subscription for domain={domain} "
            f"(reseller={reseller}, user-scope={user_scope}): {failure_reason}. "
            f"Attempted state: {sanitized_body}"
        ),
    )

    return None


def update_subscription(
    client: ApiClient,
    subscription: Subscription,
    reseller: str,
    user_scope: str,
    subscription_expires: str,
    config: Config,
    selection: DomainSelection
) -> bool:
    """Update a subscription. Returns True on success, False on failure."""
    json_body = _build_subscription_body(
        subscription.model,
        subscription.domain,
        reseller,
        user_scope,
        subscription_expires,
        config,
        selection,
        current_active_server=subscription.current_active_server,
    )
    
    print(f"Updating {subscription.model.value} subscription {subscription.id}...")
    
    success, response = client.put_json_with_status(f"/ns-api/v2/subscriptions/{subscription.id}", json_body)
    
    if success:
        print(f"{subscription.model.value} subscription {subscription.id} updated successfully")
        return True

    print(f"Error: Empty or failed response from {subscription.model.value} subscription update request")

    # Per-failure remote log: include the full attempted state so the server-side
    # log captures every field value the script tried to apply for this subscription.
    sanitized_body = _format_subscription_body_for_log(json_body, config)
    print(
        f"[UPDATE_FAILURE] Failed state for id={subscription.id} "
        f"domain={subscription.domain} model={subscription.model.value}: {sanitized_body}"
    )
    send_script_log(
        callback_config=callback_config_from_config(config),
        level="ERROR",
        message=(
            f"Failed to update {subscription.model.value} subscription id={subscription.id} "
            f"(domain={subscription.domain}, reseller={reseller}, user-scope={user_scope}). "
            f"Attempted state: {sanitized_body}"
        ),
    )

    return False


def delete_subscription(client: ApiClient, subscription_id: str) -> bool:
    """Delete a subscription. Returns True on success, False on failure."""
    success, _ = client.delete_with_status(f"/ns-api/v2/subscriptions/{subscription_id}")
    if success:
        print(f"Subscription {subscription_id} deleted successfully")
    else:
        print(f"Error: Failed to delete subscription {subscription_id}")
    return success

# ===============================================
# Active Domains Retrieval
# ===============================================

_ACTIVE_DOMAINS_TIMEOUT_SECONDS = 15.0


def fetch_active_domains(callback_config: CallbackConfig) -> ActiveDomains:
    """
    Fetch active and editable domains from the callback host. This is the only
    source of the domain selection; there is no fallback to configuration.

    GET to: {callback_host}/netsapiens/callbacks/{cloud_id}/active-domains?password={callback_password}

    The exact configured cloud_id (including a possible editable '*' suffix) is used,
    because the callback host authenticates against the connector of that cloud_id.

    Raises RuntimeError when the request fails or the payload is not a valid
    {"activeDomains": [...], "editableDomains": [...]} object. An empty body,
    unparseable JSON or a missing field is never treated as an empty list.
    """
    encoded_cloud_id = urllib.parse.quote(callback_config.cloud_id, safe="")
    encoded_password = urllib.parse.quote(callback_config.callback_password, safe="")
    endpoint_for_logging = (
        f"{callback_config.callback_host}/netsapiens/callbacks/{encoded_cloud_id}/active-domains"
    )
    path = f"/netsapiens/callbacks/{encoded_cloud_id}/active-domains?password={encoded_password}"

    callback_client = ApiClient(callback_config.callback_host, timeout_seconds=_ACTIVE_DOMAINS_TIMEOUT_SECONDS)
    print(f"Fetching active domains from {endpoint_for_logging}...")

    success, response = callback_client.get_with_status(path)
    if not success:
        raise RuntimeError(f"Failed to fetch active domains from {endpoint_for_logging}")
    if response is None:
        raise RuntimeError(
            f"Active domains response from {endpoint_for_logging} has an empty or unparseable body"
        )

    active_domains = parse_active_domains(response)
    print(
        f"Loaded active domains from Messaging API: "
        f"activeDomains count={len(active_domains.active_domains)} "
        f"{_format_list_preview(active_domains.active_domains)}, "
        f"editableDomains count={len(active_domains.editable_domains)} "
        f"{_format_list_preview(active_domains.editable_domains)}"
    )
    return active_domains


def parse_active_domains(data: Any) -> ActiveDomains:
    """Validate the active-domains payload and return deduplicated domain lists."""
    if not isinstance(data, dict):
        raise RuntimeError(f"Expected active domains object, got {type(data).__name__}")
    return ActiveDomains(
        active_domains=_parse_domain_name_list(data, "activeDomains"),
        editable_domains=_parse_domain_name_list(data, "editableDomains"),
    )


def _parse_domain_name_list(data: dict, field_name: str) -> list[str]:
    if field_name not in data:
        raise RuntimeError(f"Missing '{field_name}' in active domains response")
    values = data[field_name]
    if not isinstance(values, list):
        raise RuntimeError(f"Expected '{field_name}' to be a list, got {type(values).__name__}")
    for index, value in enumerate(values):
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Invalid domain name at '{field_name}'[{index}]: {value!r}")
    return list(dict.fromkeys(values))


def log_domain_selection(callback_config: CallbackConfig, selection: DomainSelection) -> None:
    print(
        f"[DOMAIN_SELECTION] source=messaging-api "
        f"active={_format_list_preview(selection.active_domains)} "
        f"editable={_format_list_preview(selection.editable_domains)} "
        f"selected count={len(selection.selected_domain_names)} {_format_list_preview(selection.selected_domain_names)} "
        f"missing={_format_list_preview(selection.missing_active_domains)}"
    )
    if selection.missing_active_domains:
        send_script_log(
            callback_config=callback_config,
            level="WARN",
            message=(
                f"Active domains from Messaging are not known to NetSapiens, no subscriptions are created for them: "
                f"count={len(selection.missing_active_domains)} {_format_list_preview(selection.missing_active_domains)}"
            ),
        )

# ===============================================
# Failing Subscriptions Recreation
# ===============================================

# Defensive pause between the delete and the create call on NetSapiens for a
# recreated failing subscription, giving NS time to release the deleted
# subscription before the new one is registered for the same (domain, model).
_FAILING_SUB_DELETE_TO_CREATE_DELAY_SECONDS = 5.0


def fetch_failing_subscriptions(callback_config: CallbackConfig) -> Optional[list[FailingSubscription]]:
    """
    Fetch the list of failing subscriptions tracked on the callback host.

    GET to: {callback_host}/netsapiens/callbacks/{cloud_id}/failing-subscriptions?password={callback_password}

    Returns the parsed list on success, None on transport/HTTP failure.
    """
    encoded_password = urllib.parse.quote(callback_config.callback_password, safe="")
    path = (
        f"/netsapiens/callbacks/{callback_config.cloud_id}/failing-subscriptions"
        f"?password={encoded_password}"
    )

    callback_client = ApiClient(callback_config.callback_host)
    print(
        f"Fetching failing subscriptions from "
        f"{callback_config.callback_host}/netsapiens/callbacks/{callback_config.cloud_id}/failing-subscriptions..."
    )

    success, response = callback_client.get_with_status(path)
    if not success:
        print("Failed to fetch failing subscriptions")
        return None

    if response is None:
        return []

    if not isinstance(response, list):
        raise TypeError(f"Expected list of failing subscriptions, got {type(response).__name__}")

    return [FailingSubscription.from_dict(item) for item in response]


def mark_failing_subscription_deleted(
    callback_config: CallbackConfig,
    subscription_id: str
) -> bool:
    """
    Mark a single failing subscription record as deleted on the callback host.

    DELETE to: {callback_host}/netsapiens/callbacks/{cloud_id}/failing-subscriptions
                ?subscriptionId={subscription_id}&password={callback_password}

    The server performs a soft delete (sets the deleted flag) of the matching row.
    """
    encoded_password = urllib.parse.quote(callback_config.callback_password, safe="")
    encoded_subscription_id = urllib.parse.quote(subscription_id, safe="")
    path = (
        f"/netsapiens/callbacks/{callback_config.cloud_id}/failing-subscriptions"
        f"?subscriptionId={encoded_subscription_id}&password={encoded_password}"
    )

    callback_client = ApiClient(callback_config.callback_host)
    success, _ = callback_client.delete_with_status(path)
    if success:
        print(f"Failing subscription record {subscription_id} marked as deleted on callback host")
    else:
        print(f"Error: Failed to mark failing subscription {subscription_id} as deleted on callback host")
    return success


def recreate_failing_subscriptions(
    client: ApiClient,
    config: Config,
    selection: DomainSelection,
    domain_to_reseller: dict[str, str],
    current_subscriptions: list[Subscription],
    user_scope: str,
    subscription_expires: str,
) -> None:
    """
    Fetch failing subscriptions from the callback host and recreate each on NetSapiens.

    For every failing record returned by the callback host this function tries to:
      1. Delete the existing subscription from NetSapiens (looked up by subscription id
         in the previously fetched current NS subscriptions).
      2. Create a new subscription for the same (domain, model) using the existing helper,
         only when the subscription's domain is selected in this run. A subscription of a
         domain outside the selection is removed without replacement.
      3. Mark the failing subscription record on the callback host as soft-deleted.

    A successful recreation emits an INFO log to the subscriptions-script-log endpoint;
    per-record failures are caught, logged as ERROR, and do not stop processing of
    remaining records.
    """
    callback_config = callback_config_from_config(config)

    failing_subscriptions = fetch_failing_subscriptions(callback_config)
    if failing_subscriptions is None:
        send_script_log(
            callback_config=callback_config,
            level="ERROR",
            message=(
                f"Failed to fetch failing subscriptions for cloudId={config.cloud_id} "
                f"from callback host {config.callback_host}"
            ),
        )
        return

    print(f"Found {len(failing_subscriptions)} failing subscriptions on callback host")
    if not failing_subscriptions:
        return

    subscriptions_by_id = {subscription.id: subscription for subscription in current_subscriptions}

    for failing_subscription in failing_subscriptions:
        try:
            _recreate_failing_subscription(
                client=client,
                config=config,
                selection=selection,
                callback_config=callback_config,
                subscriptions_by_id=subscriptions_by_id,
                domain_to_reseller=domain_to_reseller,
                failing_subscription=failing_subscription,
                user_scope=user_scope,
                subscription_expires=subscription_expires,
            )
        except Exception as recreate_error:
            stack_trace = _format_exception_stack_trace(recreate_error)
            print(
                f"[FAILING_SUB_RECREATE] Failed to recreate failing subscription "
                f"subscriptionId={failing_subscription.subscription_id}, "
                f"domain={failing_subscription.domain}: {recreate_error}"
            )
            print(stack_trace)
            send_script_log(
                callback_config=callback_config,
                level="ERROR",
                message=(
                    f"Failed to recreate failing subscription "
                    f"subscriptionId={failing_subscription.subscription_id}, "
                    f"domain={failing_subscription.domain}: {recreate_error}"
                ),
                callstack=stack_trace,
            )


def _recreate_failing_subscription(
    client: ApiClient,
    config: Config,
    selection: DomainSelection,
    callback_config: CallbackConfig,
    subscriptions_by_id: dict[str, Subscription],
    domain_to_reseller: dict[str, str],
    failing_subscription: FailingSubscription,
    user_scope: str,
    subscription_expires: str,
) -> None:
    """
    Recreate a single failing subscription on NetSapiens and mark the failing record as deleted.

    If the underlying NS subscription cannot be located (e.g., already deleted by a previous
    cleanup), a WARN is logged and the failing record is left untouched so an operator
    can investigate.

    Once the old subscription has been successfully deleted from NetSapiens the failing
    record is always marked as deleted on the callback host, even if the subsequent
    create call fails. After the delete the old subscription id no longer exists on
    NetSapiens, so the failing record is permanently stale; leaving it in place would
    cause it to be retried indefinitely on every subsequent script run without any
    possibility of being resolved. APPLY_SUBSCRIPTION_CHANGES, which runs right after
    this stage on a freshly re-fetched subscriptions snapshot, will pick up the
    missing (domain, model) and create a replacement subscription if needed.
    """
    existing_subscription = subscriptions_by_id.get(failing_subscription.subscription_id)
    if existing_subscription is None:
        print(
            f"[FAILING_SUB_RECREATE] Skipping recreation: NS subscription "
            f"{failing_subscription.subscription_id} not found in current NetSapiens snapshot "
            f"(domain={failing_subscription.domain})"
        )
        send_script_log(
            callback_config=callback_config,
            level="WARN",
            message=(
                f"Cannot recreate failing subscription "
                f"subscriptionId={failing_subscription.subscription_id} "
                f"(domain={failing_subscription.domain}): "
                f"NS subscription not found in current snapshot, model cannot be determined"
            ),
        )
        return

    if not selection.is_selected(existing_subscription.domain):
        _remove_failing_subscription_of_unselected_domain(
            client, callback_config, failing_subscription, existing_subscription
        )
        return

    reseller = domain_to_reseller.get(existing_subscription.domain)
    if not reseller:
        raise RuntimeError(
            f"Cannot determine reseller for domain={existing_subscription.domain} "
            f"while recreating failing subscription {failing_subscription.subscription_id}"
        )

    if not delete_subscription(client, existing_subscription.id):
        raise RuntimeError(
            f"Failed to delete subscription {existing_subscription.id} from NetSapiens "
            f"(domain={existing_subscription.domain}, model={existing_subscription.model.value})"
        )

    try:
        time.sleep(_FAILING_SUB_DELETE_TO_CREATE_DELAY_SECONDS)

        new_subscription_id = create_subscription(
            client=client,
            model=existing_subscription.model,
            domain=existing_subscription.domain,
            reseller=reseller,
            user_scope=user_scope,
            subscription_expires=subscription_expires,
            config=config,
            selection=selection,
            current_active_server=existing_subscription.current_active_server,
        )
        if new_subscription_id is None:
            raise RuntimeError(
                f"Failed to create new subscription for domain={existing_subscription.domain}, "
                f"model={existing_subscription.model.value} "
                f"after deleting old subscription {existing_subscription.id}"
            )

        print(
            f"Recreated failing subscription: oldId={failing_subscription.subscription_id}, "
            f"newId={new_subscription_id}, domain={existing_subscription.domain}, "
            f"model={existing_subscription.model.value}"
        )
        send_script_log(
            callback_config=callback_config,
            level="INFO",
            message=(
                f"Recreated failing subscription: oldSubscriptionId={failing_subscription.subscription_id}, "
                f"newSubscriptionId={new_subscription_id}, domain={existing_subscription.domain}, "
                f"model={existing_subscription.model.value}"
            ),
        )
    finally:
        _mark_failing_subscription_deleted_after_ns_delete(callback_config, failing_subscription, existing_subscription)


def _remove_failing_subscription_of_unselected_domain(
    client: ApiClient,
    callback_config: CallbackConfig,
    failing_subscription: FailingSubscription,
    existing_subscription: Subscription,
) -> None:
    """
    Delete a failing subscription whose domain is not active in Messaging, without
    creating a replacement that the following cleanup would delete again. The failing
    record is marked as deleted only after the NetSapiens delete succeeded.
    """
    if not delete_subscription(client, existing_subscription.id):
        raise RuntimeError(
            f"Failed to delete failing subscription {existing_subscription.id} of inactive domain "
            f"(domain={existing_subscription.domain}, model={existing_subscription.model.value})"
        )

    print(
        f"Removed failing subscription of inactive domain without replacement: "
        f"id={existing_subscription.id}, domain={existing_subscription.domain}, "
        f"model={existing_subscription.model.value}"
    )
    send_script_log(
        callback_config=callback_config,
        level="INFO",
        message=(
            f"Removed failing subscription of inactive domain without replacement: "
            f"subscriptionId={existing_subscription.id}, domain={existing_subscription.domain}, "
            f"model={existing_subscription.model.value}"
        ),
    )
    _mark_failing_subscription_deleted_after_ns_delete(callback_config, failing_subscription, existing_subscription)


def _mark_failing_subscription_deleted_after_ns_delete(
    callback_config: CallbackConfig,
    failing_subscription: FailingSubscription,
    existing_subscription: Subscription,
) -> None:
    if not mark_failing_subscription_deleted(callback_config, failing_subscription.subscription_id):
        send_script_log(
            callback_config=callback_config,
            level="WARN",
            message=(
                f"Failed to mark failing record as deleted on callback host after deleting NS subscription: "
                f"oldSubscriptionId={failing_subscription.subscription_id}, "
                f"domain={existing_subscription.domain}, "
                f"model={existing_subscription.model.value}"
            ),
        )


# ===============================================
# Paginated API fetch helper
# ===============================================

T = TypeVar('T')


def fetch_paginated(
    client: ApiClient,
    endpoint: str,
    extractor: Callable[[Any], list[T]],
    item_name: str,
    page_size: int = 500
) -> list[T]:
    """
    Generic paginated fetch for NetSapiens API endpoints.
    
    Args:
        client: The authenticated API client
        endpoint: The API endpoint path (without query params)
        extractor: Function to extract items from API response
        item_name: Name of items for logging (e.g., "domains", "subscriptions")
        page_size: Number of items per page (default 500)
        
    Returns:
        List of all items across all pages
        
    Raises:
        RuntimeError: If the first page request fails
    """
    all_items: list[T] = []
    start = 0
    
    while True:
        response = client.get(f"{endpoint}?start={start}&limit={page_size}")
        
        if response is None:
            raise RuntimeError(f"Failed to fetch {item_name}: No response (start={start})")
        
        page_items = extractor(response)
        all_items.extend(page_items)
        
        if len(page_items) < page_size:
            break
        
        start += page_size
        print(f"  Fetched {len(all_items)} {item_name} so far...")
    
    return all_items


# ===============================================
# Domain list retrieval
# ===============================================

def get_domain_list(client: ApiClient) -> list[DomainInfo]:
    """Get the list of domains from the NetSapiens API with pagination."""
    print("Fetching domain list...")
    domains = fetch_paginated(
        client=client,
        endpoint="/ns-api/v2/domains",
        extractor=extract_domain_list,
        item_name="domains"
    )
    print(f"Found {len(domains)} domains")
    return domains


def fetch_subscriptions(
    client: ApiClient,
    callback_host: Optional[str] = None
) -> Optional[list[Subscription]]:
    """
    Fetch subscriptions from the NetSapiens API with pagination.
    
    Args:
        client: The authenticated API client
        callback_host: If provided, filter subscriptions to only those targeting this callback host
        
    Returns:
        List of subscriptions, or None if the API request failed
    """
    try:
        subscriptions = fetch_paginated(
            client=client,
            endpoint="/ns-api/v2/subscriptions",
            extractor=extract_subscriptions_from_api_response,
            item_name="subscriptions"
        )
    except RuntimeError:
        return None
    
    if callback_host:
        return [sub for sub in subscriptions if _post_url_matches_callback_host(sub.post_url, callback_host)]
    return subscriptions

# ===============================================
# Authentication
# ===============================================

def _fetch_oauth_token(ns_api_host: str, oauth_config: OAuthConfig) -> tuple[str, str]:
    """
    Fetch an OAuth token from the NetSapiens API.

    Uses a plain ApiClient without a token_refresher to avoid recursion during refresh.

    Returns (access_token, user_scope).
    Raises RuntimeError if the request fails or required fields are missing.
    """
    token_client = ApiClient(ns_api_host)
    response = token_client.post_json(
        "/ns-api/v2/tokens",
        body={
            "grant_type": "password",
            "username": oauth_config.username,
            "password": oauth_config.password,
            "client_id": oauth_config.client_id,
            "client_secret": oauth_config.client_secret,
        },
    )

    if not response:
        raise RuntimeError(f"Failed to fetch OAuth token from {ns_api_host}: Empty response from token request")

    access_token = json_value(response, "access_token")
    user_scope = json_value(response, "scope")

    if not access_token:
        raise RuntimeError(f"Failed to fetch OAuth token from {ns_api_host}: access_token not found in response: {response}")
    if not user_scope:
        raise RuntimeError(f"Failed to fetch OAuth token from {ns_api_host}: scope not found in response: {response}")

    return access_token, user_scope


def get_authenticated_api_client(
    config: Config,
    oauth_config: OAuthConfig,
    context: Optional[ProcessingContext] = None,
) -> AuthResult:
    """
    Create an authenticated API client for the NetSapiens API.
    
    Returns an AuthResult with the access token set on the client and the user scope extracted.
    The client is configured with automatic token refresh on 401 responses.
    Raises RuntimeError if authentication fails.
    """
    set_stage(context, ProcessingStage.AUTH_FETCH_OAUTH_TOKEN)

    print("Fetching OAuth token...")
    access_token, user_scope = _fetch_oauth_token(config.ns_api_host, oauth_config)
    print("Access token set successfully")

    def refresh_token() -> str:
        token, _ = _fetch_oauth_token(config.ns_api_host, oauth_config)
        return token

    client = ApiClient(config.ns_api_host, access_token=access_token, token_refresher=refresh_token)

    return AuthResult(client=client, user_scope=user_scope)

# ===============================================
# Subscription Purge
# ===============================================

# Path of post-urls created by this script: /netsapiens/callbacks/{cloud_id}/{message|message-session}.
# Domain subscriptions created by Messaging (/netsapiens/callbacks/{cloud_id}/domain/{domain}/...)
# deliberately do not match.
_SCRIPT_POST_URL_PATH_PATTERN = re.compile(r"^/netsapiens/callbacks/([^/]+)/(message|message-session)$")


@dataclass
class PurgeResult:
    matched_subscriptions: list[Subscription]
    failed_subscription_ids: list[str]


def _script_post_url_cloud_id(post_url: str) -> Optional[str]:
    match = _SCRIPT_POST_URL_PATH_PATTERN.match(urllib.parse.urlparse(post_url).path)
    return match.group(1) if match else None


def is_purgeable_subscription(subscription: Subscription, callback_host: str) -> bool:
    """Return True for subscriptions of our callback host that follow the script's post-url pattern, for any cloud id."""
    return (
        _post_url_matches_callback_host(subscription.post_url, callback_host)
        and _script_post_url_cloud_id(subscription.post_url) is not None
    )


def _post_url_without_query(post_url: str) -> str:
    # The query carries the callback password of the subscription's cloud id, which may belong to another app.
    parsed = urllib.parse.urlparse(post_url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _print_purge_candidates(subscriptions: list[Subscription]) -> None:
    for subscription in subscriptions:
        print(
            f"  id={subscription.id} domain={subscription.domain} model={subscription.model.value} "
            f"cloudId={_script_post_url_cloud_id(subscription.post_url)} "
            f"post-url={_post_url_without_query(subscription.post_url)}"
        )


def purge_subscriptions(context: ProcessingContext, execute: bool) -> PurgeResult:
    """
    Delete every subscription on our callback host whose post-url follows the script's
    pattern, regardless of cloud id. Without `execute` only lists what would be deleted.

    Active domains are not loaded and nothing is created, updated or reported.
    Raises RuntimeError after all deletes were attempted if any of them failed.
    """
    set_stage(context, ProcessingStage.LOAD_CONFIG)
    config, oauth_config = load_all_config_from_env()

    auth = get_authenticated_api_client(config=config, oauth_config=oauth_config, context=context)

    set_stage(context, ProcessingStage.FETCH_SUBSCRIPTIONS)
    print("\nFetching subscriptions...")
    subscriptions = fetch_subscriptions(auth.client)
    if subscriptions is None:
        raise RuntimeError("Failed to fetch subscriptions: Empty response")

    matched = [
        subscription for subscription in subscriptions
        if is_purgeable_subscription(subscription, config.callback_host)
    ]
    print(
        f"Found {len(matched)} of {len(subscriptions)} subscriptions matching "
        f"{config.callback_host}/netsapiens/callbacks/{{cloudId}}/(message|message-session):"
    )
    _print_purge_candidates(matched)

    if not execute:
        print("Dry run: no subscription was deleted. Add --execute to delete them.")
        return PurgeResult(matched_subscriptions=matched, failed_subscription_ids=[])

    set_stage(context, ProcessingStage.PURGE_SUBSCRIPTIONS)
    failed_ids = [
        subscription.id for subscription in matched
        if not delete_subscription(auth.client, subscription.id)
    ]
    _report_purge_result(config, matched, failed_ids)
    if failed_ids:
        raise RuntimeError(
            f"Failed to delete {len(failed_ids)} of {len(matched)} subscriptions: {_format_list_preview(failed_ids)}"
        )
    return PurgeResult(matched_subscriptions=matched, failed_subscription_ids=failed_ids)


def _report_purge_result(config: Config, matched: list[Subscription], failed_ids: list[str]) -> None:
    deleted_count = len(matched) - len(failed_ids)
    cloud_ids = sorted({_script_post_url_cloud_id(subscription.post_url) for subscription in matched})
    message = (
        f"Purged subscriptions of callback host {config.callback_host}: "
        f"deleted={deleted_count}, failed={len(failed_ids)}, cloudIds={_format_list_preview(cloud_ids)}"
    )
    print(message)
    send_script_log(
        callback_config=callback_config_from_config(config),
        level="ERROR" if failed_ids else "INFO",
        message=message if not failed_ids else f"{message}, failedIds={_format_list_preview(failed_ids)}",
    )


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NetSapiens message events subscription management")
    parser.add_argument(
        "--purge-subscriptions",
        action="store_true",
        help=(
            "Purge mode: list subscriptions of CALLBACK_HOST whose post-url matches "
            "/netsapiens/callbacks/{cloudId}/(message|message-session) for any cloudId "
            "(dry run unless --execute is given); no regular processing is done"
        ),
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Together with --purge-subscriptions actually delete the listed subscriptions",
    )
    args = parser.parse_args(argv)
    if args.execute and not args.purge_subscriptions:
        parser.error("--execute can only be used together with --purge-subscriptions")
    return args


def describe_run_mode(args: argparse.Namespace) -> str:
    if not args.purge_subscriptions:
        return ""
    return " (purge mode, execute)" if args.execute else " (purge mode, dry run)"


def run(context: ProcessingContext, args: argparse.Namespace) -> None:
    if args.purge_subscriptions:
        purge_subscriptions(context, execute=args.execute)
    else:
        main(context)

# ===============================================
# Main
# ===============================================

def main(context: Optional[ProcessingContext] = None):
    if context is None:
        context = ProcessingContext()
    
    set_stage(context, ProcessingStage.LOAD_CONFIG)
    config, oauth_config = load_all_config_from_env()
    callback_config = callback_config_from_config(config)

    set_stage(context, ProcessingStage.FETCH_ACTIVE_DOMAINS)
    active_domains = fetch_active_domains(callback_config)

    auth = get_authenticated_api_client(config=config, oauth_config=oauth_config, context=context)
    client = auth.client
    user_scope = auth.user_scope

    set_stage(context, ProcessingStage.FETCH_DOMAINS)
    domain_infos = get_domain_list(client)

    # Build a lookup dictionary for domain -> reseller
    domain_to_reseller = {info.domain: info.reseller for info in domain_infos}

    selection = compute_domain_selection(active_domains, domain_infos)
    log_domain_selection(callback_config, selection)

    set_stage(context, ProcessingStage.FETCH_SUBSCRIPTIONS)
    # List all subscriptions
    print("\nFetching subscriptions...")
    subscriptions = fetch_subscriptions(client, callback_host=config.callback_host)
    if subscriptions is None:
        raise RuntimeError("Failed to fetch subscriptions: Empty response")
    print(f"Found {len(subscriptions)} subscriptions for callback host {config.callback_host}:")

    # Calculate expiration date (20 years from now) — used by both the failing
    # subscriptions recreation stage below and the subscription mutation stage later.
    subscription_expires = (datetime.now(timezone.utc) + timedelta(days=365 * 20)).strftime("%Y-%m-%d %H:%M:%S")

    set_stage(context, ProcessingStage.RECREATE_FAILING_SUBSCRIPTIONS)
    print("\nRecreating failing subscriptions reported by callback host...")
    recreate_failing_subscriptions(
        client=client,
        config=config,
        selection=selection,
        domain_to_reseller=domain_to_reseller,
        current_subscriptions=subscriptions,
        user_scope=user_scope,
        subscription_expires=subscription_expires,
    )

    set_stage(context, ProcessingStage.FETCH_SUBSCRIPTIONS)
    print("\nRe-fetching subscriptions after failing subscription recreation...")
    subscriptions = fetch_subscriptions(client, callback_host=config.callback_host)
    if subscriptions is None:
        raise RuntimeError("Failed to re-fetch subscriptions after failing subscription recreation")
    print(f"Found {len(subscriptions)} subscriptions for callback host {config.callback_host} after recreation")

    subs_review = review_subscriptions(
        subscriptions,
        domain_infos,
        config=config,
        selection=selection
    )
    inactive_subs = subs_review.subscriptions_in_inactive_domains
    unknown_subs = subs_review.subscriptions_in_unknown_domains
    invalid_active_server_subs = subs_review.subscriptions_with_invalid_active_server
    outdated_post_url_subs = subs_review.subscriptions_with_outdated_post_url
    print(f"Subscriptions in inactive domains: {_format_subscription_list(inactive_subs)}")
    print(f"Subscriptions in unknown domains: {_format_subscription_list(unknown_subs)}")
    print(f"Subscriptions with invalid active server: {_format_subscription_list(invalid_active_server_subs)}")
    print(f"Subscriptions with outdated post-url path: {_format_subscription_list(outdated_post_url_subs)}")
    print(f"Domains without message subscription: {_format_list_preview(subs_review.domains_without_message_subscription)}")
    print(f"Domains without messagesession subscription: {_format_list_preview(subs_review.domains_without_messagesession_subscription)}")
    print(f"Active domains missing in NetSapiens: {_format_list_preview(subs_review.missing_active_domains)}")

    set_stage(context, ProcessingStage.APPLY_SUBSCRIPTION_CHANGES)

    failed_deletes: list[str] = []
    failed_updates: list[str] = []
    failed_creates: list[str] = []

    # delete subs in inactive domains and unknown domains
    subscriptions_to_delete = subs_review.subscriptions_in_inactive_domains + subs_review.subscriptions_in_unknown_domains
    for subscription in subscriptions_to_delete:
        print(f"Deleting subscription: {subscription.id}")
        if not delete_subscription(client, subscription.id):
            failed_deletes.append(subscription.id)

    # Merge subs that need re-posting (invalid active server OR outdated post-url path)
    # and dedup by id so a sub flagged by both conditions is only updated once.
    subscriptions_to_update: list[Subscription] = []
    seen_update_ids: set[str] = set()
    for subscription in (
        subs_review.subscriptions_with_invalid_active_server
        + subs_review.subscriptions_with_outdated_post_url
    ):
        if subscription.id in seen_update_ids:
            continue
        seen_update_ids.add(subscription.id)
        subscriptions_to_update.append(subscription)

    for subscription in subscriptions_to_update:
        reseller = domain_to_reseller.get(subscription.domain)
        if not reseller:
            print(f"Warning: Could not find reseller for domain {subscription.domain}, skipping subscription update")
            continue
        if not update_subscription(
            client,
            subscription,
            reseller,
            user_scope,
            subscription_expires,
            config,
            selection,
        ):
            failed_updates.append(f"{subscription.id} (domain={subscription.domain}, model={subscription.model.value})")
    
    for domain in subs_review.domains_without_message_subscription:
        reseller = domain_to_reseller.get(domain)
        if not reseller:
            print(f"Warning: Could not find reseller for domain {domain}, skipping message subscription creation")
            continue
        sub_id = create_subscription(
            client,
            SubscriptionModel.MESSAGE,
            domain,
            reseller,
            user_scope,
            subscription_expires,
            config,
            selection,
        )
        if sub_id is None:
            failed_creates.append(f"message for domain={domain}")
    
    for domain in subs_review.domains_without_messagesession_subscription:
        reseller = domain_to_reseller.get(domain)
        if not reseller:
            print(f"Warning: Could not find reseller for domain {domain}, skipping messagesession subscription creation")
            continue
        sub_id = create_subscription(
            client,
            SubscriptionModel.MESSAGE_SESSION,
            domain,
            reseller,
            user_scope,
            subscription_expires,
            config,
            selection,
        )
        if sub_id is None:
            failed_creates.append(f"messagesession for domain={domain}")

    if failed_deletes or failed_updates or failed_creates:
        parts: list[str] = []
        if failed_deletes:
            parts.append(f"delete failures ({len(failed_deletes)}): {_format_list_preview(failed_deletes)}")
        if failed_updates:
            parts.append(f"update failures ({len(failed_updates)}): {_format_list_preview(failed_updates)}")
        if failed_creates:
            parts.append(f"create failures ({len(failed_creates)}): {_format_list_preview(failed_creates)}")
        failure_summary = "; ".join(parts)

        send_script_log(
            callback_config=callback_config,
            level="ERROR",
            message=f"Subscription mutation errors during APPLY_SUBSCRIPTION_CHANGES: {failure_summary}",
        )

    set_stage(context, ProcessingStage.REFETCH_SUBSCRIPTIONS)
    # Re-fetch subscriptions to get the latest state after all create/update/delete operations
    print("\nRe-fetching subscriptions for status report...")
    updated_subscriptions = fetch_subscriptions(client)
    if updated_subscriptions is None:
        raise RuntimeError("Failed to re-fetch subscriptions for status report")
    print(f"Found {len(updated_subscriptions)} total subscriptions")
    
    set_stage(context, ProcessingStage.BUILD_STATUS_REPORT)
    # Build and send status report
    report_timestamp = _get_utc_timestamp()
    report = build_status_report(
        subscriptions=updated_subscriptions,
        domain_infos=domain_infos,
        config=config,
        selection=selection,
        report_timestamp=report_timestamp
    )
    
    set_stage(context, ProcessingStage.SEND_STATUS_REPORT)
    if not send_status_report(report=report, config=config):
        raise RuntimeError("Failed to send status report")

if __name__ == "__main__":
    cli_args = parse_arguments(sys.argv[1:])
    run_mode = describe_run_mode(cli_args)
    ctx = ProcessingContext()
    print(f"[INFO] Subscriptions script started{run_mode}")

    callback_cfg: Optional[CallbackConfig] = None
    try:
        callback_cfg = load_callback_config_from_env()
    except Exception as e:
        print(f"[WARN] Could not load callback config for script logging: {e}")
    
    if callback_cfg is not None:
        try:
            ok = send_script_log(
                callback_config=callback_cfg,
                level="INFO",
                message=f"Subscriptions script started{run_mode}",
            )
            if not ok:
                print("[WARN] Remote start log failed; continuing with local stdout only")
        except Exception as e:
            print(f"[WARN] Remote start log threw exception; continuing: {e}")
    
    try:
        run(ctx, cli_args)
    except KeyboardInterrupt:
        raise
    except Exception as e:
        failure_stage = ctx.stage
        error_code = _infer_error_code(failure_stage)
        stack_trace = _format_exception_stack_trace(e)

        print(
            f"[ERROR] Subscriptions script failed, "
            f"code={error_code.value}, stage={failure_stage.value}, exception_type={type(e).__name__}: {e}"
        )
        print(stack_trace)

        if callback_cfg is None:
            try:
                callback_cfg = load_callback_config_from_env()
            except Exception as cb_e:
                print(f"[WARN] Could not load callback config for remote error logging: {cb_e}")
                callback_cfg = None

        if callback_cfg is not None:
            try:
                ok = send_script_log(
                    callback_config=callback_cfg,
                    level="ERROR",
                    message=(
                        f"Subscriptions script failed, code={error_code.value}, stage={failure_stage.value}: {e}"
                    ),
                    callstack=stack_trace,
                )
                if not ok:
                    print("[WARN] Remote error log failed; see stdout for details")
            except Exception as send_e:
                print(f"[WARN] Remote error log threw exception; see stdout for details: {send_e}")
        else:
            print("[WARN] No callback config available; cannot send remote error log (stdout has details)")

        raise
    else:
        print(f"[INFO] Subscriptions script completed successfully{run_mode}")
        if callback_cfg is not None:
            try:
                ok = send_script_log(
                    callback_config=callback_cfg,
                    level="INFO",
                    message=f"Subscriptions script completed successfully{run_mode}",
                )
                if not ok:
                    print("[WARN] Remote completion log failed")
            except Exception as completion_e:
                print(f"[WARN] Remote completion log threw exception: {completion_e}")
