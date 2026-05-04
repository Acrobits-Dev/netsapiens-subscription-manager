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
"""

import json
import os
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
# Processing Context (for error/script-log reporting)
# ===============================================

class ProcessingStage(Enum):
    """Stages of script execution for error tracking."""
    LOAD_CONFIG = "LOAD_CONFIG"
    AUTH_FETCH_OAUTH_TOKEN = "AUTH_FETCH_OAUTH_TOKEN"
    FETCH_DOMAINS = "FETCH_DOMAINS"
    FETCH_SUBSCRIPTIONS = "FETCH_SUBSCRIPTIONS"
    APPLY_SUBSCRIPTION_CHANGES = "APPLY_SUBSCRIPTION_CHANGES"
    REFETCH_SUBSCRIPTIONS = "REFETCH_SUBSCRIPTIONS"
    BUILD_STATUS_REPORT = "BUILD_STATUS_REPORT"
    SEND_STATUS_REPORT = "SEND_STATUS_REPORT"


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
    editable_version_domain: str
    allowed_domains: list[str]
    disallowed_domains: list[str]


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


def _parse_comma_separated_list(value: str) -> list[str]:
    """Parse a comma-separated string into a list, filtering empty values."""
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


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
    
    Optional environment variables:
        EDITABLE_VERSION_DOMAIN: Domain for editable version (default: "")
        ALLOWED_DOMAINS: Comma-separated list of allowed domains (default: "")
        DISALLOWED_DOMAINS: Comma-separated list of disallowed domains (default: "")
    
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
    
    allowed_domains = _parse_comma_separated_list(os.environ.get("ALLOWED_DOMAINS", ""))
    disallowed_domains = _parse_comma_separated_list(os.environ.get("DISALLOWED_DOMAINS", ""))
    
    if allowed_domains and disallowed_domains:
        raise RuntimeError("ALLOWED_DOMAINS and DISALLOWED_DOMAINS cannot be configured at the same time")
    
    return Config(
        ns_api_host=_normalize_base_url(ns_api_host),
        callback_host=_normalize_base_url(callback_host),
        callback_password=callback_password,
        cloud_id=cloud_id,
        editable_version_domain=os.environ.get("EDITABLE_VERSION_DOMAIN", ""),
        allowed_domains=allowed_domains,
        disallowed_domains=disallowed_domains,
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
    AUTH_TOKEN_RETRIEVAL_FAILURE = "AUTH_TOKEN_RETRIEVAL_FAILURE"
    DOMAIN_RETRIEVAL_FAILURE = "DOMAIN_RETRIEVAL_FAILURE"
    SUBSCRIPTIONS_RETRIEVAL_FAILURE = "SUBSCRIPTIONS_RETRIEVAL_FAILURE"
    SUBSCRIPTION_MUTATION_FAILURE = "SUBSCRIPTION_MUTATION_FAILURE"
    STATUS_REPORT_SEND_FAILURE = "STATUS_REPORT_SEND_FAILURE"
    UNHANDLED_EXCEPTION = "UNHANDLED_EXCEPTION"


def _infer_error_code(stage: ProcessingStage) -> ErrorCode:
    """Map processing stage to appropriate error code."""
    if stage == ProcessingStage.LOAD_CONFIG:
        return ErrorCode.CONFIG_LOAD_FAILURE
    if stage == ProcessingStage.AUTH_FETCH_OAUTH_TOKEN:
        return ErrorCode.AUTH_TOKEN_RETRIEVAL_FAILURE
    if stage == ProcessingStage.FETCH_DOMAINS:
        return ErrorCode.DOMAIN_RETRIEVAL_FAILURE
    if stage in (ProcessingStage.FETCH_SUBSCRIPTIONS, ProcessingStage.REFETCH_SUBSCRIPTIONS):
        return ErrorCode.SUBSCRIPTIONS_RETRIEVAL_FAILURE
    if stage == ProcessingStage.APPLY_SUBSCRIPTION_CHANGES:
        return ErrorCode.SUBSCRIPTION_MUTATION_FAILURE
    if stage == ProcessingStage.SEND_STATUS_REPORT:
        return ErrorCode.STATUS_REPORT_SEND_FAILURE
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
    ):
        self.base_url = base_url.rstrip("/")
        self.access_token = access_token
        self.ssl_ctx = ssl.create_default_context()
        self._token_refresher = token_refresher
    
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
                with urllib.request.urlopen(request, context=self.ssl_ctx) as response:
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
    
    def get(self, path: str) -> Optional[dict]:
        """Make a GET request with automatic authentication."""
        return self._make_request(path, method="GET")
    
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
    
    def delete(self, path: str) -> Optional[dict]:
        """Make a DELETE request with automatic authentication."""
        return self._make_request(path, method="DELETE")
    
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
#     "preferred-server": "https:\/\/sipns.acme.net",
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
    subscriptions_in_disallowed_domains: list[Subscription]
    subscriptions_in_unknown_domains: list[Subscription]
    subscriptions_with_invalid_active_server: list[Subscription]
    domains_without_message_subscription: list[str]
    domains_without_messagesession_subscription: list[str]
    missing_allowed_domains: list[str]

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
    
    - EXCLUDED: Domain is filtered out by configuration (in disallowed_domains list 
      or not in allowed_domains list). No issues are tracked for excluded domains.
    """
    FULLY_COVERED = "FULLY_COVERED"
    COVERED_WITH_ISSUES = "COVERED_WITH_ISSUES"
    PARTIALLY_COVERED = "PARTIALLY_COVERED"
    NOT_COVERED = "NOT_COVERED"
    EXCLUDED = "EXCLUDED"


class ExclusionReason(Enum):
    """Reason why a domain was excluded from coverage tracking."""
    DISALLOWED_DOMAIN = "DISALLOWED_DOMAIN"      # Domain is in disallowed_domains list
    NOT_IN_ALLOWED_LIST = "NOT_IN_ALLOWED_LIST"  # allowed_domains is configured but domain is not in it


class SubscriptionIssue(Enum):
    """
    Issues detected for a domain's subscriptions.
    
    Coverage-related issues:
    - MISSING_MESSAGE_SUBSCRIPTION: No 'message' subscription exists for the domain.
      Present when coverage is PARTIALLY_COVERED, COVERED_WITH_ISSUES (partial), or NOT_COVERED.
    - MISSING_MESSAGESESSION_SUBSCRIPTION: No 'messagesession' subscription exists.
      Present when coverage is PARTIALLY_COVERED, COVERED_WITH_ISSUES (partial), or NOT_COVERED.
    
    Subscription health issues (triggers COVERED_WITH_ISSUES status):
    - INVALID_ACTIVE_SERVER: A subscription exists but has empty current_active_server.
      This indicates the subscription may not be functioning properly.
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
    editable_version_domain: str

    def to_dict(self) -> dict:
        return {
            "ns_api_host": self.ns_api_host,
            "callback_host": self.callback_host,
            "allowed_domains": self.allowed_domains,
            "disallowed_domains": self.disallowed_domains,
            "editable_version_domain": self.editable_version_domain
        }


@dataclass
class StatusReport:
    report_version: int
    report_timestamp: str
    cloud_id: str
    configuration: StatusReportConfiguration
    summary: StatusReportSummary
    domains: list[DomainReport]
    orphaned_subscriptions: list[OrphanedSubscriptionReport]

    def to_dict(self) -> dict:
        return {
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

def _compute_effective_domains(
    allowed_domains: list[str],
    disallowed_domains: list[str],
    editable_version_domain: Optional[str]
) -> tuple[list[str], list[str]]:
    """
    Compute effective allowed and disallowed domain lists.
    
    Returns (effective_allowed, effective_disallowed) with editable_version_domain
    always treated as allowed and never disallowed.
    """
    effective_allowed = allowed_domains.copy()
    effective_disallowed = disallowed_domains.copy()
    
    if editable_version_domain:
        # Add to allowed list if allowed domains are configured and it's not already there
        if effective_allowed and editable_version_domain not in effective_allowed:
            effective_allowed.append(editable_version_domain)
        # Remove from disallowed list
        effective_disallowed = [d for d in effective_disallowed if d != editable_version_domain]
    
    return effective_allowed, effective_disallowed


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
    config: Config
) -> SubscriptionReviewResult:
    """
    Analyze existing subscriptions against domain metadata and configuration.
    
    - Subscriptions not targeting our callback host (config.callback_host) are filtered out.
    - Subscriptions in domains not returned by the API are marked as unknown.
    - Allowed/disallowed domains are mutually exclusive; subscriptions outside the allowed list (or inside the disallowed list) are surfaced.
    - For the remaining subscriptions, missing or empty `current_active_server` is flagged.
    - For each relevant domain, presence of both `message` and `messagesession` subscriptions is verified.
    - `editable_version_domain` from `config` is always treated as eligible for the checks above even if not in allowed domains and never treated as disallowed.
    """
    
    # Filter out subscriptions that do not target our callback host to avoid touching foreign subscriptions.
    # NS API is fragile, in theory, we can still break foreign subscriptions by subscribing to the same domain,
    # but we just assume that that will not be the case.
    subscriptions = [
        subscription
        for subscription in subscriptions
        if _post_url_matches_callback_host(subscription.post_url, config.callback_host)
    ]
    
    allowed_domains = config.allowed_domains
    disallowed_domains = config.disallowed_domains
    editable_version_domain = config.editable_version_domain
    
    # Extract domain names for membership checks
    domain_names = [info.domain for info in domain_infos]
    
    allowed_domains_configured = len(allowed_domains) > 0
    disallowed_domains_configured = len(disallowed_domains) > 0

    missing_allowed_domains: list[str] = []
    effective_allowed_domains, effective_disallowed_domains = _compute_effective_domains(
        allowed_domains, disallowed_domains, editable_version_domain
    )

    # Filter out subscriptions that are in unknown domains, not sure if this can happen, but just in case.
    subscriptions_in_unknown_domains = [
        subscription for subscription in subscriptions if subscription.domain not in domain_names
    ]

    if allowed_domains_configured:
        subscriptions_in_disallowed_domains = [
            subscription
            for subscription in subscriptions
            if subscription.domain in domain_names and subscription.domain not in effective_allowed_domains
        ]
        missing_allowed_domains = [domain for domain in effective_allowed_domains if domain not in domain_names]
    elif disallowed_domains_configured:
        subscriptions_in_disallowed_domains = [
            subscription
            for subscription in subscriptions
            if subscription.domain in domain_names and subscription.domain in effective_disallowed_domains
        ]
    else:
        subscriptions_in_disallowed_domains = []

    excluded_subscription_ids = {
        subscription.id for subscription in (subscriptions_in_disallowed_domains + subscriptions_in_unknown_domains)
    }
    allowed_subscriptions = [
        subscription for subscription in subscriptions if subscription.id not in excluded_subscription_ids
    ]
    subscriptions_with_invalid_active_server = [
        subscription for subscription in allowed_subscriptions if not subscription.current_active_server
    ]
    domains_without_message_subscription: list[str] = []
    domains_without_messagesession_subscription: list[str] = []

    # Filter domain names based on allowed/disallowed configuration
    filtered_domain_names = domain_names
    if allowed_domains_configured:
        filtered_domain_names = [domain for domain in domain_names if domain in effective_allowed_domains]
    elif disallowed_domains_configured:
        filtered_domain_names = [domain for domain in domain_names if domain not in effective_disallowed_domains]
    # ensure editable_version_domain is still checked when not explicitly allowed
    if editable_version_domain and editable_version_domain in domain_names and editable_version_domain not in filtered_domain_names:
        filtered_domain_names.append(editable_version_domain)

    for domain in filtered_domain_names:
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
        subscriptions_in_disallowed_domains=subscriptions_in_disallowed_domains,
        subscriptions_in_unknown_domains=subscriptions_in_unknown_domains,
        subscriptions_with_invalid_active_server=subscriptions_with_invalid_active_server,
        domains_without_message_subscription=domains_without_message_subscription,
        domains_without_messagesession_subscription=domains_without_messagesession_subscription,
        missing_allowed_domains=missing_allowed_domains
    )


# ===============================================
# Status Report Builder
# ===============================================

REPORT_VERSION = 2


def build_status_report(
    subscriptions: list[Subscription],
    domain_infos: list[DomainInfo],
    config: Config,
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
    domain_to_reseller = {info.domain: info.reseller for info in domain_infos}
    
    allowed_domains_configured = len(config.allowed_domains) > 0
    disallowed_domains_configured = len(config.disallowed_domains) > 0
    
    effective_allowed_domains, effective_disallowed_domains = _compute_effective_domains(
        config.allowed_domains, config.disallowed_domains, config.editable_version_domain
    )
    
    # Build domain reports
    domain_reports: list[DomainReport] = []
    orphaned_subscriptions: list[OrphanedSubscriptionReport] = []
    
    # Track counts for summary
    fully_covered_count = 0
    covered_with_issues_count = 0
    partially_covered_count = 0
    uncovered_count = 0
    
    # Compute effective domain list (domains that should be processed)
    effective_domain_names: list[str] = []
    if allowed_domains_configured:
        effective_domain_names = [d for d in domain_names if d in effective_allowed_domains]
    elif disallowed_domains_configured:
        effective_domain_names = [d for d in domain_names if d not in effective_disallowed_domains]
    else:
        effective_domain_names = domain_names.copy()
    
    # Ensure editable_version_domain is included if it exists in API domains
    if config.editable_version_domain and config.editable_version_domain in domain_names:
        if config.editable_version_domain not in effective_domain_names:
            effective_domain_names.append(config.editable_version_domain)
    
    # Process each domain from the API
    for domain_info in domain_infos:
        domain = domain_info.domain
        reseller = domain_info.reseller
        
        # Determine if domain is excluded
        is_excluded = False
        exclusion_reason: Optional[ExclusionReason] = None
        
        if allowed_domains_configured and domain not in effective_allowed_domains:
            is_excluded = True
            exclusion_reason = ExclusionReason.NOT_IN_ALLOWED_LIST
        elif disallowed_domains_configured and domain in effective_disallowed_domains:
            is_excluded = True
            exclusion_reason = ExclusionReason.DISALLOWED_DOMAIN
        
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
            for sub in [message_sub, messagesession_sub]:
                if sub and not sub.current_active_server:
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
    
    # Build configuration
    configuration = StatusReportConfiguration(
        ns_api_host=config.ns_api_host,
        callback_host=config.callback_host,
        allowed_domains=config.allowed_domains,
        disallowed_domains=config.disallowed_domains,
        editable_version_domain=config.editable_version_domain
    )
    
    # Build summary
    summary = StatusReportSummary(
        total_domains_from_api=len(domain_infos),
        effective_domains_count=len(effective_domain_names),
        fully_covered_domains_count=fully_covered_count,
        covered_with_issues_domains_count=covered_with_issues_count,
        partially_covered_domains_count=partially_covered_count,
        uncovered_domains_count=uncovered_count
    )
    
    return StatusReport(
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


# Suffix appended to cloud_id for editable version domain to indicate wildcard matching
EDITABLE_VERSION_CLOUD_ID_SUFFIX = "*"


def _build_subscription_body(
    model: SubscriptionModel,
    domain: str,
    reseller: str,
    user_scope: str,
    subscription_expires: str,
    config: Config
) -> dict:
    """Build the JSON body for subscription create/update requests."""
    path_suffix = model.to_path_suffix()
    encoded_password = urllib.parse.quote(config.callback_password, safe='')
    # Append wildcard suffix for editable version domain to allow broader matching
    is_editable_domain = domain == config.editable_version_domain
    needs_suffix = is_editable_domain and not config.cloud_id.endswith(EDITABLE_VERSION_CLOUD_ID_SUFFIX)
    cloud_id = f"{config.cloud_id}{EDITABLE_VERSION_CLOUD_ID_SUFFIX}" if needs_suffix else config.cloud_id
    post_url = f"{config.callback_host}/netsapiens/callbacks/{cloud_id}/{path_suffix}?password={encoded_password}"
    
    return {
        "post-url": post_url,
        "model": model.value,
        "domain": domain,
        "user": "*",
        "reseller": reseller,
        "user-scope": user_scope,
        "subscription-geo-support": "yes",
        "preferred-server": config.ns_api_host,
        "subscription-expires-datetime": subscription_expires
    }


def create_subscription(
    client: ApiClient,
    model: SubscriptionModel,
    domain: str,
    reseller: str,
    user_scope: str,
    subscription_expires: str,
    config: Config
) -> Optional[str]:
    """Create a subscription and return the subscription ID."""
    json_body = _build_subscription_body(model, domain, reseller, user_scope, subscription_expires, config)
    
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
    else:
        print(f"Error: Empty or failed response from {model.value} subscription request")
    
    return None


def update_subscription(
    client: ApiClient,
    subscription: Subscription,
    reseller: str,
    user_scope: str,
    subscription_expires: str,
    config: Config
) -> None:
    """Update a subscription."""
    json_body = _build_subscription_body(subscription.model, subscription.domain, reseller, user_scope, subscription_expires, config)
    
    print(f"Updating {subscription.model.value} subscription {subscription.id}...")
    
    response = client.put_json(f"/ns-api/v2/subscriptions/{subscription.id}", json_body)
    
    if response:
        print(f"{subscription.model.value} subscription {subscription.id} updated successfully")
    else:
        print(f"Error: Empty or failed response from {subscription.model.value} subscription update request")


def delete_subscription(client: ApiClient, subscription_id: str) -> bool:
    """Delete a subscription. Returns True if the request completed (success or empty response)."""
    response = client.delete(f"/ns-api/v2/subscriptions/{subscription_id}")
    # A None response from _make_request could mean empty success or error (error is already logged)
    print(f"Subscription {subscription_id} deleted successfully")
    return True

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
# Main
# ===============================================

def main(context: Optional[ProcessingContext] = None):
    if context is None:
        context = ProcessingContext()
    
    set_stage(context, ProcessingStage.LOAD_CONFIG)
    config, oauth_config = load_all_config_from_env()
    
    auth = get_authenticated_api_client(config=config, oauth_config=oauth_config, context=context)
    client = auth.client
    user_scope = auth.user_scope
    
    set_stage(context, ProcessingStage.FETCH_DOMAINS)
    domain_infos = get_domain_list(client)
    
    # Build a lookup dictionary for domain -> reseller
    domain_to_reseller = {info.domain: info.reseller for info in domain_infos}

    set_stage(context, ProcessingStage.FETCH_SUBSCRIPTIONS)
    # List all subscriptions
    print("\nFetching subscriptions...")
    subscriptions = fetch_subscriptions(client, callback_host=config.callback_host)
    if subscriptions is None:
        raise RuntimeError("Failed to fetch subscriptions: Empty response")
    print(f"Found {len(subscriptions)} subscriptions for callback host {config.callback_host}:")

    subs_review = review_subscriptions(
        subscriptions,
        domain_infos,
        config=config
    )
    disallowed_subs = subs_review.subscriptions_in_disallowed_domains
    unknown_subs = subs_review.subscriptions_in_unknown_domains
    invalid_active_server_subs = subs_review.subscriptions_with_invalid_active_server
    print(f"Subscriptions in disallowed domains: {_format_subscription_list(disallowed_subs)}")
    print(f"Subscriptions in unknown domains: {_format_subscription_list(unknown_subs)}")
    print(f"Subscriptions with invalid active server: {_format_subscription_list(invalid_active_server_subs)}")
    print(f"Domains without message subscription: {_format_list_preview(subs_review.domains_without_message_subscription)}")
    print(f"Domains without messagesession subscription: {_format_list_preview(subs_review.domains_without_messagesession_subscription)}")
    print(f"Missing allowed domains: {_format_list_preview(subs_review.missing_allowed_domains)}")

    set_stage(context, ProcessingStage.APPLY_SUBSCRIPTION_CHANGES)
    # delete subs in disallowed domains and unknown domains
    subscriptions_to_delete = subs_review.subscriptions_in_disallowed_domains + subs_review.subscriptions_in_unknown_domains
    for subscription in subscriptions_to_delete:
        print(f"Deleting subscription: {subscription.id}")
        delete_subscription(client, subscription.id)
    
    # Calculate expiration date (20 years from now)
    subscription_expires = (datetime.now(timezone.utc) + timedelta(days=365 * 20)).strftime("%Y-%m-%d %H:%M:%S")

    for subscription in subs_review.subscriptions_with_invalid_active_server:
        reseller = domain_to_reseller.get(subscription.domain)
        if not reseller:
            print(f"Warning: Could not find reseller for domain {subscription.domain}, skipping subscription update")
            continue
        update_subscription(
            client,
            subscription,
            reseller,
            user_scope,
            subscription_expires,
            config,
        )
    
    for domain in subs_review.domains_without_message_subscription:
        reseller = domain_to_reseller.get(domain)
        if not reseller:
            print(f"Warning: Could not find reseller for domain {domain}, skipping message subscription creation")
            continue
        create_subscription(
            client,
            SubscriptionModel.MESSAGE,
            domain,
            reseller,
            user_scope,
            subscription_expires,
            config,
        )
    
    for domain in subs_review.domains_without_messagesession_subscription:
        reseller = domain_to_reseller.get(domain)
        if not reseller:
            print(f"Warning: Could not find reseller for domain {domain}, skipping messagesession subscription creation")
            continue
        create_subscription(
            client,
            SubscriptionModel.MESSAGE_SESSION,
            domain,
            reseller,
            user_scope,
            subscription_expires,
            config,
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
        report_timestamp=report_timestamp
    )
    
    set_stage(context, ProcessingStage.SEND_STATUS_REPORT)
    if not send_status_report(report=report, config=config):
        raise RuntimeError("Failed to send status report")

if __name__ == "__main__":
    ctx = ProcessingContext()
    print("[INFO] Subscriptions script started")
    
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
                message="Subscriptions script started",
            )
            if not ok:
                print("[WARN] Remote start log failed; continuing with local stdout only")
        except Exception as e:
            print(f"[WARN] Remote start log threw exception; continuing: {e}")
    
    try:
        main(context=ctx)
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
