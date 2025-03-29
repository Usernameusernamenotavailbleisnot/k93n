import asyncio
import json
import time
import random
import re
import os
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn, BarColumn, TextColumn

# =========================================================================
# CONFIG - Configuration settings and constants
# =========================================================================

# Application constants
DEFAULT_PROXY_FILE = "proxies.txt"
DEFAULT_TOKEN_FILE = "tokens.txt"
DEFAULT_REFERRAL_CODES_FILE = "referral_codes.txt"
DEFAULT_REFERRAL_CODE = "PO6NW3"

# API endpoints and URLs
KGEN_BASE_URL = "https://prod-api-backend.kgen.io"
DISCORD_BASE_URL = "https://discord.com/api/v9"
DISCORD_CLIENT_ID = "1178767510775017563"
DISCORD_REDIRECT_URI = "https://prod.devindigg.com/oauth/discord/redirect?host=web"
DISCORD_SCOPE = "identify email connections"
DISCORD_STATE = "https://play.kgen.io/"

# Default headers for API requests
DEFAULT_HEADERS = {
    "accept": "application/json",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "origin": "https://play.kgen.io",
    "priority": "u=1, i",
    "referer": "https://play.kgen.io/",
    "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "source": "website",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
    "dnt": "1",
    "pragma": "no-cache",
    "cache-control": "no-cache"
}

# Discord authentication headers
DISCORD_AUTH_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
    "dnt": "1",
    "pragma": "no-cache",
    "cache-control": "no-cache"
}

# Campaign parameters
CAMPAIGN_PARAMS = {
    "status": "LIVE,PUBLISHED",
    "pageLimit": 12,
    "applicableContinent": "AS",
    "applicableCountry": "IDN",
    "campaignVariants": "FIXED_SLOT_FIXED_REWARD,OPEN_SLOT_DYNAMIC_REWARD,POG_TIME_BOUND_TASKS",
    "excludePrivateCampaigns": "true"
}


def load_file_lines(file_path: str, default_value: Optional[List[str]] = None) -> List[str]:
    """
    Load lines from a file, returning a default value if the file doesn't exist.
    
    Args:
        file_path: Path to the file to read
        default_value: Value to return if file doesn't exist
        
    Returns:
        List of non-empty lines from the file, or default_value if file doesn't exist
    """
    try:
        with open(file_path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        return [] if default_value is None else default_value


def save_json(data: Dict[str, Any], file_path: str) -> bool:
    """
    Save data as a JSON file.
    
    Args:
        data: Dictionary data to save
        file_path: Path where to save the file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    except Exception:
        return False


# =========================================================================
# MODELS - Data models and classes
# =========================================================================

@dataclass
class Campaign:
    """Represents a KGen campaign with its details and tasks."""
    id: str
    title: str
    end_time: str
    status: str
    tasks: List[Dict]
    variant: str
    
    @property
    def sorted_tasks(self) -> List[Dict]:
        """Return tasks sorted by priority if needed"""
        return self.tasks


@dataclass
class AuthTokens:
    """Stores authentication tokens."""
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    id_token: Optional[str] = None
    
    @classmethod
    def from_api_response(cls, response_data: Dict) -> "AuthTokens":
        """Create AuthTokens object from API response."""
        return cls(
            access_token=response_data.get('accessToken'),
            refresh_token=response_data.get('refreshToken'),
            id_token=response_data.get('idToken')
        )
    
    def is_valid(self) -> bool:
        """Check if access token is valid."""
        return self.access_token is not None


# =========================================================================
# LOGGING - Enhanced logging utilities
# =========================================================================

class CustomLogger:
    """Enhanced logger using Rich console for formatted output."""
    
    def __init__(self, console: Console = None):
        """Initialize logger with optional custom console."""
        self.console = console or Console()
        self.last_error = None

    def _get_timestamp(self) -> str:
        """Get current timestamp formatted as [DD/MM/YYYY - HH.MM.SS]."""
        return datetime.now().strftime('[%d/%m/%Y - %H.%M.%S]')
        
    def _clean_error_message(self, message: str) -> str:
        """Clean error message by removing URLs and extra information."""
        # Remove URL parts
        if 'url=' in message:
            message = message.split('url=')[0]
            
        # Remove message= part
        message = message.replace("message='", "").replace("',", "")
        
        # Remove any URLs from the message
        message = re.sub(r'https?://\S+', '', message)
        
        # Clean up extra whitespace and commas
        message = message.replace('  ', ' ').strip(' ,')
        return message

    def error(self, message: str, error_code: str = None, details: str = None):
        """Log an error message with optional error code and details."""
        clean_message = self._clean_error_message(str(message))
        
        # Clean details if provided
        if details:
            details = self._clean_error_message(str(details))
            
        error_sig = f"{clean_message}{error_code}{details}"
        if error_sig == self.last_error:
            return
        
        error_parts = [f"{self._get_timestamp()} ❌ Error: {clean_message}"]
        if error_code:
            error_parts.append(f"Code: {error_code}")
        if details:
            error_parts.append(details)
        
        error_msg = " | ".join(error_parts)
        self.console.print(error_msg, style="red")
        self.last_error = error_sig

    def success(self, message: str):
        """Log a success message."""
        self.console.print(f"{self._get_timestamp()} ✅ {message}", style="green")

    def info(self, message: str):
        """Log an information message."""
        self.console.print(f"{self._get_timestamp()} ℹ️ {message}", style="cyan")

    def warning(self, message: str):
        """Log a warning message."""
        self.console.print(f"{self._get_timestamp()} ⚠️ {message}", style="yellow")


# =========================================================================
# HTTP CLIENT - HTTP client with proxy support
# =========================================================================

class ProxyManager:
    """Manages proxy selection and formatting for HTTP requests."""
    
    def __init__(self, proxy_file: str = "proxies.txt"):
        """Initialize proxy manager with proxy file path."""
        self.proxy_file = proxy_file
        self.proxies = self._load_proxies()
        self.current_proxy = None
        
    def _load_proxies(self) -> List[str]:
        """Load proxies from file."""
        try:
            with open(self.proxy_file, 'r') as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            return []
            
    def format_proxy(self, proxy: str) -> Dict[str, str]:
        """Format proxy string into proxy dictionary for aiohttp."""
        if not proxy:
            return {}
            
        # Remove 'http://' if it exists in the proxy string
        if proxy.startswith('http://'):
            proxy = proxy[7:]
            
        return {
            'http': f'http://{proxy}',
            'https': f'http://{proxy}'
        }
            
    def get_proxy(self) -> Dict[str, str]:
        """Get a randomly selected proxy."""
        if not self.proxies:
            return {}
        self.current_proxy = random.choice(self.proxies)
        return self.format_proxy(self.current_proxy)


class HttpClient:
    """HTTP client for making API requests with consistent error handling."""
    
    def __init__(
        self, 
        logger: CustomLogger,
        session: Optional[aiohttp.ClientSession] = None,
        proxy_manager: Optional[ProxyManager] = None,
        base_url: str = KGEN_BASE_URL
    ):
        """Initialize HTTP client with logger and optional session/proxy."""
        self.logger = logger
        self.session = session
        self.proxy_manager = proxy_manager
        self.base_url = base_url
        self.discord_base_url = DISCORD_BASE_URL
        self.access_token = None
        
    async def ensure_session(self):
        """Ensure an active aiohttp session exists."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)
    
    def set_access_token(self, token: str):
        """Set the access token for authenticated requests."""
        self.access_token = token
    
    async def make_request(
        self, 
        method: str, 
        endpoint: str, 
        is_discord: bool = False,
        skip_base_headers: bool = False,
        retry_count: int = 0,
        max_retries: int = 2,
        allow_already_started: bool = False,  # Parameter baru
        **kwargs
    ) -> Optional[Dict]:
        """Make an HTTP request with error handling and retries."""
        await self.ensure_session()
        
        url = f"{self.discord_base_url if is_discord else self.base_url}{endpoint}"
        
        try:
            # Add proxy to request if available and not already in kwargs
            if self.proxy_manager and 'proxy' not in kwargs:
                proxy_settings = self.proxy_manager.get_proxy()
                if proxy_settings:
                    kwargs['proxy'] = proxy_settings['http']
                    self.logger.info(f"Using proxy: {self.proxy_manager.current_proxy}")
            
            if not is_discord and not skip_base_headers:
                # Use default headers as base
                base_headers = DEFAULT_HEADERS.copy()
                
                # Add authorization if available
                if self.access_token:
                    base_headers["authorization"] = f"Bearer {self.access_token}"
                    
                # Merge with provided headers
                if kwargs.get('headers'):
                    base_headers.update(kwargs['headers'])
                
                kwargs['headers'] = base_headers
            
            async with self.session.request(method, url, **kwargs) as response:
                text = await response.text()
                
                if response.status in (200, 201):
                    return json.loads(text) if text else {}
                    
                # Handle token expiration
                if response.status == 401 and not is_discord and retry_count < max_retries:
                    self.logger.info("Got 401, token may need refresh")
                    return None  # Signal for token refresh
                
                # Check for "already started" error if allow_already_started is True
                if allow_already_started and response.status == 409:
                    try:
                        error_data = json.loads(text)
                        error_code = error_data.get('errCode', '')
                        if error_code == 'AIRDROP_CAMPAIGN_USER_HAS_ALREADY_STARTED':
                            self.logger.info("Campaign already started (detected from HTTP client)")
                            # Return special response to indicate already started
                            return {"already_started": True}
                    except:
                        pass
                        
                # Handle error responses
                try:
                    error_data = json.loads(text)
                    error_message = error_data.get('message') or error_data.get('error', 'Unknown error')
                    error_code = str(response.status)
                    
                    self.logger.error(
                        message=error_message,
                        error_code=error_code,
                        details=f"Status: {response.status}, Response: {text[:200]}"  # Log first 200 chars only
                    )
                except json.JSONDecodeError:
                    self.logger.error(
                        message=f"Failed to parse error response",
                        error_code=str(response.status)
                    )
                    
                response.raise_for_status()
                
        except Exception as e:
            error_msg = str(e)
            if 'url=' in error_msg:
                error_msg = error_msg.split('url=')[0].strip()
                
            self.logger.error(
                message=f"Request failed: {error_msg}",
                error_code='REQUEST_ERROR',
                details=f"URL: {url}"
            )
            
            # Determine if we should retry
            should_retry = isinstance(e, (aiohttp.ClientConnectorError, aiohttp.ServerTimeoutError))
                
            if should_retry and retry_count < max_retries:
                delay = 1 * (retry_count + 1)  # Exponential backoff
                self.logger.info(f"Retrying in {delay}s (attempt {retry_count+1}/{max_retries})")
                await asyncio.sleep(delay)
                return await self.make_request(
                    method, 
                    endpoint, 
                    is_discord,
                    skip_base_headers,
                    retry_count + 1,
                    max_retries,
                    allow_already_started,  # Pass the parameter forward
                    **kwargs
                )
                
            raise
    
    async def close(self):
        """Close the HTTP session if it exists and is open."""
        if self.session and not self.session.closed:
            await self.session.close()


# =========================================================================
# AUTH - Authentication module
# =========================================================================

class AuthManager:
    """Manages authentication with KGen via Discord."""
    
    def __init__(self, http_client: HttpClient, logger: CustomLogger):
        """Initialize auth manager with HTTP client and logger."""
        self.http = http_client
        self.logger = logger
        self.tokens = AuthTokens()
        self.user_id = None
        
    async def login_with_discord(self, discord_token: str, max_retries: int = 3) -> bool:
        """Login to KGen using Discord token with retry mechanism."""
        for retry_count in range(max_retries):
            try:
                # Jika ini bukan percobaan pertama, tampilkan pesan retry
                if retry_count > 0:
                    self.logger.info(f"Mencoba login ulang (percobaan {retry_count+1}/{max_retries})...")
                    # Tambahkan penundaan yang semakin lama untuk setiap percobaan
                    await asyncio.sleep(2 * retry_count)
                
                # 1. Get auth code from Discord
                auth_code = await self._get_discord_auth_code(discord_token)
                if not auth_code:
                    continue  # Coba lagi jika gagal mendapatkan auth code
                    
                await asyncio.sleep(random.uniform(1, 2))
                    
                # 2. Authenticate with KGen using the auth code
                authenticated = await self._authenticate_with_kgen(auth_code)
                if not authenticated:
                    continue  # Coba lagi jika gagal autentikasi
                    
                await asyncio.sleep(random.uniform(1, 2))
                    
                # 3. Get user profile to confirm login
                profile = await self._get_user_profile()
                if not profile:
                    continue  # Coba lagi jika gagal mendapatkan profil
                    
                self.logger.success(f"Successfully logged in. User ID: {self.user_id}")
                return True
                    
            except Exception as e:
                error_msg = str(e)
                # Jika ini adalah percobaan terakhir, log sebagai error
                if retry_count == max_retries - 1:
                    self.logger.error("Login error", details=error_msg)
                else:
                    # Jika masih ada percobaan tersisa, log sebagai warning
                    self.logger.warning(f"Login gagal: {error_msg} (akan coba lagi)")
        
        # Jika semua percobaan gagal
        self.logger.error(f"Gagal login setelah {max_retries} percobaan")
        return False

    async def _get_discord_auth_code(self, discord_token: str, max_retries: int = 2) -> Optional[str]:
        """Get authorization code from Discord OAuth with retry."""
        for retry in range(max_retries + 1):
            try:
                if retry > 0:
                    self.logger.info(f"Mencoba mendapatkan auth code lagi (percobaan {retry}/{max_retries})...")
                    await asyncio.sleep(1 * retry)  # Backoff penundaan
                    
                auth_headers = DISCORD_AUTH_HEADERS.copy()
                auth_headers["Authorization"] = discord_token

                auth_payload = {
                    "permissions": "0",
                    "authorize": True,
                    "integration_type": 0
                }

                params = {
                    "client_id": DISCORD_CLIENT_ID,
                    "response_type": "code",
                    "redirect_uri": DISCORD_REDIRECT_URI,
                    "scope": DISCORD_SCOPE,
                    "state": DISCORD_STATE,
                    "integration_type": "0"
                }

                auth_response = await self.http.make_request(
                    "POST", 
                    "/oauth2/authorize",
                    is_discord=True,
                    headers=auth_headers,
                    params=params,
                    json=auth_payload
                )
                
                if not auth_response:
                    if retry < max_retries:
                        continue
                    self.logger.error("No response from auth request")
                    return None
                    
                if "location" not in auth_response:
                    if retry < max_retries:
                        continue
                    self.logger.error("No location in auth response", details=str(auth_response))
                    return None

                auth_code = auth_response["location"].split("code=")[1].split("&")[0]
                self.logger.info(f"Got authorization code: {auth_code[:10]}...")
                return auth_code
                    
            except Exception as e:
                if retry < max_retries:
                    self.logger.warning(f"Error mendapatkan auth code: {str(e)} (akan coba lagi)")
                    continue
                self.logger.error("Failed to get Discord auth code", details=str(e))
                return None
                
        return None  # Semua percobaan gagal

    async def _authenticate_with_kgen(self, auth_code: str) -> bool:
        """Authenticate with KGen using Discord auth code."""
        try:
            headers = {
                "auth-code": auth_code,
            }
            
            auth_result = await self.http.make_request(
                "POST",
                "/oauth/discord/authenticate",
                params={"source": "website"},
                headers=headers,
                json={},
                allow_redirects=False
            )
            
            if not auth_result:
                self.logger.error("No response from authenticate request")
                return False

            self.tokens = AuthTokens.from_api_response(auth_result)
            
            if not self.tokens.is_valid():
                self.logger.error("No access token in response", details=str(auth_result))
                return False

            # Update HTTP client with access token
            self.http.set_access_token(self.tokens.access_token)
            
            # Save tokens to file
            save_json({
                'accessToken': self.tokens.access_token,
                'refreshToken': self.tokens.refresh_token,
                'idToken': self.tokens.id_token
            }, 'tokens.json')
            
            self.logger.success("Access token received and saved")
            return True
            
        except Exception as e:
            self.logger.error("Authentication failed", details=str(e))
            return False

    async def _get_user_profile(self) -> Optional[Dict]:
        """Get user profile to complete login."""
        try:
            profile = await self.http.make_request(
                "GET", 
                "/users/me/profile",
                headers={"if-none-match": 'W/"433-4UOwWtd1SNSEonTFZul6VJ5gsR4"'}
            )
            
            if not profile:
                self.logger.error("No response from profile request")
                return None

            self.user_id = profile.get('userId')
            if not self.user_id:
                self.logger.error("No user ID in profile response", details=str(profile))
                return None

            return profile
            
        except Exception as e:
            self.logger.error("Failed to get user profile", details=str(e))
            return None

    async def refresh_token(self) -> bool:
        """Refresh the access token using the refresh token."""
        try:
            if not self.tokens.refresh_token:
                self.logger.error("No refresh token available")
                return False
                
            headers = {
                "refresh-token": self.tokens.refresh_token,
                "Content-Type": "application/json"
            }
            
            auth_result = await self.http.make_request(
                "POST",
                "/oauth/refresh",
                headers=headers,
                skip_base_headers=True
            )
            
            if auth_result and auth_result.get('accessToken'):
                # Update tokens
                self.tokens.access_token = auth_result['accessToken']
                self.tokens.refresh_token = auth_result.get('refreshToken', self.tokens.refresh_token)
                
                # Update HTTP client with new access token
                self.http.set_access_token(self.tokens.access_token)
                
                # Save updated tokens
                save_json({
                    'accessToken': self.tokens.access_token,
                    'refreshToken': self.tokens.refresh_token,
                    'idToken': self.tokens.id_token
                }, 'tokens.json')
                
                self.logger.success("Token refreshed successfully")
                return True
                
            return False
            
        except Exception as e:
            self.logger.error(f"Token refresh failed: {str(e)}")
            return False


# =========================================================================
# CAMPAIGN MANAGER - Campaign operations
# =========================================================================

class CampaignManager:
    """Manages campaign operations including fetching and completing tasks."""
    
    def __init__(self, http_client: HttpClient, logger: CustomLogger, user_id: str):
        """Initialize campaign manager with HTTP client, logger and user ID."""
        self.http = http_client
        self.logger = logger
        self.user_id = user_id
        self.referral_codes = load_file_lines("referral_codes.txt", [DEFAULT_REFERRAL_CODE])
        
    def get_random_referral_code(self) -> str:
        """Get a random referral code from the loaded list."""
        return random.choice(self.referral_codes)

    async def get_live_campaigns(self) -> List[Campaign]:
        """Get active campaigns from the API."""
        try:
            endpoint = f"/platform-campaign-hub/s2s/airdrop-campaign/user-progress/{self.user_id}/campaigns/by-state"
            response = await self.http.make_request("GET", endpoint, params=CAMPAIGN_PARAMS)
            
            if not response:
                self.logger.error("No response from campaigns request")
                return []
            
            campaigns = []
            for camp in response.get("campaignsWithUserProgress", []):
                info = camp.get("campaignInfo", {})
                if info.get("campaignStatus") == "LIVE":
                    campaigns.append(Campaign(
                        id=info.get("campaignID"),
                        title=info.get("title"),
                        end_time=info.get("campaignTimelineTxt"),
                        status=info.get("campaignStatus"),
                        tasks=info.get("campaignTasks", []),
                        variant=info.get("campaignVariant", "")
                    ))
            
            # Sort campaigns by end time for priority processing
            campaigns.sort(key=lambda x: x.end_time)
            return campaigns

        except Exception as e:
            self.logger.error("Error getting campaigns", details=str(e))
            return []

    async def start_campaign(self, campaign_id: str) -> bool:
        """Start a campaign for the user."""
        try:
            endpoint = f"/platform-campaign-hub/s2s/airdrop-campaign/user-progress/{self.user_id}/campaigns/{campaign_id}/start"
            
            # Gunakan parameter allow_already_started=True untuk mendeteksi error tersebut di HTTP client
            response = await self.http.make_request("POST", endpoint, json={}, allow_already_started=True)
            
            # Cek apakah campaign sudah dimulai
            if response and response.get('already_started', False):
                self.logger.info(f"Campaign already started: {campaign_id}, proceeding with tasks")
                return True
                
            # Jika response valid dan bukan error
            if response:
                self.logger.success(f"Started campaign: {campaign_id}")
                return True
                
            return False
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # Perbaikan pengecekan kata kunci di pesan error
            if ("already started" in error_msg or 
                "has_already_started" in error_msg or 
                "airdrop_campaign_user_has_already_started" in error_msg):
                self.logger.info(f"Campaign already started (detected from exception): {campaign_id}")
                return True
                
            self.logger.error(f"Error starting campaign {campaign_id}", 
                            error_code=getattr(e, 'status', 'UNKNOWN'),
                            details=str(e))
            return False

    async def get_campaign_progress(self, campaign_id: str) -> Dict:
        """Get current progress for a campaign."""
        try:
            endpoint = f"/platform-campaign-hub/s2s/airdrop-campaign/user-progress/{self.user_id}/campaigns/{campaign_id}"
            params = {
                "applicableContinent": "AS",
                "applicableCountry": "IDN"
            }
            return await self.http.make_request("GET", endpoint, params=params) or {}
        except Exception as e:
            self.logger.error(f"Error checking campaign progress", details=str(e))
            return {}

    def _prepare_task_payload(self, task: Dict) -> Dict:
        """Prepare payload for task completion based on task type and title."""
        task_type = task.get("taskType", "")
        task_title = task.get("title", "").lower()
        
        # Default empty payload
        payload = {}
        
        # Referral tasks
        if task_type == "CREATE_REFERRAL" or "referral" in task_title:
            payload = {"referralCode": self.get_random_referral_code()}
            
        # Duper tasks - generate random data based on task requirements
        elif "duper" in task_title:
            if "email" in task_title:
                payload = {"email": f"user{random.randint(1000,9999)}@gmail.com"}
            elif "account" in task_title:
                payload = {"username": f"user{random.randint(1000,9999)}"}
            elif "referral" in task_title:
                payload = {"referralCode": self.get_random_referral_code()}
                
        return payload

    async def complete_task(self, campaign_id: str, task: Dict) -> bool:
        """Attempt to complete a campaign task."""
        try:
            task_id = task["taskID"]
            task_type = task["taskType"]
            task_title = task["title"]
            
            # Check deadline
            current_time = datetime.now(timezone.utc)
            if "taskDeadline" in task:
                task_deadline = datetime.fromisoformat(task["taskDeadline"].replace('Z', '+00:00'))
                if current_time > task_deadline:
                    self.logger.info(f"Skipping task (deadline exceeded): {task_title}")
                    return True
            
            # Prepare endpoint and payload
            endpoint = f"/platform-campaign-hub/s2s/airdrop-campaign/user-progress/{self.user_id}/campaigns/{campaign_id}/tasks/{task_id}/validate"
            payload = self._prepare_task_payload(task)
            
            # Add slight delay before request for rate limiting
            await asyncio.sleep(random.uniform(1, 2))
            
            # Make request
            try:
                response = await self.http.make_request(
                    "POST", 
                    endpoint, 
                    json=payload
                )
                
                if isinstance(response, dict) and response.get("success", False):
                    self.logger.success(f"Completed task: {task_title}")
                    return True
                
                if isinstance(response, dict):
                    error_reason = response.get("error") or response.get("failureReason", "Unknown error")
                    error_code = response.get("errCode") or response.get("errorCode", "NO_CODE")
                    self.logger.error(
                        message=error_reason,
                        error_code=error_code
                    )
                    return False
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # Handle specific error cases with custom handling
                if "task deadline exceeded" in error_msg:
                    self.logger.info(f"Task deadline exceeded (skipping): {task_title}")
                    return True
                elif "wallet account linking is not active" in error_msg:
                    self.logger.error(message="Wallet account linking not active", error_code="WALLET_NOT_ACTIVE")
                    return False
                
                # Generic error handling
                self.logger.error(message=f"Task completion error: {error_msg}")
                return False
            
            return False
            
        except Exception as e:
            self.logger.error(f"Task completion error: {str(e)}")
            return False

    async def get_completed_tasks(self, campaign_id: str) -> Set[str]:
        """Get the set of completed task IDs for a campaign."""
        progress = await self.get_campaign_progress(campaign_id)
        completed_tasks = set()
        
        progress_details = progress.get('userCampaignProgressInfo', {}).get('progressDetails', [])
        for detail in progress_details:
            if detail.get('userCampaignTaskProgressState') == 'VALIDATED':
                completed_tasks.add(detail.get('taskID'))
                
        return completed_tasks

    async def get_statistics(self) -> Dict:
        """Get user statistics for all campaigns."""
        try:
            endpoint = f"/platform-campaign-hub/s2s/airdrop-campaign/user-progress/{self.user_id}/total-kpoints-earned"
            stats = await self.http.make_request("GET", endpoint)
            
            if stats:
                stats_text = (
                    f"Current statistics:\n"
                    f"Total task rewards: {stats.get('totalTaskRewardsEarned', 0)} {stats.get('taskRewardType', '')}\n"
                    f"Total referral rewards: {stats.get('totalReferralRewardsEarned', 0)} {stats.get('referralRewardType', '')}\n"
                    f"Participation count: {stats.get('participationCount', 0)}"
                )
                self.logger.info(stats_text)
            return stats or {}
            
        except Exception as e:
            self.logger.error("Error getting statistics", details=str(e))
            return {}


# =========================================================================
# MAIN BOT - Main bot class
# =========================================================================

class KGenBot:
    """Main bot class that orchestrates the entire process."""
    
    def __init__(self, token_file: str = DEFAULT_TOKEN_FILE, proxy_file: str = DEFAULT_PROXY_FILE):
        """Initialize the bot with token and proxy file paths."""
        self.console = Console()
        self.logger = CustomLogger(self.console)
        self.tokens = load_file_lines(token_file)
        self.proxy_manager = ProxyManager(proxy_file)
        self.http_client = None
        self.auth_manager = None
        self.campaign_manager = None
        
    async def process_account(self, discord_token: str) -> bool:
        """Process a single account with the given Discord token."""
        try:
            # Setup HTTP client with proxy support
            self.http_client = HttpClient(self.logger, proxy_manager=self.proxy_manager)
            
            # Setup authentication manager
            self.auth_manager = AuthManager(self.http_client, self.logger)
            
            # Login to account with max 3 kali retry
            if not await self.auth_manager.login_with_discord(discord_token, max_retries=3):
                self.logger.error("Failed to login with Discord token after retries")
                return False
                
            await asyncio.sleep(random.uniform(2, 4))
            
            # Setup campaign manager
            self.campaign_manager = CampaignManager(
                self.http_client, 
                self.logger,
                self.auth_manager.user_id
            )
            
            # Get active campaigns
            campaigns = await self.campaign_manager.get_live_campaigns()
            if not campaigns:
                self.logger.info("No active campaigns found")
                return True
                
            # Process each campaign
            await self._process_campaigns(campaigns)
            
            # Show final statistics
            await self.campaign_manager.get_statistics()
            
            return True
            
        except Exception as e:
            self.logger.error("Account processing error", details=str(e))
            return False
            
        finally:
            # Clean up HTTP client session
            if self.http_client:
                await self.http_client.close()

    async def _process_campaigns(self, campaigns):
        """Process all campaigns with progress tracking."""
        with Progress(
            SpinnerColumn(),
            TextColumn("[cyan]{task.description}"),
            BarColumn(complete_style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console
        ) as progress:
            
            for campaign in campaigns:
                try:
                    # Setup progress tracking
                    campaign_task = progress.add_task(
                        f"Campaign: {campaign.title}",
                        total=len(campaign.tasks)
                    )
                    
                    # Start campaign
                    if not await self.campaign_manager.start_campaign(campaign.id):
                        self.logger.error(f"Failed to start campaign: {campaign.title}")
                        continue
                    
                    self.logger.info(f"Processing tasks for: {campaign.title}")
                    
                    # Get completed tasks to avoid reprocessing
                    completed_tasks = await self.campaign_manager.get_completed_tasks(campaign.id)
                    
                    # Process tasks
                    for task in campaign.tasks:
                        task_id = task['taskID']
                        
                        # Skip if already completed
                        if task_id in completed_tasks:
                            self.logger.info(f"Task already completed: {task['title']}")
                            progress.advance(campaign_task)
                            continue
                            
                        # Update progress description
                        progress.update(campaign_task, description=f"Task: {task['title']}")
                        
                        # Attempt task completion
                        if await self.campaign_manager.complete_task(campaign.id, task):
                            progress.advance(campaign_task)
                        
                        # Add delay between tasks
                        await asyncio.sleep(random.uniform(2, 4))
                    
                    # Show completion status
                    campaign_progress = await self.campaign_manager.get_campaign_progress(campaign.id)
                    completed_count = campaign_progress.get("userCampaignProgressInfo", {}).get("completedTaskCount", 0)
                    self.logger.info(f"Completed {completed_count}/{len(campaign.tasks)} tasks in {campaign.title}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing campaign {campaign.title}", details=str(e))
                    continue

    async def run(self):
        """Main method to run the bot on all accounts."""
        if not self.tokens:
            self.logger.error("No tokens found in tokens.txt")
            return
            
        try:
            for i, token in enumerate(self.tokens, 1):
                self.logger.info(f"Processing account {i}/{len(self.tokens)}")
                self.logger.info(f"Using token: {token[:20]}...")
                
                await self.process_account(token)
                
                # Add delay between accounts (except for the last one)
                if i < len(self.tokens):
                    delay = random.uniform(5, 10)
                    self.logger.info(f"Waiting {delay:.2f}s before next account...")
                    await asyncio.sleep(delay)
                    
        except KeyboardInterrupt:
            self.logger.warning("Bot stopped by user")
        except Exception as e:
            self.logger.error(f"Bot error", details=str(e))


# =========================================================================
# ENTRY POINT
# =========================================================================

def main():
    """Main entry point for the application."""
    try:
        bot = KGenBot()
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\nProgram terminated by user")
    except Exception as e:
        print(f"Program error: {str(e)}")

if __name__ == "__main__":
    main()
