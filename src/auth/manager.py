"""
OAuth 2.0 Authentication Manager for Upstox API
Uses database for credential storage - zero config
"""
import json
import time
import webbrowser
from pathlib import Path
from typing import Optional, Dict
from urllib.parse import urlencode, parse_qs, urlparse
import logging
import hashlib
import secrets

from flask import Flask, request, redirect, session
import httpx

from ..config import config
from ..database.manager import DatabaseManager

logger = logging.getLogger(__name__)

class AuthManager:
    """
    Handles Upstox OAuth 2.0 authentication flow
    Credentials stored encrypted in database
    """

    def __init__(self):
        """Initialize authentication manager"""
        self.base_url = config.UPSTOX_BASE_URL
        self.db_manager = DatabaseManager()

        # Load credentials from database
        self._load_credentials()

    def _load_credentials(self) -> bool:
        """Load credentials from database"""
        creds = self.db_manager.get_credentials()

        if creds:
            self.api_key = creds['api_key']
            self.api_secret = creds['api_secret']
            self.redirect_uri = creds['redirect_uri']
            self.access_token = creds['access_token']
            self.token_expiry = creds['token_expiry']
            return True
        else:
            # No credentials yet
            self.api_key = None
            self.api_secret = None
            self.redirect_uri = config.UPSTOX_REDIRECT_URI
            self.access_token = None
            self.token_expiry = None
            return False

    def has_credentials(self) -> bool:
        """Check if API credentials are configured"""
        return bool(self.api_key and self.api_secret)

    def save_credentials(self, api_key: str, api_secret: str, redirect_uri: str = None) -> bool:
        """Save API credentials to database"""
        success = self.db_manager.save_credentials(api_key, api_secret, redirect_uri)

        if success:
            self.api_key = api_key
            self.api_secret = api_secret
            self.redirect_uri = redirect_uri or config.UPSTOX_REDIRECT_URI
            logger.info("Credentials saved to database")

        return success

    def is_token_valid(self) -> bool:
        """Check if current token is valid"""
        if not self.access_token:
            return False

        if self.token_expiry and time.time() >= self.token_expiry:
            return False

        return True

    def get_authorization_url(self) -> str:
        """
        Generate OAuth authorization URL

        Returns:
            Authorization URL for user login
        """
        if not self.has_credentials():
            raise ValueError("API credentials not configured. Please set them first.")

        params = {
            'client_id': self.api_key,
            'redirect_uri': self.redirect_uri,
            'response_type': 'code',
            'state': secrets.token_urlsafe(32)
        }

        auth_url = f"https://api.upstox.com/v2/login/authorization/dialog?{urlencode(params)}"
        return auth_url

    async def exchange_code_for_token(self, auth_code: str) -> bool:
        """
        Exchange authorization code for access token

        Args:
            auth_code: Authorization code from OAuth callback

        Returns:
            True if token exchange successful
        """
        if not self.has_credentials():
            raise ValueError("API credentials not configured")

        url = f"{self.base_url}/login/authorization/token"

        data = {
            'code': auth_code,
            'client_id': self.api_key,
            'client_secret': self.api_secret,
            'redirect_uri': self.redirect_uri,
            'grant_type': 'authorization_code'
        }

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, data=data, headers=headers)

                if response.status_code == 200:
                    token_data = response.json()
                    self.access_token = token_data.get('access_token')

                    # Calculate token expiry (usually 1 day)
                    expires_in = token_data.get('expires_in', 86400)
                    self.token_expiry = time.time() + expires_in - 300  # 5 min buffer

                    # Save token to database
                    self.db_manager.save_token(self.access_token, self.token_expiry)

                    logger.info("Successfully obtained access token")
                    return True
                else:
                    logger.error(f"Token exchange failed: {response.status_code} - {response.text}")
                    return False

        except Exception as e:
            logger.error(f"Error exchanging code for token: {e}")
            return False

    def authenticate(self, open_browser: bool = True) -> bool:
        """
        Start authentication flow

        Args:
            open_browser: Whether to automatically open browser

        Returns:
            True if authentication successful
        """
        # Check if credentials are configured
        if not self.has_credentials():
            print("\n" + "="*50)
            print("API Credentials Required")
            print("="*50)
            print("Please configure your Upstox API credentials first.")
            print("You can do this via:")
            print("1. Web interface: python expirytrack_app.py")
            print("2. CLI: python main.py setup")
            print("="*50)
            return False

        if self.is_token_valid():
            logger.info("Using existing valid token")
            return True

        # Start Flask server for OAuth callback
        app = Flask(__name__)
        app.secret_key = secrets.token_hex(32)

        auth_complete = {'status': False}

        @app.route('/upstox/callback')
        def callback():
            """Handle OAuth callback"""
            auth_code = request.args.get('code')
            error = request.args.get('error')

            if error:
                auth_complete['status'] = False
                return f"Authentication failed: {error}", 400

            if auth_code:
                # Run async function in sync context
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                success = loop.run_until_complete(
                    self.exchange_code_for_token(auth_code)
                )
                loop.close()

                auth_complete['status'] = success
                if success:
                    return """
                    <html>
                    <head><title>Authentication Successful</title></head>
                    <body style="font-family: Arial; text-align: center; padding: 50px;">
                        <h1>Authentication Successful!</h1>
                        <p>You can close this window and return to ExpiryTrack.</p>
                        <script>setTimeout(function(){ window.close(); }, 3000);</script>
                    </body>
                    </html>
                    """
                else:
                    return "Failed to exchange code for token", 500

            return "No authorization code received", 400

        @app.route('/shutdown')
        def shutdown():
            """Shutdown Flask server"""
            func = request.environ.get('werkzeug.server.shutdown')
            if func:
                func()
            return "Server shutting down..."

        # Get authorization URL
        auth_url = self.get_authorization_url()
        print(f"\nPlease authenticate at: {auth_url}\n")

        if open_browser:
            webbrowser.open(auth_url)

        # Run Flask server
        try:
            app.run(host='127.0.0.1', port=5000, debug=False)
        except Exception as e:
            logger.error(f"Failed to start callback server: {e}")
            return False

        return auth_complete['status']

    def get_headers(self) -> Dict[str, str]:
        """
        Get headers with authentication token

        Returns:
            Dictionary with authorization headers
        """
        # Reload credentials to get latest token
        self._load_credentials()

        if not self.is_token_valid():
            raise ValueError("Invalid or expired token. Please authenticate first.")

        return {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def clear_tokens(self) -> None:
        """Clear stored tokens (keeps API credentials)"""
        self.access_token = None
        self.token_expiry = None

        # Clear token in database
        self.db_manager.save_token("", 0)
        logger.info("Cleared stored tokens")

    def refresh_if_needed(self) -> bool:
        """
        Refresh token if needed

        Returns:
            True if token is valid (refreshed or still valid)
        """
        # Reload from database
        self._load_credentials()

        if self.is_token_valid():
            return True

        logger.info("Token expired, re-authentication required")
        return self.authenticate()

    def __str__(self) -> str:
        """String representation"""
        if not self.has_credentials():
            return "AuthManager(no_credentials)"

        if self.is_token_valid():
            remaining = (self.token_expiry - time.time()) / 3600
            return f"AuthManager(valid_token, expires_in={remaining:.1f}h)"

        return "AuthManager(expired_token)"