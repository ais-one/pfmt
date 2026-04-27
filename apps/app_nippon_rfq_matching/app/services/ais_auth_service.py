"""
AIS Manager Authentication Service

Service for validating tokens and getting user info from AIS Manager backend
"""

import logging
from typing import Any

import requests

from apps.app_nippon_rfq_matching.app.core.config import settings

logger = logging.getLogger(__name__)


class AISAuthService:
    """Service for authenticating with AIS Manager"""

    def __init__(self):
        self.base_url = settings.AISMBACKEND_URL + "/api/ais-manager/auth"
        self.timeout = 30  # seconds

    def validate_token(self, token: str) -> bool:
        """
        Validate JWT token with AIS Manager

        Args:
            token: JWT token to validate

        Returns:
            True if token is valid, False otherwise
        """
        if not token:
            logger.warning("Token is empty")
            return False

        try:
            url = f"{self.base_url}/verify"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            response = requests.get(url, headers=headers, timeout=self.timeout)

            if response.status_code == 200:
                logger.info("Token is valid")
                return True
            else:
                error_msg = response.json().get("message", "Unknown error")
                logger.warning(
                    f"Token validation failed: {response.status_code} - {error_msg}"
                )
                return False

        except requests.exceptions.RequestException as e:
            logger.error(f"Error validating token: {str(e)}")
            return False

    def get_user_info(self, token: str) -> dict[str, Any] | None:
        """
        Get user information from token

        Args:
            token: JWT token

        Returns:
            User info dict if successful, None otherwise
        """
        if not token:
            logger.warning("Token is empty")
            return None

        try:
            url = f"{self.base_url}/verify"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            response = requests.get(url, headers=headers, timeout=self.timeout)

            if response.status_code == 200:
                user_info = response.json()
                logger.info(
                    f"User info retrieved: {user_info.get('data', {}).get('user', {}).get('email')}"
                )
                return user_info
            else:
                error_msg = response.json().get("message", "Unknown error")
                logger.warning(
                    f"Failed to get user info: {response.status_code} - {error_msg}"
                )
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting user info: {str(e)}")
            return None

    def get_user_id_from_token(self, token: str) -> str | None:
        """
        Extract user ID from token

        Args:
            token: JWT token

        Returns:
            User ID if successful, None otherwise
        """
        user_info = self.get_user_info(token)
        if user_info and "data" in user_info and "user" in user_info["data"]:
            return user_info["data"]["user"].get("id")
        return None


# Service instance
ais_auth_service = AISAuthService()
