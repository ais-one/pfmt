"""
Authentication Middleware for handling Authorization from cookies
"""

import logging

from fastapi import HTTPException, Request, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from apps.app_nippon_rfq_matching.app.services.ais_auth_service import ais_auth_service
from apps.app_nippon_rfq_matching.app.services.ais_manager_service import (
    AISManagerService,
)

logger = logging.getLogger(__name__)


class AuthenticationMiddleware(BaseHTTPMiddleware):
    """Middleware for extracting and verifying Authorization token from cookies"""

    def __init__(self, app):
        super().__init__(app)
        self.ais_manager_service = AISManagerService()
        self.ais_auth_service = ais_auth_service

    async def dispatch(self, request: Request, call_next):
        """Process each request through authentication middleware"""

        # Skip authentication for CORS preflight requests
        if request.method == "OPTIONS":
            return await call_next(request)

        # Skip authentication for certain paths
        skip_paths = ["/", "/docs", "/openapi.json", "/health", "/favicon.ico"]

        # Check if current path should skip authentication
        if request.url.path in skip_paths or request.url.path.startswith("/docs"):
            return await call_next(request)

        # Extract Authorization token from cookies or Authorization header
        auth_cookie = request.cookies.get("Authorization")
        auth_header = request.headers.get("Authorization")

        # Check cookie first, then header
        if auth_cookie:
            auth_value = auth_cookie
        elif auth_header:
            auth_value = auth_header
        else:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "detail": "Authentication required",
                    "message": "Authorization token is missing",
                },
            )

        # Verify the token with AIS Manager backend
        try:
            # Extract Bearer token (remove 'Bearer ' prefix)
            if auth_value.startswith("Bearer "):
                token = auth_value[7:]
            else:
                # If doesn't have Bearer prefix, use it as-is
                token = auth_value

            # Get user info using AIS auth service with /me endpoint
            user_info = self.ais_auth_service.get_user_info(token)

            if not user_info:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or expired token",
                )

            # Handle different response formats from AIS Manager
            if "data" in user_info and "user" in user_info["data"]:
                # Format: {"data": {"user": {...}}}
                user_data = user_info["data"]["user"]
            elif "user" in user_info:
                # Format: {"user": {...}}
                user_data = user_info["user"]
            else:
                # Direct format
                user_data = user_info

            # Extract user information
            request.state.user_id = str(user_data.get("id") or user_data.get("user_id"))
            request.state.user_email = user_data.get("email")
            request.state.user_role = user_data.get("role")
            request.state.user_company_id = user_data.get("company_id")
            request.state.user_company_type = user_data.get("company_type")

        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "detail": "Authentication failed",
                    "message": "Invalid or expired token",
                },
            )

        # Add user info headers for logging purposes (without sensitive data)
        request.state.user_headers = {
            "X-User-ID": str(request.state.user_id),
            "X-User-Email": request.state.user_email,
            "X-User-Role": request.state.user_role,
        }

        # Extract and store Bearer token for AIS Manager API calls
        if auth_value.startswith("Bearer "):
            request.state.bearer_token = auth_value[7:]
        else:
            request.state.bearer_token = auth_value

        # Continue with the request
        response = await call_next(request)

        # Add user info to response headers for logging
        if hasattr(request.state, "user_headers"):
            for key, value in request.state.user_headers.items():
                if value is not None:
                    response.headers[key] = str(value)

        return response
