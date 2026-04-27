"""
Security dependencies for API endpoints
"""

from fastapi import APIRouter, HTTPException, Request, status

from apps.app_nippon_rfq_matching.app.services.ais_auth_service import ais_auth_service

router = APIRouter()


async def get_current_user(request: Request):
    """
    Dependency to get current authenticated user from cookie
    """
    # Extract Authorization token from cookie
    auth_cookie = request.cookies.get("Authorization")

    if not auth_cookie:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Extract Bearer token (remove 'Bearer ' prefix)
    if auth_cookie.startswith("Bearer "):
        token = auth_cookie[7:]
    else:
        # If cookie doesn't have Bearer prefix, use it as-is
        token = auth_cookie

    # Verify the token with AIS Manager backend
    user_info = ais_auth_service.get_user_info(token)

    if not user_info:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Return user data
    return user_info
