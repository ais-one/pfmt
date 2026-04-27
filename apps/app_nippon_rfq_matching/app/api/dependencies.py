"""
FastAPI dependencies for authentication
"""

from fastapi import HTTPException, status

from apps.app_nippon_rfq_matching.app.services.ais_auth_service import ais_auth_service


def get_current_user(bearer_token: str = None) -> str:
    """
    Get current user ID from Bearer token

    Args:
        bearer_token: Bearer token from Authorization header

    Returns:
        User ID string

    Raises:
        HTTPException: If token is invalid or missing
    """
    # Check if token is provided
    if not bearer_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token Missing"
        )

    # Validate token
    if not ais_auth_service.validate_token(bearer_token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token Error"
        )

    # Get user info
    user_id = ais_auth_service.get_user_id_from_token(bearer_token)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid Token"
        )

    return user_id


def get_optional_user(bearer_token: str | None = None) -> str | None:
    """
    Get current user ID from Bearer token (optional)

    Args:
        bearer_token: Bearer token from Authorization header (optional)

    Returns:
        User ID string if token is valid, None otherwise
    """
    if not bearer_token:
        return None

    try:
        if ais_auth_service.validate_token(bearer_token):
            return ais_auth_service.get_user_id_from_token(bearer_token)
    except Exception:
        pass

    return None


class TokenAuth:
    """
    Dependency class for Bearer token authentication
    """

    def __init__(self, optional: bool = False):
        self.optional = optional

    def __call__(self, authorization: str | None = None) -> str:
        """
        Extract Bearer token from Authorization header

        Args:
            authorization: Authorization header value (e.g., "Bearer <token>")

        Returns:
            Bearer token string
        """
        if not authorization:
            if self.optional:
                return None
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Token Missing"
            )

        # Extract Bearer token
        try:
            scheme, token = authorization.split(" ", 1)
            if scheme.lower() != "bearer":
                if self.optional:
                    return None
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid Authentication Scheme",
                )
            return token
        except ValueError:
            if self.optional:
                return None
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid Authentication Format",
            )
