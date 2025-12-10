"""
Platform-specific mapping rules and validation.

Defines rules for how each platform should map data to the schema,
including required fields, default values, and validation constraints.
"""

from __future__ import annotations

from typing import Dict, Any, Optional

# Platform rules configuration
PLATFORM_RULES: Dict[str, Dict[str, Any]] = {
    "News": {
        "requires_metrics": False,
        "default_metrics": "N/A",
        "allow_username_style_profile": False,
        "requires_followers": False,
        "default_followers": "N/A",
        "requires_profile_link": False,
        "default_profile_link": "N/A",
    },
    "X": {
        "requires_metrics": True,
        "default_metrics": "0",  # Should be numeric, not "N/A"
        "allow_username_style_profile": True,
        "requires_followers": False,  # Can be "N/A" if not available
        "default_followers": "N/A",
        "requires_profile_link": False,
        "default_profile_link": "N/A",
    },
    "Reddit": {
        "requires_metrics": True,
        "default_metrics": "0",
        "allow_username_style_profile": True,
        "requires_followers": False,
        "default_followers": "N/A",
        "requires_profile_link": False,
        "default_profile_link": "N/A",
    },
}


def get_platform_rule(platform: str, rule_key: str, default: Any = None) -> Any:
    """
    Get a platform-specific rule value.

    Args:
        platform: Platform name ("News", "X", "Reddit", etc.)
        rule_key: Rule key to look up
        default: Default value if platform or rule not found

    Returns:
        Rule value or default
    """
    rules = PLATFORM_RULES.get(platform, {})
    return rules.get(rule_key, default)


def validate_platform_item(item: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Validate that an item conforms to its platform's rules.

    Args:
        item: Normalized item dictionary

    Returns:
        Tuple of (is_valid, error_message)
        - is_valid: True if item passes validation
        - error_message: None if valid, error description if invalid
    """
    platform = item.get("platform", "")

    if not platform:
        return False, "Platform is missing"

    rules = PLATFORM_RULES.get(platform)
    if not rules:
        # Unknown platform - allow it but log a warning
        return True, None

    # Check metrics requirements
    if rules.get("requires_metrics", False):
        # For platforms that require metrics, ensure they're numeric (not "N/A")
        likes = item.get("likes", "N/A")
        comments = item.get("comments", "N/A")
        shares = item.get("shares", "N/A")

        # At least one should be numeric (not "N/A")
        metrics_are_numeric = (
            likes != "N/A"
            or comments != "N/A"
            or shares != "N/A"
        )

        if not metrics_are_numeric:
            return False, f"Platform '{platform}' requires numeric metrics, but all are 'N/A'"

    # Check followers requirement
    if rules.get("requires_followers", False):
        followers = item.get("followers", "N/A")
        if followers == "N/A" or not followers:
            return False, f"Platform '{platform}' requires followers count"

    return True, None


def apply_platform_defaults(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply platform-specific default values to an item.

    This ensures that items conform to platform rules by filling in
    missing values with appropriate defaults.

    Args:
        item: Normalized item dictionary (may be modified in place)

    Returns:
        Item dictionary with defaults applied
    """
    platform = item.get("platform", "")
    if not platform:
        return item

    rules = PLATFORM_RULES.get(platform, {})

    # Apply metric defaults if needed
    if not rules.get("requires_metrics", False):
        # News platform: all metrics should be "N/A"
        if platform == "News":
            item["views"] = item.get("views") or "N/A"
            item["likes"] = item.get("likes") or "N/A"
            item["comments"] = item.get("comments") or "N/A"
            item["shares"] = item.get("shares") or "N/A"
            item["eng_total"] = item.get("eng_total") or "N/A"

    # Apply followers default
    if not item.get("followers"):
        item["followers"] = rules.get("default_followers", "N/A")

    # Apply profile link default
    if not item.get("profile_link"):
        item["profile_link"] = rules.get("default_profile_link", "N/A")

    return item

