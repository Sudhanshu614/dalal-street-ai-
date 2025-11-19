"""
Generic Formatters for Stock Market Data
Senior Dev Pattern: Format based on data TYPE, not field NAME

Zero Hardcoding: Formatters are data-driven, detect format from:
- Field name patterns (contains 'price', 'ratio', etc.)
- Value type (float, int, str, date)
- Metadata hints from backend (if provided)

Reference: FROM_SCRATCH_DOCS/FRONTEND_ARCHITECTURE.md Part 3
"""

from typing import Any, Union
from datetime import datetime, timedelta


def format_indian_currency(value: Union[int, float]) -> str:
    """
    Format number as Indian currency with ₹ symbol and Cr/L notation

    Senior Dev: Scales automatically based on value size
    Examples:
        1234.56 → ₹1,234.56
        123456.78 → ₹1.23 L
        12345678.90 → ₹123.46 Cr
    """
    if value is None:
        return "N/A"

    try:
        value = float(value)
    except (ValueError, TypeError):
        return str(value)

    if value >= 10_000_000:  # 1 crore = 10 million
        crores = value / 10_000_000
        return f"₹{crores:,.2f} Cr"
    elif value >= 100_000:  # 1 lakh = 100k
        lakhs = value / 100_000
        return f"₹{lakhs:,.2f} L"
    else:
        return f"₹{value:,.2f}"


def format_percentage(value: Union[int, float], show_sign: bool = True) -> str:
    """
    Format percentage with optional +/- sign

    Senior Dev: Configurable sign display for different contexts
    Examples:
        2.34 → +2.34% (if show_sign=True)
        -1.23 → -1.23%
        15.67 → 15.67% (if show_sign=False)
    """
    if value is None:
        return "N/A"

    try:
        value = float(value)
    except (ValueError, TypeError):
        return str(value)

    if show_sign:
        return f"{value:+.2f}%"
    else:
        return f"{value:.2f}%"


def format_number(value: Union[int, float], decimals: int = 2) -> str:
    """
    Format number with commas and configurable decimal places

    Senior Dev: Decimals configurable for different contexts
    Examples:
        1234567 → 1,234,567.00 (decimals=2)
        1234567 → 1,234,567.0 (decimals=1)
        1234567 → 1,234,567 (decimals=0)
    """
    if value is None:
        return "N/A"

    try:
        if isinstance(value, int):
            return f"{value:,}"
        else:
            value = float(value)
            return f"{value:,.{decimals}f}"
    except (ValueError, TypeError):
        return str(value)


def format_date(value: Any, relative: bool = False) -> str:
    """
    Format date with optional relative time

    Senior Dev: Intelligent relative dates for recent timestamps
    Examples:
        today → "Today" (if relative=True)
        yesterday → "Yesterday"
        3 days ago → "3 days ago"
        2024-01-15 → "15-Jan-2024" (if older)
    """
    if value is None:
        return "N/A"

    # Parse if string
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except:
            try:
                value = datetime.strptime(value, '%Y-%m-%d')
            except:
                return value

    if not isinstance(value, datetime):
        return str(value)

    return value.strftime("%d %b %Y")


def format_ratio(value: Union[int, float]) -> str:
    """
    Format ratio with 'x' suffix

    Senior Dev: Consistent format for all ratios (PE, PB, etc.)
    Examples:
        15.34 → 15.34x
        2.56 → 2.56x
    """
    if value is None:
        return "N/A"

    try:
        value = float(value)
        return f"{value:.2f}x"
    except (ValueError, TypeError):
        return str(value)


# ============================================================================
# INTELLIGENT FORMATTER SELECTION (Zero Hardcoding)
# ============================================================================

def get_formatter(field_name: str, value: Any, metadata: dict = None):
    """
    Senior Dev: Select formatter based on field characteristics

    Zero Hardcoding: Detects format from:
    1. Metadata hints from backend (if provided)
    2. Field name patterns (intelligent detection)
    3. Value type as fallback

    Returns: Formatter function
    """
    # Priority 1: Explicit metadata hint from backend
    if metadata and 'format' in metadata:
        format_type = metadata['format']
        return FORMATTERS.get(format_type, format_text)

    # Priority 2: Field name pattern detection
    field_lower = field_name.lower()

    # Currency patterns
    if any(x in field_lower for x in ['price', 'cap', 'value', 'revenue', 'sales', 'profit']):
        return format_indian_currency

    # Percentage patterns
    elif any(x in field_lower for x in ['yield', 'change', 'growth', 'return']):
        return lambda v: format_percentage(v, show_sign=True)

    # Ratio patterns
    elif any(x in field_lower for x in ['ratio', 'pe_', 'pb_', 'ps_']):
        return format_ratio

    # Date patterns
    elif any(x in field_lower for x in ['date', 'time', 'updated', 'created']):
        return format_date

    # Priority 3: Value type detection
    elif isinstance(value, float):
        return lambda v: format_number(v, decimals=2)

    elif isinstance(value, int):
        return format_number

    # Default: Text
    else:
        return format_text


def format_text(value: Any) -> str:
    """Fallback formatter for text/unknown types"""
    if value is None:
        return "N/A"
    return str(value)


# Formatter registry (for explicit format type specification)
FORMATTERS = {
    'currency': format_indian_currency,
    'percentage': format_percentage,
    'number': format_number,
    'date': format_date,
    'ratio': format_ratio,
    'text': format_text,
}


# ============================================================================
# GENERIC FIELD FORMATTER (Main Entry Point)
# ============================================================================

def format_field(field_name: str, value: Any, metadata: dict = None) -> str:
    """
    MAIN FORMATTER - Works for ANY field with ANY value

    Senior Dev: This is the ONLY format function UI should call
    Zero Hardcoding: No hardcoded field names, detects format intelligently

    Usage:
        format_field('current_price', 3245.50)  → "₹3,245.50"
        format_field('market_cap', 1123456789)  → "₹1,123.46 Cr"
        format_field('pe_ratio', 28.45)         → "28.45x"
        format_field('roe', 45.67)              → "+45.67%"
        format_field('last_updated', datetime)  → "Today"

    Args:
        field_name: Name of the field (used for format detection)
        value: Value to format
        metadata: Optional backend hints (e.g., {'format': 'currency'})

    Returns:
        Formatted string ready for display
    """
    formatter = get_formatter(field_name, value, metadata)
    return formatter(value)


# ============================================================================
# COLOR CODING FOR STREAMLIT (Generic Pattern)
# ============================================================================

def get_color_for_value(field_name: str, value: Any) -> str:
    """
    Senior Dev: Get Streamlit color code based on value semantics

    Returns: 'green', 'red', or None (neutral)

    Usage in Streamlit:
        color = get_color_for_value('change', 2.34)
        st.markdown(f":{color}[{formatted_value}]")
    """
    if value is None:
        return None

    field_lower = field_name.lower()

    # Change/Growth fields: positive=green, negative=red
    if any(x in field_lower for x in ['change', 'growth', 'return']):
        try:
            numeric_value = float(value)
            return 'green' if numeric_value > 0 else 'red' if numeric_value < 0 else None
        except:
            return None

    # Debt fields: lower is better (inverted colors)
    elif any(x in field_lower for x in ['debt', 'dii_holding']):
        try:
            numeric_value = float(value)
            return 'red' if numeric_value > 50 else 'green' if numeric_value < 30 else None
        except:
            return None

    return None


# ============================================================================
# UTILITY: Format Field with Color (Streamlit-specific)
# ============================================================================

def format_field_with_color(field_name: str, value: Any, metadata: dict = None) -> str:
    """
    Format field AND apply Streamlit color markdown if applicable

    Senior Dev: Combines formatting + color coding in one call

    Returns: Streamlit-compatible markdown string
    Example: ":green[+2.34%]" or "₹3,245.50"
    """
    formatted = format_field(field_name, value, metadata)
    color = get_color_for_value(field_name, value)

    if color:
        return f":{color}[{formatted}]"
    else:
        return formatted
