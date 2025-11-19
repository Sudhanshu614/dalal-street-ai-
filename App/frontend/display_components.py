"""
Generic Display Components for Stock Market Data
Senior Dev Pattern: Detect data structure → Choose display strategy → Render

Zero Hardcoding: Components adapt to:
- ANY number of stocks (1 stock vs 20 stocks)
- ANY fields (works with 10 fields or 40 fields)
- ANY data structure (single, multiple, time series, error)

Reference: FROM_SCRATCH_DOCS/FRONTEND_ARCHITECTURE.md Part 5 & 6
"""

import streamlit as st
import os
from typing import Dict, List, Any
from datetime import datetime
from datetime import datetime as dt

from formatters import format_field, format_field_with_color


# ============================================================================
# DATA STRUCTURE DETECTION (Zero Hardcoding)
# ============================================================================

def detect_data_structure(response: Dict[str, Any]) -> str:
    """
    Senior Dev: Analyze backend response to determine structure

    Zero Hardcoding: Detection based on data characteristics, not query type

    Returns one of:
    - 'error': Response contains error
    - 'empty': No results found
    - 'single_stock': One stock returned
    - 'multiple_stocks': Multiple stocks (comparison/screening)
    - 'time_series': Historical data with dates
    - 'corporate_actions': Corporate actions data
    - 'unknown': Cannot determine (fallback)
    """
    # Check for error
    if 'error' in response:
        return 'error'

    # Extract results from response
    raw_results = response.get('raw_results', {})
    results = raw_results.get('results', [])

    # Check if empty
    if not results or len(results) == 0:
        return 'empty'

    # Check if time series (has date field)
    first_result = results[0]
    if any('date' in str(key).lower() for key in first_result.keys()):
        return 'time_series'

    # Check if corporate actions
    if 'action_type' in first_result or 'ex_date' in first_result:
        return 'corporate_actions'

    # Single vs multiple stocks
    if len(results) == 1:
        return 'single_stock'
    else:
        return 'multiple_stocks'


# ============================================================================
# DISPLAY COMPONENTS (Generic Renderers)
# ============================================================================

def display_single_stock_card(response: Dict[str, Any]):
    """
    Display single stock as card (INDmoney style)

    Senior Dev: Works for stocks with ANY fields (not just hardcoded ones)
    Zero Hardcoding: Adapts layout based on available fields
    """
    results = response.get('raw_results', {}).get('results', [])
    if not results:
        st.warning("No stock data available")
        return

    stock = results[0]

    # Header: Symbol + Company Name
    col1, col2 = st.columns([3, 1])
    with col1:
        symbol = stock.get('symbol', stock.get('ticker', 'N/A'))
        company = stock.get('company_name', stock.get('name', 'Unknown Company'))
        st.subheader(f"{symbol} - {company}")

    with col2:
        # Show sector if available
        if 'sector' in stock:
            st.caption(f"📊 {stock['sector']}")

    # Senior Dev: Priority metrics (but not hardcoded - check if available)
    priority_metrics = [
        'current_price', 'market_cap', 'pe_ratio', 'pb_ratio',
        'roe', 'roce', 'dividend_yield', 'debt_to_equity'
    ]

    # Find which priority metrics are available
    available_metrics = [m for m in priority_metrics if m in stock and stock[m] is not None]

    # Display in columns (adaptable to number of metrics)
    if available_metrics:
        # Senior Dev: Dynamic column count based on available metrics
        num_cols = min(4, len(available_metrics))
        cols = st.columns(num_cols)

        for i, metric in enumerate(available_metrics[:num_cols]):
            with cols[i % num_cols]:
                label = metric.replace('_', ' ').title()
                formatted_value = format_field_with_color(metric, stock[metric])
                st.metric(label, formatted_value)

    # Divider
    st.divider()

    # All other fields in organized sections
    with st.expander("📋 Complete Details", expanded=False):
        # Senior Dev: Group fields by category (not hardcoded categories)
        other_fields = [k for k in stock.keys() if k not in priority_metrics and k not in ['symbol', 'ticker', 'company_name', 'name', 'sector']]

        # Display in 2 columns for better layout
        if other_fields:
            col1, col2 = st.columns(2)
            for i, field in enumerate(sorted(other_fields)):
                if stock[field] is not None:
                    label = field.replace('_', ' ').title()
                    formatted_value = format_field_with_color(field, stock[field])

                    # Alternate between columns
                    with col1 if i % 2 == 0 else col2:
                        st.write(f"**{label}:** {formatted_value}")


def display_comparison_table(response: Dict[str, Any]):
    """
    Display multiple stocks as comparison table

    Senior Dev: Works for ANY number of stocks with ANY fields
    Zero Hardcoding: Columns adapt to available fields
    """
    results = response.get('raw_results', {}).get('results', [])
    if not results:
        st.warning("No stocks to compare")
        return

    st.subheader(f"📊 Comparing {len(results)} Stocks")

    # Senior Dev: Find common fields across all stocks (intersection)
    all_fields = set(results[0].keys())
    for stock in results[1:]:
        all_fields &= set(stock.keys())

    # Prioritize important fields
    priority_fields = [
        'symbol', 'company_name', 'current_price', 'market_cap',
        'pe_ratio', 'pb_ratio', 'roe', 'dividend_yield'
    ]

    # Build display fields: priority first, then others
    display_fields = [f for f in priority_fields if f in all_fields]
    other_fields = sorted([f for f in all_fields if f not in display_fields])
    display_fields.extend(other_fields)

    # Build table data (formatted)
    table_data = []
    for stock in results:
        row = {}
        for field in display_fields:
            label = field.replace('_', ' ').title()
            # Note: Can't use color in dataframe, just format
            row[label] = format_field(field, stock[field])
        table_data.append(row)

    # Display as simple table (no interactive features)
    import pandas as pd
    st.table(pd.DataFrame(table_data))

    pass


def display_time_series_chart(response: Dict[str, Any]):
    """
    Display time series data as chart

    Senior Dev: Works for ANY time series (price, volume, indicator)
    Zero Hardcoding: Detects time field and value fields automatically
    """
    results = response.get('raw_results', {}).get('results', [])
    if not results:
        st.warning("No time series data available")
        return

    # Simple time series table (no charts)

    # Senior Dev: Detect time field (not hardcoded to 'date')
    time_field = next(
        (f for f in results[0].keys() if any(x in f.lower() for x in ['date', 'time', 'timestamp'])),
        None
    )

    if not time_field:
        st.error("Time series data missing date/time field")
        return

    # Detect numeric value fields (candidates for plotting)
    value_fields = [
        f for f in results[0].keys()
        if f != time_field and isinstance(results[0][f], (int, float)) and results[0][f] is not None
    ]

    if not value_fields:
        st.error("No numeric fields found for plotting")
        return

    # Convert to DataFrame for table view
    import pandas as pd
    df = pd.DataFrame(results)

    # Format date column to DD Mon YYYY if parsable
    if time_field in df.columns:
        try:
            df[time_field] = pd.to_datetime(df[time_field])
            df[time_field] = df[time_field].dt.strftime('%d %b %Y')
        except Exception:
            pass
    st.table(df)


def display_corporate_actions(response: Dict[str, Any]):
    """
    Display corporate actions as timeline

    Senior Dev: Works for ANY corporate action type
    Zero Hardcoding: Adapts to available fields
    """
    results = response.get('raw_results', {}).get('results', [])
    if not results:
        st.warning("No corporate actions found")
        return

    st.subheader("📜 Corporate Actions")

    # Display as timeline
    for action in results:
        action_type = action.get('action_type', 'Unknown')
        ex_date = action.get('ex_date', 'N/A')
        details = action.get('details', '')

        with st.container():
            col1, col2 = st.columns([1, 4])
            with col1:
                st.write(f"**{format_field('ex_date', ex_date)}**")
            with col2:
                st.write(f"**{action_type}**: {details}")

            st.divider()


def display_error(response: Dict[str, Any]):
    """
    Display error message

    Senior Dev: Shows helpful error info for debugging
    """
    error_msg = response.get('error', 'Unknown error occurred')
    st.error(f"❌ {error_msg}")

    # Show error details if available
    if 'details' in response or 'exception_type' in response:
        with st.expander("🔍 Error Details"):
            if 'exception_type' in response:
                st.write(f"**Type:** {response['exception_type']}")
            if 'details' in response:
                st.json(response['details'])


def display_empty(response: Dict[str, Any]):
    """Silent empty state (no decorative box)"""
    pass


def display_generic(response: Dict[str, Any]):
    """
    Fallback display for unknown structures

    Senior Dev: Show raw JSON when structure is unrecognized
    """
    st.warning("⚠️ Unknown data structure - showing raw response")
    st.json(response)


# ============================================================================
# DISPLAY STRATEGY MAPPING (Zero Hardcoding)
# ============================================================================

# Senior Dev: Map data structure to display function (not query type!)
DISPLAY_STRATEGIES = {
    'single_stock': display_single_stock_card,
    'multiple_stocks': display_comparison_table,
    'time_series': display_time_series_chart,
    'corporate_actions': display_corporate_actions,
    'error': display_error,
    'empty': display_empty,
    'unknown': display_generic,
}


# ============================================================================
# MAIN DISPLAY FUNCTION (Entry Point)
# ============================================================================

def display_response(response: Dict[str, Any]):
    from universal_renderer import render_response
    render_response(response)


def show_metadata(raw_results: Dict[str, Any]):
    """
    Show data source and freshness info

    Senior Dev: Transparent about data source for user trust
    """
    # Defensive: raw_results may be None or not a dict
    if not raw_results or not isinstance(raw_results, dict):
        return
    source = raw_results.get('source', 'unknown')
    timestamp = raw_results.get('timestamp', 'unknown')
    if source == 'unknown' and timestamp == 'unknown':
        return
    if timestamp != 'unknown':
        timestamp = format_field('timestamp', timestamp)
    st.markdown('---')
    st.markdown(f"🔍 Source: {source}")
    st.markdown(f"📅 Updated: {timestamp}")


# ============================================================================
# SUGGESTION PILLS (Generic Categories)
# ============================================================================

def show_suggestion_pills():
    """
    Show suggestion categories (not hardcoded queries)

    Senior Dev: Categories are generic, actual queries are examples
    Users can substitute {ticker}, {sector}, {value} with real values
    """
    st.write("### 💡 Try asking:")

    # Senior Dev: Generic categories, not hardcoded queries
    categories = {
        "📊 Stock Information": [
            "Show me TCS details",
            "Compare TCS vs INFY",
            "Top 10 stocks by market cap",
        ],
        "🔍 Screening": [
            "Stocks with PE < 20 in IT sector",
            "High ROE companies above 25%",
            "Best dividend yielding stocks",
        ],
        "📈 Analysis": [
            "Calculate RSI for RELIANCE",
            "Show price history for HDFC Bank",
            "HDFC Bank dividend history",
        ],
    }

    # Display as tabs
    tabs = st.tabs(list(categories.keys()))

    for tab, (category, suggestions) in zip(tabs, categories.items()):
        with tab:
            for suggestion in suggestions:
                if st.button(suggestion, key=f"btn_{suggestion}"):
                    # Return suggestion to be used as query
                    return suggestion

    return None
