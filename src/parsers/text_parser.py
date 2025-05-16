"""
Parser functions for extracting data from text-based loyalty transaction logs
with improved error handling and robustness
"""

import re
import json
from datetime import datetime
import logging

# Configure logging
logger = logging.getLogger(__name__)

def safe_parser(func):
    """
    Decorator for making parser functions more robust against failures
    """
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logger.warning(f"Error in {func.__name__}: {str(e)}")
            return None
    return wrapper

@safe_parser
def split_transactions(text):
    """
    Split a text file containing multiple transaction records into individual transactions.
    
    Args:
        text: The entire text file content
        
    Returns:
        List of individual transaction strings
    """
    if not text or not isinstance(text, str):
        return [text] if text else []
        
    # Identify common transaction separators
    separators = [
        r"---\s*End of Transaction\s*---",
        r"---\s*Loyalty Transaction Log Start\s*---",
        r"\*PARTIAL DATA\*"
    ]
    
    # Try splitting by the most common separator pattern
    pattern = "|".join(separators)
    parts = re.split(pattern, text, flags=re.IGNORECASE)
    
    # Filter out empty parts and strip whitespace
    transactions = [part.strip() for part in parts if part and part.strip()]
    
    # If no transactions found, return the original text as a single transaction
    if not transactions:
        return [text]
        
    return transactions

@safe_parser
def extract_customer_id(text):
    """Extract customer ID from a single transaction text"""
    if not text or not isinstance(text, str):
        return None
        
    patterns = [
        r'Customer ID\s*[=:]\s*(\d+)',
        r'Customer\s+(\d+)'
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None

@safe_parser
def extract_flight_no(text):
    """Extract flight number from a single transaction text"""
    if not text or not isinstance(text, str):
        return None
        
    patterns = [
        r'Flight No[=:]\s*([A-Z0-9]+)',
        r'flight\s+([A-Z0-9]+)'
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    return None

@safe_parser
def extract_departure(text):
    """Extract departure airport from a single transaction text"""
    if not text or not isinstance(text, str):
        return None
        
    match = re.search(r'Departure[=:]\s*([A-Z]{3})', text)
    return match.group(1) if match else None

@safe_parser
def extract_arrival(text):
    """Extract arrival airport from a single transaction text"""
    if not text or not isinstance(text, str):
        return None
        
    match = re.search(r'Arrival[=:]\s*([A-Z]{3})', text)
    return match.group(1) if match else None

@safe_parser
def extract_transaction_date(text):
    """Extract transaction date from a single transaction text"""
    if not text or not isinstance(text, str):
        return None
        
    patterns = [
        r'Date[=:]\s*(\d{4}-\d{2}-\d{2})',
        r'on\s+(\d{4}-\d{2}-\d{2})'
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            date_str = match.group(1)
            try:
                return datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                continue
    return None

@safe_parser
def extract_earned_points(text):
    """Extract earned points from a single transaction text"""
    if not text or not isinstance(text, str):
        return None
        
    patterns = [
        r'Earned Points[=:]\s*(\d+)',
        r'Earned\s+(\d+)\s+points'
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                continue
    return None

@safe_parser
def extract_transaction_ref(text):
    """Extract transaction reference from a single transaction text"""
    if not text or not isinstance(text, str):
        return None
        
    patterns = [
        r'Transaction Ref[=:]\s*(TXN-\d+-[A-Z]+)',
        r'TXREF\s*=\s*(TXN-\d+-[A-Z]+)'
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None

@safe_parser
def extract_channel(text):
    """Extract channel from a single transaction text"""
    if not text or not isinstance(text, str):
        return None
        
    patterns = [
        r'Channel[=:]\s*(\w+(?:_\w+)*)',
        r'Source\s*=\s*(\w+(?:_\w+)*)'
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None

@safe_parser
def extract_additional_data(text):
    """Extract additional data as JSON from a single transaction text"""
    if not text or not isinstance(text, str):
        return "{}"
        
    additional_data = {}
    patterns = {
        'tier_status': r'Tier Status\s*:\s*(\w+)',
        'additional_miles': r'Additional Miles\s*=\s*(\d+)',
        'special_request': r'Special Request\s*:\s*([^,\n]+)',
        'seat_location': r'Seat Location\s*:\s*([^,\n]+)',
        'co_branded_card': r'Co-branded Card Used\s*:\s*(\w+)',
        'booking_class': r'Booking Class\s*:\s*([^,\n]+)',
        'fare_paid': r'Fare Paid\s*=\s*([^,\n]+)',
        'miles_multiplier': r'Miles Multiplier\s*:\s*([^,\n]+)',
        'special_service': r'Special_Service_Request\s*:\s*([^,\n]+)',
        'premium_membership': r'Premium Membership\s*=\s*(\w+)',
        'seat_changed': r'Seat Changed\s*:\s*([^,\n]+)',
        'wifi_package': r'WiFi Package\s*\|\s*([^,\n]+)',
        'feedback_rating': r'Feedback Rating\s*:\s*([^,\n]+)',
        'partner': r'PARTNER\s*:\s*([^,\n|]+)',
        'booking_notes': r'Booking Notes\s*:\s*([^,\n]+)',
        'status_match': r'Status Match Request\s*\|\s*([^,\n]+)',
        'agent_comment': r'Agent Comment\s*:\s*([^,\n]+)',
        'lounge_access': r'lounge access\s*\.\s*',
        'anniversary_bonus': r'anniversary bonus'
    }
    for key, pattern in patterns.items():
        try:
            match = re.search(pattern, text)
            if match and match.groups():
                additional_data[key] = match.group(1).strip()
            elif match:
                additional_data[key] = "Yes"
        except Exception:
            continue
    
    try:
        return json.dumps(additional_data)
    except Exception:
        return "{}"

@safe_parser
def process_transaction_text(text):
    """
    Process a single transaction text and extract all fields in one function.
    This helps reduce the number of UDF calls.
    
    Args:
        text: A single transaction text
        
    Returns:
        Dictionary with all extracted fields
    """
    if not text or not isinstance(text, str):
        return None
        
    result = {
        'customer_id': extract_customer_id(text),
        'flight_no': extract_flight_no(text),
        'departure': extract_departure(text),
        'arrival': extract_arrival(text),
        'transaction_date': extract_transaction_date(text),
        'earned_points': extract_earned_points(text),
        'transaction_ref': extract_transaction_ref(text),
        'channel': extract_channel(text),
        'additional_data': extract_additional_data(text),
        'source_type': 'text'
    }
    
    # Filter out None values
    return {k: v for k, v in result.items() if v is not None}