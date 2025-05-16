"""
Parser functions for processing JSON loyalty transaction logs
"""

import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

def get_json_schema():
    """Define the schema for JSON loyalty transaction logs"""
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("flight_details", StructType([
            StructField("flight_no", StringType(), True),
            StructField("departure", StringType(), True),
            StructField("arrival", StringType(), True),
            StructField("date", StringType(), True)
        ]), True),
        StructField("transaction_info", StructType([
            StructField("earned_points", IntegerType(), True),
            StructField("transaction_ref", StringType(), True),
            StructField("channel", StringType(), True)
        ]), True),
        StructField("additional_info", StructType([
            StructField("class", StringType(), True),
            StructField("status", StringType(), True),
            StructField("baggage_allowance", StringType(), True),
            StructField("lounge_access", BooleanType(), True)
        ]), True),
        StructField("promo_details", StructType([
            StructField("code_used", StringType(), True),
            StructField("bonus_points", IntegerType(), True)
        ]), True)
    ])

def prepare_json_for_combined_schema(json_str):
    """
    Process JSON string to fit the combined schema structure
    This function helps standardize JSON structure to match our unified schema
    """
    try:
        data = json.loads(json_str)
        
        # Extract and restructure fields to match our common schema
        result = {
            "customer_id": data.get("customer_id"),
            "flight_no": data.get("flight_details", {}).get("flight_no"),
            "departure": data.get("flight_details", {}).get("departure"),
            "arrival": data.get("flight_details", {}).get("arrival"),
            "transaction_date": data.get("flight_details", {}).get("date"),
            "earned_points": data.get("transaction_info", {}).get("earned_points"),
            "transaction_ref": data.get("transaction_info", {}).get("transaction_ref"),
            "channel": data.get("transaction_info", {}).get("channel"),
            "additional_data": json.dumps({
                "additional_info": data.get("additional_info"),
                "promo_details": data.get("promo_details")
            }),
            "source_type": "json"
        }
        
        return result
    except Exception as e:
        print(f"Error processing JSON data: {str(e)}")
        return None

def extract_nested_json_fields(json_obj, field_path, default=None):
    """
    Helper function to safely extract nested fields from JSON objects
    
    Args:
        json_obj: The JSON object
        field_path: A string with dot notation for nested fields (e.g., "flight_details.flight_no")
        default: Default value to return if field doesn't exist
        
    Returns:
        The value at the specified path or the default value
    """
    try:
        if not json_obj:
            return default
            
        parts = field_path.split(".")
        current = json_obj
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return default
                
        return current
    except Exception as e:
        print(f"Error extracting field {field_path}: {str(e)}")
        return default