#!/usr/bin/env python3
"""
JSON Flattener - Universal flattening of nested JSON structures
Handles any level of nesting, arrays, and complex structures
"""
from typing import Dict, Any, List


def flatten_json(data: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """
    Flatten a nested JSON structure into a single-level dictionary.
    
    This handles:
    - Nested dictionaries: {"a": {"b": "c"}} → {"a_b": "c"}
    - Arrays of dictionaries: {"blocks": [{"date": "2024"}]} → {"blocks_0_date": "2024"}
    - Mixed structures: Any combination of the above
    
    Args:
        data: Dictionary to flatten
        parent_key: Key prefix for nested items (used in recursion)
        sep: Separator between nested keys (default: '_')
    
    Returns:
        Flattened dictionary with all nested values at top level
    
    Examples:
        >>> flatten_json({"a": {"b": "c"}})
        {"a_b": "c"}
        
        >>> flatten_json({"blocks": [{"date": "2024", "name": "John"}]})
        {"blocks_0_date": "2024", "blocks_0_name": "John"}
        
        >>> flatten_json({"reverification_blocks": [{"employer_signature_date": "2024"}]})
        {"reverification_blocks_0_employer_signature_date": "2024"}
    """
    items = []
    
    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        
        if isinstance(value, dict):
            # Recursively flatten nested dictionary
            items.extend(flatten_json(value, new_key, sep=sep).items())
        
        elif isinstance(value, list):
            # Handle arrays
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    # Array of dictionaries - flatten each with index
                    items.extend(flatten_json(item, f"{new_key}{sep}{i}", sep=sep).items())
                else:
                    # Array of primitives - keep as is with index
                    items.append((f"{new_key}{sep}{i}", item))
        
        else:
            # Primitive value - keep as is
            items.append((new_key, value))
    
    return dict(items)


def flatten_extracted_values(extracted_values: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten extracted values from a catalog page.
    
    This is specifically designed for I-9 catalog data where Gemini may extract
    data in various nested structures like:
    - reverification_blocks
    - section_3_blocks
    - document_lists
    - etc.
    
    Args:
        extracted_values: The extracted_values dictionary from a catalog page
    
    Returns:
        Flattened dictionary with all nested values accessible at top level
        
    Example:
        Input:
        {
            "employee_name": "John Doe",
            "reverification_blocks": [
                {
                    "date_signed": "08/30/2024",
                    "document_title": "EAD",
                    "employer_signature": "Digitally signed..."
                }
            ]
        }
        
        Output:
        {
            "employee_name": "John Doe",
            "reverification_blocks_0_date_signed": "08/30/2024",
            "reverification_blocks_0_document_title": "EAD",
            "reverification_blocks_0_employer_signature": "Digitally signed..."
        }
    """
    return flatten_json(extracted_values)


def get_all_values_from_nested_structure(data: Dict[str, Any], target_keys: List[str]) -> List[Any]:
    """
    Extract all values for specific keys from a nested structure.
    
    This is useful when you want to find all occurrences of a field
    regardless of nesting level.
    
    Args:
        data: Dictionary to search (can be nested)
        target_keys: List of keys to find values for
    
    Returns:
        List of all values found for the target keys
        
    Example:
        >>> data = {"blocks": [{"date": "2024"}, {"date": "2023"}]}
        >>> get_all_values_from_nested_structure(data, ["date"])
        ["2024", "2023"]
    """
    values = []
    
    def _search(obj, keys):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in keys:
                    values.append(v)
                _search(v, keys)
        elif isinstance(obj, list):
            for item in obj:
                _search(item, keys)
    
    _search(data, target_keys)
    return values


# Example usage and tests
if __name__ == "__main__":
    # Test 1: Simple nested structure
    print("Test 1: Simple nested structure")
    data1 = {
        "employee": {
            "first_name": "John",
            "last_name": "Doe"
        }
    }
    print(f"Input: {data1}")
    print(f"Output: {flatten_json(data1)}")
    print()
    
    # Test 2: Array of dictionaries (Balder case)
    print("Test 2: Array of dictionaries (Balder case)")
    data2 = {
        "employee_name": "Pauline Balder",
        "reverification_blocks": [
            {
                "block_number": 1,
                "date_signed": "08/30/2024",
                "document_title": "EAD ISSUED BY DHS",
                "employer_signature": "Digitally signed by: Jeanie Huang"
            }
        ]
    }
    print(f"Input: {data2}")
    flattened = flatten_json(data2)
    print(f"Output: {flattened}")
    print()
    print("Key signature fields found:")
    for key in flattened:
        if 'signature' in key.lower() or 'date' in key.lower():
            print(f"  - {key}: {flattened[key]}")
    print()
    
    # Test 3: Multiple blocks
    print("Test 3: Multiple blocks")
    data3 = {
        "section_3_blocks": [
            {"date": "2024-01-01", "doc": "Passport"},
            {"date": "2024-02-01", "doc": "EAD"}
        ]
    }
    print(f"Input: {data3}")
    print(f"Output: {flatten_json(data3)}")
    print()
    
    # Test 4: Deep nesting
    print("Test 4: Deep nesting")
    data4 = {
        "level1": {
            "level2": {
                "level3": {
                    "signature_date": "2024-01-01"
                }
            }
        }
    }
    print(f"Input: {data4}")
    print(f"Output: {flatten_json(data4)}")
    print()
    
    # Test 5: Get all values
    print("Test 5: Get all signature dates from nested structure")
    data5 = {
        "employer_signature_date": "2024-01-01",
        "blocks": [
            {"date_signed": "2024-02-01"},
            {"date_signed": "2024-03-01"}
        ]
    }
    dates = get_all_values_from_nested_structure(data5, ["date_signed", "employer_signature_date"])
    print(f"Input: {data5}")
    print(f"All signature dates found: {dates}")
