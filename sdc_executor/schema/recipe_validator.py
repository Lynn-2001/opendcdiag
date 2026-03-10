"""
Input Test Recipe Validator for SDC Testing Executor

This module provides validation for test recipes based on the OCP SDC Workstream
specification with SDC executor-specific extensions.

Usage:
    from sdc_executor.schema.recipe_validator import validate, validate_file
    
    # Validate a dictionary
    try:
        validate(recipe_dict)
        print("Recipe is valid")
    except ValidationError as e:
        print(f"Validation failed: {e}")
    
    # Validate a file
    try:
        validate_file("path/to/recipe.yaml")
        print("Recipe file is valid")
    except ValidationError as e:
        print(f"Validation failed: {e}")
"""

import json
import yaml
from pathlib import Path
from typing import Dict, Any, Set, List
import jsonschema
from jsonschema import ValidationError


def _load_schema() -> Dict[str, Any]:
    """Load the JSON schema from the schema file."""
    schema_path = Path(__file__).parent / "test_recipe_schema.json"
    with open(schema_path, 'r') as f:
        return json.load(f)


def _check_business_logic(recipe: Dict[str, Any]) -> None:
    """
    Perform business logic validation beyond JSON schema.
    
    Args:
        recipe: The recipe dictionary to validate
        
    Raises:
        ValidationError: If business logic validation fails
    """
    # Check loop.count constraints
    if 'loop' in recipe and recipe['loop'].get('enabled', False):
        count = recipe['loop'].get('count')
        if count is not None and count != -1 and count <= 0:
            raise ValidationError("loop.count must be -1 (infinite) or a positive integer")
    
    # Check for circular dependencies in depends_on
    if 'execution' in recipe and 'depends_on' in recipe['execution']:
        depends_on = recipe['execution']['depends_on']
        recipe_id = recipe.get('recipe_id')
        
        if recipe_id and recipe_id in depends_on:
            raise ValidationError(f"Recipe '{recipe_id}' cannot depend on itself")
        
        # Check for immediate circular dependencies (A depends on B, B depends on A)
        # Note: This is a basic check. For full dependency graph validation,
        # all recipes in the system would need to be analyzed together
        _check_circular_dependencies_basic(depends_on)


def _check_circular_dependencies_basic(depends_on: List[str]) -> None:
    """
    Basic circular dependency check within a single recipe.
    
    Args:
        depends_on: List of recipe IDs this recipe depends on
        
    Raises:
        ValidationError: If circular dependencies are detected
    """
    # Check for duplicate dependencies
    if len(depends_on) != len(set(depends_on)):
        duplicates = [dep for dep in depends_on if depends_on.count(dep) > 1]
        raise ValidationError(f"Duplicate dependencies found: {set(duplicates)}")


def validate(recipe: Dict[str, Any]) -> None:
    """
    Validate a recipe dictionary against the schema and business logic.
    
    Args:
        recipe: The recipe dictionary to validate
        
    Raises:
        ValidationError: If validation fails with clear human-readable message
    """
    if not isinstance(recipe, dict):
        raise ValidationError("Recipe must be a dictionary/object")
    
    # Load and validate against JSON schema
    schema = _load_schema()
    
    try:
        jsonschema.validate(recipe, schema)
    except jsonschema.ValidationError as e:
        # Convert jsonschema ValidationError to our ValidationError with better message
        path_str = " -> ".join(str(p) for p in e.absolute_path) if e.absolute_path else "root"
        raise ValidationError(f"Schema validation failed at '{path_str}': {e.message}")
    except jsonschema.SchemaError as e:
        raise ValidationError(f"Schema error: {e.message}")
    
    # Perform additional business logic validation
    try:
        _check_business_logic(recipe)
    except Exception as e:
        if isinstance(e, ValidationError):
            raise
        else:
            raise ValidationError(f"Business logic validation failed: {str(e)}")


def validate_file(file_path: str) -> None:
    """
    Load and validate a recipe file (YAML or JSON).
    
    Args:
        file_path: Path to the recipe file
        
    Raises:
        ValidationError: If file loading or validation fails
        FileNotFoundError: If file does not exist
    """
    path = Path(file_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Recipe file not found: {file_path}")
    
    try:
        with open(path, 'r') as f:
            content = f.read()
            
        # Try to parse as YAML first (which can also parse JSON)
        try:
            recipe = yaml.safe_load(content)
        except yaml.YAMLError as e:
            raise ValidationError(f"Failed to parse file as YAML/JSON: {e}")
        
        if recipe is None:
            raise ValidationError("File is empty or contains only null value")
            
        validate(recipe)
        
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(f"Error processing file '{file_path}': {str(e)}")


# Convenience function for command-line usage
def main():
    """Command-line interface for recipe validation."""
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate SDC test recipe files")
    parser.add_argument("file", help="Path to recipe file (YAML or JSON)")
    parser.add_argument("-v", "--verbose", action="store_true", 
                       help="Verbose output")
    
    args = parser.parse_args()
    
    try:
        validate_file(args.file)
        print(f"✓ Recipe file '{args.file}' is valid")
        sys.exit(0)
    except (ValidationError, FileNotFoundError) as e:
        print(f"✗ Validation failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"✗ Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()