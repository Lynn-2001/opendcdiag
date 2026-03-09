"""
OCP Report Output Module for SDC Testing Executor.

This module converts OpenDCDiag YAML output to OCP-compliant JSON format using the ocptv library,
with additional SDC-specific custom fields.

Usage Example:
    # From YAML file
    from sdc_executor.reporting.ocp_reporter import OCPReporter
    
    reporter = OCPReporter()
    ocp_json = reporter.convert_yaml_file('opendcdiag_output.yaml')
    print(ocp_json)
    
    # From parsed dict
    yaml_data = {...}  # Your parsed YAML data
    ocp_json = reporter.convert_yaml_dict(yaml_data)
    
    # Save to file
    reporter.convert_yaml_file('input.yaml', output_file='output.json')
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
import yaml

try:
    import ocptv
except ImportError:
    raise ImportError(
        "ocptv library not found. Install it with: pip install ocptv"
    )


class OCPReporter:
    """
    Converts OpenDCDiag YAML output to OCP-compliant JSON format.
    
    Maps OpenDCDiag fields to OCP format and adds custom SDC extension fields
    for Time To Failure (TTF), repeatability metrics, and footprint tax.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def convert_yaml_file(self, yaml_file_path: Union[str, Path], 
                         output_file: Optional[Union[str, Path]] = None) -> str:
        """
        Convert OpenDCDiag YAML file to OCP JSON format.
        
        Args:
            yaml_file_path: Path to the OpenDCDiag YAML file
            output_file: Optional path to save the JSON output
            
        Returns:
            OCP-compliant JSON string
        """
        yaml_path = Path(yaml_file_path)
        if not yaml_path.exists():
            raise FileNotFoundError(f"YAML file not found: {yaml_file_path}")
        
        try:
            with open(yaml_path, 'r', encoding='utf-8') as f:
                yaml_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse YAML file: {e}")
        
        ocp_json = self.convert_yaml_dict(yaml_data)
        
        if output_file:
            output_path = Path(output_file)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(ocp_json)
            self.logger.info(f"OCP JSON output saved to: {output_path}")
        
        return ocp_json
    
    def convert_yaml_dict(self, yaml_data: Dict[str, Any]) -> str:
        """
        Convert parsed OpenDCDiag YAML dictionary to OCP JSON format.
        
        Args:
            yaml_data: Parsed OpenDCDiag YAML data as dictionary
            
        Returns:
            OCP-compliant JSON string
        """
        if not yaml_data:
            raise ValueError("YAML data is empty or None")
        
        # Create OCP test run artifact
        ocp_data = self._build_ocp_structure(yaml_data)
        
        return json.dumps(ocp_data, indent=2, ensure_ascii=False)
    
    def _build_ocp_structure(self, yaml_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build the complete OCP JSON structure from OpenDCDiag data."""
        test_data = yaml_data.get('test', {})
        
        ocp_artifact = {
            "schemaVersion": {
                "major": 2,
                "minor": 0
            },
            "sequenceNumber": 1,
            "timestamp": self._safe_get_timestamp(test_data, 'start_time'),
            "testRunArtifact": {
                "testRunStart": self._build_test_run_start(test_data),
                "testRunEnd": self._build_test_run_end(test_data),
                "log": self._build_log_entries(test_data),
                "error": self._build_error_info(test_data)
            },
            "testStepArtifact": {
                "testStepStart": self._build_test_step_start(test_data),
                "testStepEnd": self._build_test_step_end(test_data),
                "measurement": self._build_measurements(test_data)
            }
        }
        
        # Remove None/empty sections
        ocp_artifact = self._clean_empty_fields(ocp_artifact)
        
        return ocp_artifact
    
    def _build_test_run_start(self, test_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build testRunStart section."""
        return {
            "name": self._safe_get(test_data, 'id', 'unknown_test'),
            "version": self._safe_get(test_data, 'version', '1.0.0'),
            "timestamp": self._safe_get_timestamp(test_data, 'start_time'),
            "metadata": {
                "testType": "hardware_validation"
            }
        }
    
    def _build_test_run_end(self, test_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build testRunEnd section with footprint tax extension."""
        result = self._map_test_result(test_data.get('result'))
        
        test_run_end = {
            "status": "COMPLETE",
            "result": result,
            "timestamp": self._safe_get_timestamp(test_data, 'end_time')
        }
        
        # Add SDC custom footprint tax field
        footprint_tax = self._safe_get(test_data, 'footprint_tax_percent')
        if footprint_tax is not None:
            test_run_end["footprint_tax_percent"] = {
                "value": float(footprint_tax),
                "unit": "%"
            }
        
        return test_run_end
    
    def _build_log_entries(self, test_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Build log entries from OpenDCDiag log data."""
        logs = test_data.get('log', [])
        if not isinstance(logs, list):
            return []
        
        ocp_logs = []
        for log_entry in logs:
            if not isinstance(log_entry, dict):
                continue
                
            ocp_log = {
                "severity": self._map_log_level(log_entry.get('level', 'INFO')),
                "message": self._safe_get(log_entry, 'message', ''),
                "timestamp": self._safe_get_timestamp(log_entry, 'timestamp')
            }
            ocp_logs.append(ocp_log)
        
        return ocp_logs
    
    def _build_error_info(self, test_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build error information if test failed."""
        error_msg = test_data.get('error')
        if not error_msg:
            return None
        
        return {
            "symptom": str(error_msg),
            "timestamp": self._safe_get_timestamp(test_data, 'end_time')
        }
    
    def _build_test_step_start(self, test_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build testStepStart section."""
        return {
            "name": f"{self._safe_get(test_data, 'id', 'unknown_test')}_step",
            "timestamp": self._safe_get_timestamp(test_data, 'start_time')
        }
    
    def _build_test_step_end(self, test_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build testStepEnd section."""
        return {
            "status": "COMPLETE",
            "timestamp": self._safe_get_timestamp(test_data, 'end_time')
        }
    
    def _build_measurements(self, test_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Build measurement entries including SDC custom fields."""
        measurements = []
        
        # Add CPU affinity as resource group measurement
        cpu_affinity = test_data.get('cpu_affinity')
        if cpu_affinity is not None:
            measurements.append({
                "name": "cpu_affinity",
                "value": str(cpu_affinity),
                "unit": "resource_group"
            })
        
        # Add SDC custom metrics
        
        # TTF (Time To Failure)
        ttf_seconds = self._calculate_ttf(test_data)
        measurements.append({
            "name": "ttf_seconds",
            "value": ttf_seconds,
            "unit": "s"
        })
        
        # Repeatability Rate
        repeatability = self._safe_get(test_data, 'repeatability_rate')
        if repeatability is not None:
            measurements.append({
                "name": "repeatability_rate",
                "value": float(repeatability),
                "unit": "ratio"
            })
        else:
            # Default to null if not provided
            measurements.append({
                "name": "repeatability_rate",
                "value": None,
                "unit": "ratio"
            })
        
        return measurements
    
    def _calculate_ttf(self, test_data: Dict[str, Any]) -> Optional[float]:
        """
        Calculate Time To Failure (TTF) in seconds.
        
        Returns elapsed time from test start until first failure is detected.
        Returns None if test passed with no failure.
        """
        result = test_data.get('result', '').lower()
        
        # If test passed, TTF should be null
        if result == 'pass':
            return None
        
        # Try to calculate TTF from timestamps
        start_time = test_data.get('start_time')
        end_time = test_data.get('end_time')
        
        if start_time and end_time:
            try:
                # Simple calculation - in real implementation, you might want
                # to parse actual timestamp formats
                if isinstance(start_time, (int, float)) and isinstance(end_time, (int, float)):
                    return float(end_time - start_time)
            except (ValueError, TypeError):
                pass
        
        # Look for explicit TTF field
        ttf = test_data.get('ttf_seconds')
        if ttf is not None:
            return float(ttf)
        
        # Default to null if cannot calculate
        return None
    
    def _map_test_result(self, result: Optional[str]) -> str:
        """Map OpenDCDiag result to OCP result format."""
        if not result:
            return "NOT_APPLICABLE"
        
        result_lower = str(result).lower()
        if result_lower == 'pass':
            return "PASS"
        elif result_lower == 'fail':
            return "FAIL"
        elif result_lower == 'skip':
            return "SKIP"
        else:
            return "NOT_APPLICABLE"
    
    def _map_log_level(self, level: str) -> str:
        """Map OpenDCDiag log level to OCP severity."""
        if not level:
            return "INFO"
        
        level_upper = str(level).upper()
        severity_map = {
            'DEBUG': 'DEBUG',
            'INFO': 'INFO',
            'WARN': 'WARNING',
            'WARNING': 'WARNING',
            'ERROR': 'ERROR',
            'CRITICAL': 'FATAL',
            'FATAL': 'FATAL'
        }
        
        return severity_map.get(level_upper, 'INFO')
    
    def _safe_get(self, data: Dict[str, Any], key: str, default: Any = None) -> Any:
        """Safely get a value from dictionary, handling missing keys gracefully."""
        return data.get(key, default)
    
    def _safe_get_timestamp(self, data: Dict[str, Any], key: str) -> Optional[str]:
        """Safely get timestamp, converting to ISO format if needed."""
        timestamp = data.get(key)
        if timestamp is None:
            return None
        
        # If already a string, return as-is (assuming it's properly formatted)
        if isinstance(timestamp, str):
            return timestamp
        
        # If numeric (Unix timestamp), convert to ISO format
        if isinstance(timestamp, (int, float)):
            try:
                from datetime import datetime
                return datetime.fromtimestamp(timestamp).isoformat() + 'Z'
            except (ValueError, OSError):
                return str(timestamp)
        
        return str(timestamp)
    
    def _clean_empty_fields(self, data: Any) -> Any:
        """Recursively remove None values and empty collections from data structure."""
        if isinstance(data, dict):
            cleaned = {}
            for key, value in data.items():
                cleaned_value = self._clean_empty_fields(value)
                if cleaned_value is not None and cleaned_value != [] and cleaned_value != {}:
                    cleaned[key] = cleaned_value
            return cleaned
        elif isinstance(data, list):
            return [self._clean_empty_fields(item) for item in data if item is not None]
        else:
            return data


def main():
    """Example usage of OCPReporter."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Convert OpenDCDiag YAML to OCP JSON')
    parser.add_argument('yaml_file', help='Input OpenDCDiag YAML file')
    parser.add_argument('-o', '--output', help='Output JSON file (default: stdout)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    
    try:
        reporter = OCPReporter()
        ocp_json = reporter.convert_yaml_file(args.yaml_file, args.output)
        
        if not args.output:
            print(ocp_json)
        else:
            print(f"OCP JSON written to: {args.output}")
            
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())