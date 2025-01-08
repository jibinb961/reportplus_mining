import json
import os
import re

def load_json_file(file_path):
    """
    Load the JSON file and return its contents.
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except json.JSONDecodeError as e:
        print(f"Invalid JSON format: {e}")
        return None
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None


def validate_json_structure(data):
    """
    Validate the structure of the JSON to ensure it adheres to expected fields and types.
    Modify this function based on the expected schema of your BigQuery table.
    """
    issues = []
    
    # Example schema check (adjust according to your schema requirements)
    expected_fields = ['user_id', 'details', 'exams', 'subjects']
    
    # Check if each expected field is present and of the correct type
    for field in expected_fields:
        if field not in data:
            issues.append(f"Missing field: {field}")
        elif isinstance(data.get(field), dict) or isinstance(data.get(field), list):
            # For nested fields, you might need more validation
            continue
        else:
            if not isinstance(data.get(field), (dict, list)):
                issues.append(f"Field '{field}' is expected to be a dict or list, found {type(data.get(field))}")
    
    return issues


def validate_json_data(data):
    """
    Check the data for common issues such as missing fields, invalid values, etc.
    This is a placeholder for more detailed validation logic based on your requirements.
    """
    issues = []

    # Example validation: Ensure user_id exists and is a string
    if not isinstance(data.get('user_id'), str):
        issues.append("user_id must be a string")

    # Further validation for other nested fields can be added here
    if 'details' in data and not isinstance(data['details'], dict):
        issues.append("details must be a dictionary")

    # Example for validating nested 'exams' or 'subjects' fields:
    if 'exams' in data:
        if not isinstance(data['exams'], dict):
            issues.append("exams must be a dictionary")
        else:
            for exam_key, exam_value in data['exams'].items():
                if 'examname' not in exam_value:
                    issues.append(f"Exam '{exam_key}' is missing 'examname' field")
                if 'examstartdate' not in exam_value:
                    issues.append(f"Exam '{exam_key}' is missing 'examstartdate' field")
                if 'examenddate' not in exam_value:
                    issues.append(f"Exam '{exam_key}' is missing 'examenddate' field")
    
    if 'subjects' in data:
        if not isinstance(data['subjects'], dict):
            issues.append("subjects must be a dictionary")
        else:
            for subject_key, subject_value in data['subjects'].items():
                if 'subjectname' not in subject_value:
                    issues.append(f"Subject '{subject_key}' is missing 'subjectname' field")

    return issues


def check_json_for_bigquery(file_path):
    """
    Main function to check the JSON file for issues before uploading to BigQuery.
    """
    data = load_json_file(file_path)

    if data is None:
        return  # Exit if the JSON file is invalid or not found

    # Initialize an empty list to hold issues
    all_issues = []

    # Iterate through each record in the JSON file (if it's an array of records)
    if isinstance(data, list):
        for index, record in enumerate(data):
            record_issues = validate_json_data(record)
            if record_issues:
                all_issues.append(f"Record {index + 1} has issues: {record_issues}")
    elif isinstance(data, dict):  # If it's a single object
        record_issues = validate_json_data(data)
        if record_issues:
            all_issues.append(f"Root object has issues: {record_issues}")
    else:
        all_issues.append("Invalid root structure. Expected a dictionary or list of records.")

    # Validate the overall structure of the JSON
    structure_issues = validate_json_structure(data)
    if structure_issues:
        all_issues.extend(structure_issues)

    # Print out all issues found
    if all_issues:
        print("Issues found in the JSON file:")
        for issue in all_issues:
            print(f"- {issue}")
    else:
        print("No issues found. The JSON file is ready for BigQuery upload.")


if __name__ == "__main__":
    # Provide the path to your JSON file here
    json_file_path = '/Users/jibinbaby/Documents/Data_Engineering/ReportPlusDataMining/Sources/report_plus_data.json'

    # Run the validation
    check_json_for_bigquery(json_file_path)
