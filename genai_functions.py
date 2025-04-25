import google.generativeai as genai
from config import API_KEY, model_version
import time
import json

# Configure the Gemini API client
genai.configure(api_key=API_KEY)

# Choose the Gemini model
model = genai.GenerativeModel(model_version)

# Serionity levels
SENIORITY_LEVELS = ["Junior", "Mid-Senior", "Senior", "Lead or greater"]
IN_ENGLISH = ["Yes", "No"]
REQUIRES_DEGREE_IT = ["Yes", "No"]
MENTIONS_CERTIFICATIONS = ["Yes", "No"]
YEARS_OF_EXPERIENCE = ["0-1", "1+", "2+", "3+", "5+", "7+", "Not Specified"] 
CLARITY_LEVELS  = ["High", "Medium", "Low"]
CLOUD_PREFERENCES = ["AWS", "Azure", "GCP", "Other Cloud", "Multiple Clouds", "No Preference", "No Mention"]
SKILLS_WANTED  = ["Databricks or snowflake",
                 "Develop pipelines or ETL/ELT processes",
                 "Data modeling",
                 "Data analysis or visualization",
                 "Data quality",
                 "Data governance",
                 "Knowledge of Machine Learning or MLOps",
                 "CI/CD",
                 "Collaboration with data scientists or analysts",
                 "Automation/Orchestration (Airflow, Prefect, Dagster, etc.)",
                 "Data monitoring",
                 "Migration",
                 "Version control (GIT or similar)",
                 "APIs",
                 "Spark knowledge",
                 "None of the above mentioned skills",
                 ]

EXPECTED_KEYS = [
    "task_clarity",
    "seniority_level_ai",
    "requires_degree_it",
    "mentions_certifications",
    "years_of_experience",
    "is_in_english",
    "cloud_preference",
    "skills_mentioned" 
]

# --- Helper to clean potential JSON output ---
def clean_json_string(raw_string: str) -> str:
    """Attempts to extract a JSON object from a string that might contain extra text."""
    # Look for the first '{' and the last '}'
    start = raw_string.find('{')
    end = raw_string.rfind('}')
    if start != -1 and end != -1 and end > start:
        json_candidate = raw_string[start:end+1]
        # Basic check for plausible JSON structure (can be improved)
        if json_candidate.count('{') == json_candidate.count('}') and \
           json_candidate.count('[') == json_candidate.count(']'):
            return json_candidate


def classify_job_description(job_description: str,
                             delay: int = 5,
                             max_retries: int = 3) -> str:
    
    prompt = f"""
    Analyze the following job description based on the criteria below.
    Provide the output STRICTLY as a JSON object containing ONLY the keys specified.
    Do NOT include any introductory text, explanations, or markdown formatting like ```json.

    Job Description:
    ---
    {job_description}
    ---

    Classification Criteria and Allowed Values:

    1.  "task_clarity": How clear are the specific tasks and responsibilities?
        Allowed values: {CLARITY_LEVELS}
    2.  "seniority_level_ai": What is the implied seniority based on the description?
        Allowed values: {SENIORITY_LEVELS}
        Interpret context (e.g., "lead role", "entry-level").
    3.  "requires_degree_it": Does it explicitly require a Bachelor's or higher degree in IT, Computer Science, or a related STEM field?
        Allowed values: {REQUIRES_DEGREE_IT}
        If not mentioned, use "No".
    4.  "mentions_certifications": Does it mention specific certifications (e.g., AWS Certified, Azure Data Engineer, PMP) as required or preferred?
        Allowed values: {MENTIONS_CERTIFICATIONS}
    5.  "years_of_experience": What is the minimum years of experience mentioned?
        Allowed values: {YEARS_OF_EXPERIENCE}
        Interpret phrases like "at least 3 years" as "3+", "5-7 years" as "5+". If not mentioned, use "Not Specified".
    6.  "is_in_english": Is the primary language of the description English or its mentioned that the job will need you to communicate in English?.
        Allowed values: {IN_ENGLISH}
    7.  "cloud_preference": What is the main cloud platform focus?
        Allowed values: {CLOUD_PREFERENCES}
        Prioritize: Specific cloud (AWS/Azure/GCP) > Multiple Clouds > Other Cloud > No Preference (if cloud mentioned but general) > No Mention.
    8.  "skills_mentioned": Identify which skills from the provided list are mentioned or strongly implied in the description. Output these as a JSON list of strings.
        Skill list: {SKILLS_WANTED}
        If none from the list are clearly mentioned, provide an empty list [].


    Required JSON Output Format (example):
    {{
      "task_clarity": "Medium",
      "seniority_level_ai": "Senior",
      "requires_degree_it": "Yes",
      "mentions_certifications": "No",
      "years_of_experience": "5+",
      "is_in_english": "Yes",
      "cloud_preference": "AWS",
      "skills_mentioned": ["Develop pipelines or ETL/ELT processes", "APIs", "AWS"]
    }}

    Provide ONLY the JSON object below:
    """
    
    for attempt in range(max_retries):
        time.sleep(delay)
        try:
            print(f"Attempt {attempt + 1}/{max_retries}: Calling Gemini API...")
            response = model.generate_content(prompt)

            raw_output = response.text.strip()
            # print(f"Raw API Response (Attempt {attempt + 1}):\n{raw_output}") # Log raw response for debugging

            # Attempt to clean and parse the JSON
            cleaned_output = clean_json_string(raw_output)
            parsed_json = json.loads(cleaned_output)

            # --- Validation of Parsed JSON ---
            if not isinstance(parsed_json, dict):
                print(f"Error: Parsed output is not a dictionary. Attempt {attempt + 1}")
                raise ValueError("Parsed output is not a dictionary") # Trigger retry or fail

            # Check for missing keys (optional but recommended)
            missing_keys = [key for key in EXPECTED_KEYS if key not in parsed_json]
            if missing_keys:
                 print(f"Warning: Parsed JSON is missing expected keys: {missing_keys}. Attempt {attempt + 1}")
                 # Decide if you want to retry or accept partial data. Here we'll retry.
                 raise ValueError(f"Missing keys in JSON response: {missing_keys}")

            print(f"Successfully parsed JSON (Attempt {attempt + 1}).")
            return parsed_json # Return the dictionary

        except json.JSONDecodeError as json_e:
            print(f"API Error (Attempt {attempt + 1}/{max_retries}): Failed to decode JSON.")
            print(f"JSONDecodeError: {json_e}")
            print(f"Raw Response that failed parsing: {raw_output}")
            # Optionally try more aggressive cleaning here if needed
            error_message = f"JSONDecodeError: {json_e}"

        except Exception as e:
            # Catch other potential API errors 
            print(f"API Error (Attempt {attempt + 1}/{max_retries}): {e}")
            error_message = str(e)

        # If loop continues, it means an error occurred
        if attempt < max_retries - 1:
            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)

        else:
            print("Error: Max retries reached. Failed to classify job description.")
            # Log the last error encountered
            print(f"Last error message: {error_message}")
            return None # Return None after exhausting retries

    return None

