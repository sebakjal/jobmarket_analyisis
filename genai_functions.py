import google.generativeai as genai
from config import API_KEY, model_version
import time

# Configure the Gemini API client
genai.configure(api_key=API_KEY)

# Choose the Gemini model
model = genai.GenerativeModel(model_version)

# Serionity levels
seniority_levels = ["Junior",
                     "Mid-Senior",
                     "Senior",
                     "Lead"
                     ]

in_english = ["Yes", "No"]

requires_degree_in_IT = ["Yes", "No"]

mention_certifications = ["Yes", "No"]

years_of_experience = ["0-1", "1+", "2+", "3+", "5+", "7+"]

clarity = ["High", "Medium", "Low"]

cloud_preference = ["AWS", "Azure", "GCP", "No preference", "No mention"]

skills_wanted = ["Databricks or snowflake",
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

def classify_job_description(job_description: str) -> str:
    
    prompt = f"""

    Analyze the following job description and classify the next things:
    -The clarity of the tasks.
    -The seniority level of the job.
    -If the job requires a degree in IT or related field.
    -If the job mentions certifications.
    -The years of experience required.
    -If the description is in English or mentions that the job is in English.
    -If the job is more focused on one cloud or another, or if it doesn't mention any cloud preference or doesn't mention any cloud at all.
    -If the job description mentions any of the skills from a list of wanted skills.

    The task clarity can be in one of the following levels: {clarity}
    The seniority level can be in one of the following levels: {seniority_levels}
    The IT degree requirement can be one of the following values: {requires_degree_in_IT}
    The certification mention can be one of the following values: {mention_certifications}
    The years of experience can be one of the following values: {years_of_experience}
    The language can be one of the following values: {in_english}
    The cloud preference can be one of the following values: {cloud_preference}
    The skills wanted can be one or more of the following values: {skills_wanted}

    Job description: {job_description}

    Output the answer in the following format:
    Task clarity: <clarity>
    Seniority level: <seniority_level>
    Requires degree in IT: <requires_degree_in_IT>
    Mentions certifications: <mentions_certifications>
    Years of experience: <years_of_experience>
    In English: <in_english>
    Cloud preference: <cloud_preference>
    Skills wanted: <skills_wanted>

    """
    
    try:
        response = model.generate_content(prompt)
        classification = response.text.strip()
        print(f"Classification:\n {classification}")
        return classification

    except Exception as e:
        print(f"Error: {e}")
        classification = None

    return "API_Error"

