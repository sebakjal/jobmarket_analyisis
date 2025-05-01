FROM apache/airflow:3.0.0-python3.11

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Agrega el folder de python scripts al path de python del container
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/python_scripts"