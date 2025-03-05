FROM python:3.9

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./consumer

# CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]