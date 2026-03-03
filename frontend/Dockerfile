FROM python:3.10-slim

# Cartella di lavoro dentro il container
WORKDIR /app

# Copio i requirements del frontend e installo le dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copio il codice sorgente e i template del frontend
COPY src/ ./src
COPY templates/ ./templates

# Comando di avvio del server frontend
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
