FROM python:3.10-slim

# Imposta la directory di lavoro
WORKDIR /app

# Installa le dipendenze di sistema necessarie per il connettore MariaDB.
RUN apt-get update && apt-get install -y \
    gcc \
    libmariadb-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copia i requirements e installa le dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice sorgente
COPY src/ ./src

# Espone la porta richiesta
EXPOSE 8000

# Avvia il server. 
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]