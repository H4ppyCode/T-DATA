# Utiliser une image Python de base
FROM python:3.12-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier le fichier requirements.txt dans le conteneur
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier le script crypto_producer.py dans le conteneur
COPY src/crypto-kafka/crypto_producer.py .

# Exposer le port pour Prometheus
EXPOSE 8000

# Définir la commande par défaut pour exécuter le script
CMD ["sh", "-c", "sleep 10 && python -u crypto_producer.py"]