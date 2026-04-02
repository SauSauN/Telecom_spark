# On utilise une version spécifique (Bookworm) pour garantir la présence de Java 17
FROM python:3.9-slim-bookworm

# Installation de Java et utilitaires
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Définition de la variable d'environnement JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Création du répertoire de travail
WORKDIR /app

# Copie des dépendances et installation
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code source et des données
COPY . .

# Commande par défaut
CMD ["python", "main.py"]