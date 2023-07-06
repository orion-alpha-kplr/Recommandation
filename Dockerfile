# Utilisez une image de base contenant Python et Spark
FROM apache/spark:3.4.0

# Définir le répertoire de travail de l'application
WORKDIR /app

COPY ./requirements.txt  /app/requirements.txt

# Installer les dépendances requises
RUN pip install -r requirements.txt
#RUN pip install --no-cache-dir -r /workspace/Recommandation/requirements.txt

COPY . .

# Définir l'environnement Flask
#ENV FLASK_APP app.py

# Exposer le port sur lequel l'application Flask sera disponible
EXPOSE 8080

# Commande pour exécuter l'application Flask
CMD ["spark-submit", "server.py", "ml-latest/movies.csv", "ml-latest/ratings.csv"]