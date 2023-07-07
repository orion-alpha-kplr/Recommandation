# Utilisez une image de base contenant Python et Spark
FROM apache/spark:3.4.0

# Install any additional dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip

# Définir le répertoire de travail de l'application
WORKDIR /app

COPY ./app /app 
#copie dans l'image docker 

COPY ./requirements.txt  /app/requirements.txt

COPY ./app/ml-latest /ml-latest

# Installer les dépendances requises
RUN pip3 install -r requirements.txt
#RUN pip install --no-cache-dir -r /workspace/Recommandation/requirements.txt

#COPY . .

# Définir l'environnement Flask
#ENV FLASK_APP app.py

# Exposer le port sur lequel l'application Flask sera disponible
EXPOSE 5000

# Commande pour exécuter l'application Flask
CMD ["spark-submit", "server.py", "ml-latest/movies.csv", "ml-latest/ratings.csv"]