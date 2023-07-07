# Importez les bibliothèques nécessaires
import time
import sys
import cherrypy
import os
from cheroot.wsgi import Server as WSGIServer
from pyspark import SparkConf
from pyspark.sql import SparkSession
from app import create_app

# Créez un objet SparkConf
conf = SparkConf().setAppName("movie_recommendation-server")

# Initialisez le contexte Spark
sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])

# Obtenez les chemins des jeux de données des films et des évaluations à partir des arguments de la ligne de commande
movies_set_path = sys.argv[1] if len(sys.argv) > 1 else ""
ratings_set_path = sys.argv[2] if len(sys.argv) > 2 else ""

# Créez l'application Flask
app = create_app(sc, movies_set_path, ratings_set_path)

# Configurez et démarrez le serveur CherryPy
cherrypy.tree.graft(app.wsgi_app, '/')
cherrypy.config.update({
    'server.socket_host': '0.0.0.0',
    'server.socket_port': 5000,
    'engine.autoreload.on': False
})
cherrypy.engine.start()

# Démarrez le serveur WSGI CherryPy
WSGIServer(('0.0.0.0', 5000), cherrypy.tree).start()