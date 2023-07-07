from flask import Flask, Blueprint, render_template, request, jsonify
from engine import RecommendationEngine
import findspark
from pyspark import SparkContext, SparkConf

def create_app(spark_context, movies_set_path, ratings_set_path):
    # Initialise Spark
    findspark.init()
    
    # Crée une instance du moteur de recommandation
    engine = RecommendationEngine(spark_context, movies_set_path, ratings_set_path)
    
    # Crée une application Flask
    app = Flask(__name__)
    
    # Crée un Blueprint Flask
    main = Blueprint('main', __name__)
    
    # Définit la route principale
    @main.route("/", methods=["GET"])
    def home():
        return render_template("/app/templates/index.html")
    
    # Définit la route pour récupérer les détails d'un film
    @main.route("/movies/<int:movie_id>", methods=["GET"])
    def get_movie(movie_id):
        movie = engine.get_movie(movie_id)
        return jsonify(movie)
    
    # Définit la route pour ajouter de nouvelles évaluations pour les films
    @main.route("/newratings/<int:user_id>", methods=["POST"])
    def new_ratings(user_id):
        ratings = request.json
        engine.add_ratings(user_id, ratings)
        if engine.is_user_known(user_id):
            return jsonify(user_id)
        else:
            return jsonify("")
    
    # Définit la route pour ajouter des évaluations à partir d'un fichier
    @main.route("/<int:user_id>/ratings", methods=["POST"])
    def add_ratings(user_id):
        file = request.files['file']
        engine.add_ratings_from_file(user_id, file)
        return jsonify("Model prediction recalculated")
    
    # Définit la route pour obtenir la note prédite d'un utilisateur pour un film
    @main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
    def movie_rating(user_id, movie_id):
        rating = engine.predict_rating(user_id, movie_id)
        return str(rating)
    
    # Définit la route pour obtenir les meilleures évaluations recommandées pour un utilisateur
    @main.route("/<int:user_id>/recommendations", methods=["GET"])
    def get_recommendations(user_id):
        nb_movies = request.args.get('nb_movies')
        recommendations = engine.recommend_for_user(user_id, int(nb_movies))
        return jsonify(recommendations)
    
    # Définit la route pour obtenir les évaluations d'un utilisateur
    @main.route("/ratings/<int:user_id>", methods=["GET"])
    def get_ratings_for_user(user_id):
        ratings = engine.get_ratings_for_user(user_id)
        return jsonify(ratings)
    
    # Enregistre le Blueprint dans l'application
    app.register_blueprint(main)
    
    return app

if __name__ == "__main__":
    # Configuration de Spark
    conf = SparkConf().setAppName("MovieRecommender").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    
    # Chemins vers les ensembles de données
    movies_set_path = "/app/ml-latest/movies.csv"
    ratings_set_path = "/app/ml-latest/ratings.csv"
    
    # Crée l'application Flask
    app = create_app(sc, movies_set_path, ratings_set_path)
    
    # Exécute l'application Flask
    app.run(host="0.0.0.0", port=5000)
