'''from pyspark.sql.types import *
from pyspark.sql.functions import explode, col
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame'''
from pyspark.sql.types import *
from pyspark.sql.functions import explode, col
 
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
 
from pyspark.sql import SQLContext
 
import random

class RecommendationEngine:
    def __init__(self, spark_session, movies_set_path, ratings_set_path):#def init(self, spark_session, movies_path, ratings_path):
        self.spark = spark_session
        self.sqlContext = SQLContext(spark_session)
        self.movies_df = self.load_movies(movies_set_path)
        self.ratings_df = self.load_ratings(ratings_set_path)
        self.max_user_identifier = self.ratings_df.agg({"userId": "max"}).collect()[0][0]
        self.model = self.train_model()


    def load_movies(self, path):
        schema = StructType([
            StructField("movieId", IntegerType(), True),
            StructField("title", StringType(), True)
        ])
        return self.sqlContext.read.csv(path, header=True, schema=schema)

    def load_ratings(self, path):
        schema = StructType([
            StructField("userId", IntegerType(), True),
            StructField("movieId", IntegerType(), True),
            StructField("rating", FloatType(), True)
        ])
        return self.sqlContext.read.csv(path, header=True, schema=schema)

    def create_user(self, user_id=None):
        if user_id is None:
            user_id = self.max_user_identifier + 1
            self.max_user_identifier = user_id
        return user_id

    def is_user_known(self, user_id):
        return user_id is not None and user_id <= self.max_user_identifier

    def get_movie(self, movie_id=None):
        if movie_id is None:
            movie_id = self.movies_df.sample(False, 0.01).limit(1).select("movieId").collect()[0][0]
        return self.movies_df.filter(col("movieId") == movie_id)

    def get_ratings_for_user(self, user_id):
        return self.ratings_df.filter(col("userId") == user_id)

    def add_ratings(self, user_id, ratings):
        if isinstance(ratings, DataFrame):
            new_ratings_df = ratings
        else:
            new_ratings_df = self.sqlContext.createDataFrame(ratings, self.ratings_df.schema)
        self.ratings_df = self.ratings_df.union(new_ratings_df)
        self.model = self.train_model()


    def predict_rating(self, user_id, movie_id):
        rating_df = self.sqlContext.createDataFrame([(user_id, movie_id)], self.ratings_df.schema)
        prediction_df = self.model.transform(rating_df)
        predictions = prediction_df.select("prediction").collect()
        if predictions:
            return predictions[0][0]
        else:
            return -1

    def recommend_for_user(self, user_id, nb_movies):
        user_df = self.sqlContext.createDataFrame([(user_id,)], StructType([StructField("userId", IntegerType(), True)]))
        recommendations_df = self.model.recommendForUserSubset(user_df, nb_movies)
        recommended_movie_ids = recommendations_df.select(explode(col("recommendations.movieId"))).collect()
        recommended_movies_df = self.movies_df.join(recommended_movie_ids, self.movies_df.movieId == recommended_movie_ids.col)
        return recommended_movies_df.select("title")

    def train_model(self):
        als = ALS(maxIter=10, regParam=0.1, userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
        model = als.fit(self.ratings_df)
        return model

spark = SparkSession.builder.getOrCreate()
engine = RecommendationEngine(spark, movies_set_path, ratings_set_path) #(spark, "ml-latest/movies.csv", "ml-latest/ratings.csv")

# Exemple d'utilisation des méthodes de la classe RecommendationEngine
user_id = engine.create_user(None)
if engine.is_user_known(user_id):
    movie = engine.get_movie(None)
    ratings = engine.get_ratings_for_user(user_id)
    engine.add_ratings(user_id, ratings)
    prediction = engine.predict_rating(user_id, movie.movieId)
    recommendations = engine.recommend_for_user(user_id, 10)