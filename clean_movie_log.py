import re
from shutil import copyfile
import findspark
import pyspark
from pyspark.sql import SQLContext

"""
Data Cleaning
This notebook is converted from clean_movie_log.ipynb. It cleans and reorganzie the logs in records.txt using spark. 
This produces 
a. Two folders - userRating and userWatchLength, each containing a set of partition files about the users' ratings and users' watch history, a.k.a the time users spend on each movie <br>
b. Two files -  users.txt and movies.txt, containing user IDs and movie IDs that occurred in the past history.

Run python3 collect_and_clean_data/clean_movie_log.py
"""

def clean_movie_log(input_file, output_dir, output_user, output_movie):
    # Create spark context and read in the records.txt
    findspark.init()
    sc = pyspark.SparkContext(appName="hw")
    sqlContext = SQLContext(sc)
    print("Spark context started")

    recordsRDD = sc.textFile(input_file)

    # Separate different types of messages
    watchRDD = recordsRDD.filter(lambda rec: '/data/m' in rec)
    ratingRDD = recordsRDD.filter(lambda rec: '/rate/' in rec)
    recommendRDD = recordsRDD.filter(lambda rec: 'recommendation' in rec)

    # Parse users' watch history
    watchListRDD = watchRDD.map(lambda rec: rec.split(','))
    watchPairRDD = watchListRDD.map(lambda lst: (lst[1], lst[2].split('/')))
    userMoviePairRDD = watchPairRDD.map(lambda tup: ((tup[0], tup[1][3]), tup[1][4])).groupByKey().mapValues(list)
    userMoviePairRDD = userMoviePairRDD.map(lambda tup: (tup[0], tup[1][-1]))
    userMoviePairRDD = userMoviePairRDD.mapValues(lambda v: int(re.sub('[^0-9]', '', v)))
    userMoviePairRDD.saveAsTextFile(output_dir + '/userWatchLength')

    # Parse users' ratings
    ratingListRDD = ratingRDD.map(lambda rec: rec.split(','))
    ratingPairRDD = ratingListRDD.map(lambda lst: (lst[1], lst[2].split('/')))
    userRatingRDD = ratingPairRDD.map(lambda tup: (tup[0], tup[1][2].split('=')))
    userRatingPairRDD = userRatingRDD.map(lambda tup: ((tup[0], tup[1][0]), tup[1][1]))
    userRatingPairRDD.saveAsTextFile(output_dir + '/userRating')

    # Get all users IDs and movie IDs
    usersRDD = recordsRDD.map(lambda rec: rec.split(',')[1]).distinct().filter(lambda s: s.isdigit())
    allUsers = sorted(usersRDD.collect())
    with open(output_user, 'w') as file:
        for user in allUsers:
            file.writelines(user + '\n')

    watchedMovie = userMoviePairRDD.map(lambda tup: tup[0][1]).distinct()
    ratedMovie = userRatingPairRDD.map(lambda tup: tup[0][1]).distinct()

    allMoviesRDD = watchedMovie.union(ratedMovie).distinct()
    allMovies = allMoviesRDD.collect()
    with open(output_movie, 'w') as file:
        for movie in allMovies:
            file.write(movie + '\n')


if __name__ == '__main__':
    clean_movie_log('records_sample.txt', 'KafkaData', 'users.txt', 'movies.txt')
