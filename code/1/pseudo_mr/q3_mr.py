# read from file
map1(key, value)
    # key = null, value = line from ratings.csv
    x = value.split(',')
    # x = user_id, movie_id, rating, rating_timestamp
    emit(x.movie_id, (x.rating, rating_counter=1))

# calculate sum_of_ratings_per_movie and number_of_ratings_per_movie
reduce1(key, values)
    # key = movie_id, values = (rating, rating_counter=1)
    count_ratings = 0
    rating_sum = 0
    for v in values:
        count_ratings += v.rating_counter
        rating_sum += v.rating
    emit(key, (rating_sum, count_ratings))

# calculate average_rating_per_movie
map2(key, value):
    # key = movie_id, value = (rating_sum, count_ratings)
    average_rating_per_movie = value.rating_sum / value.total_rates
    emit(key, average_rating_per_movie)

reduce2(key, values):
    # key = movie_id, values = list of (average_rating_per_movie)
    # len(values) == 1
    for v in values:
        emit(key, v)
# All essential data from ratings.csv are gathered

# read from file
map3(key, value):
    # key = null, value = line from movie_genres.csv
    x = value.split(',')
    # x = movie_id, genre
    emit(x.movie_id, x.genre)

reduce3(kay, values):
    #key = movie_id, value = list of (genre)
    for v in values:
        emit(key, v)

MR stage 4
# join stage 3 data with stage 2 data (join_key = key) 
# -> result in (movie_id, (genre, avg_rating_per_movie))

# rearrange with genre as key and calculate sum_of_avg_ratings and movies_in_genre
map5(key, value):
    # key = movie_id, value = (genre, avg_rating_per_movie)
    new_key = value.genre
    emit(new_key, (value.avg_rating_per_movie, movies_in_genre_counter=1))

reduce5(key, values):
    # key = genre, values = list of (avg_rating_per_movie, movies_in_genre_counter=1)
    sum_of_avg_ratings = 0
    count_movies_in_genre = 0
    for v in values:
        sum_of_avg_ratings += v.avg_rating_per_movie
        count_movies_in_genre += v.movies_in_genre_counter
    emit(key, (sum_of_avg_ratings, count_movies_in_genre))

# calculate genre_rating
map6(key, value):
    # key = genre, value = (sum_of_avg_ratings, count_movies_in_genre)
    genre_rating = sum_of_avg_ratings/count_movies_in_genre
    emit(key, (genre_rating, count_movies_in_genre))

sortByKey6()

reduce6(key, values):
    # key = genre, values = list of (genre_rating, count_movies_in_genre)
    # len(values) == 1
    for v in values:
        emit(key, v)

# RESULT -> (Genre, (Genre rating, Movies in genre))