# read information from all files
map0.1(key, value) # movie_genres
    # key = null, value = line from movie_genres.csv
    x = value.split(',')
    # x = movie_id, genre
    emit(x.movie_id, x.genre)

reduce0.1(key, values):
    # key = movie_id, values = list of (genre)
    for v in values:
        emit(key, v)

map0.2(key, value) #movies
    # key = null, value = line from movies.csv
    x = value.split(',')
    #  x = (
    #     movie_id, title, abstract, release_date, 
    #     movie_length, production_cost, earnings, publicity
    #  )
    val = (x.popularity, x.movie_name)
    emit(x.movie_id, val)

reduce0.2(key, values):
    # key = movie_id, values = list of (popularity, name)
    for v in values:
        emit(key, v)

map0.3(key, value) #ratings
    # key = null, value = line from ratings.csv
    x = value.split(',')
    # x = user_id, movie_id, rating, rating_timestamp
    val = (x.user_id, x.rating, ratings_counter=1)
    emit(x.movie_id, val)

reduce0.3(key, values):
    # key = movie_id, values = list of (user_id, rating, ratings_counter=1)
    for v in values:
        emit(key, v)

map0.4(key, value) #ratings2 
    # key = movie_id, value = (user_id, rating, ratings_counter=1)
    val = (key, value.rating)
    emit(value.user_id, val)

reduce0.4(key, values):
    # key = movie_id, values = list of (user_id, rating, ratings_counter=1)
    for v in values:
        emit(key, v)

# start by finding the user with the most movie ratings per genre
MR stage 1
# # join stage 0.1 data with stage 0.3 data (genres.join(ratings))
# -> result in (movie_id, (genre, user_id, rating, ratings_counter=1))

# calculate ratings_of_user_in_genre
map2(key, value):
    # key = movie_id, value = (genre, user_id, ratings_counter=1)
    newKey = (value.genre, value.user_id)
    emit(newKey, value.ratings_counter=1)

reduce2(key, values):
    # key = (genre, user_id), values = list of ratings_counter=1
    ratings_of_user_in_genre = 0
    for v in values:
        ratings_of_user_in_genre += ratings_counter
    emit(key, ratings_of_user_in_genre)

# rearrange elements
map3(key, value):
    # key = (genre, user_id), value = ratings_of_user_in_genre
    newKey = key.genre
    val = (key.user_id, value)
    emit(newKey, val)

# keep users with most ratings per genre. 
# all users with the same amount of max ratings in genre will move to the next map reduce stage
reduce3(key, values)
    # key = genre, values = list of (user_id, ratings_of_user_in_genre)
    res = tuple() #Holds tuple of users with max_number_of_ratings at the moment
    for v in values:
        if(resIsEmptyTuple() or res[0].ratings_of_user_in_genre == v.ratings_of_user_in_genre):
            res += v # keep users with same number of_ratings
        elif(res[0].ratings_of_user_in_genre < v.ratings_of_user_in_genre)
            res = tuple(v)
            # all previous values of res have the same number of ratings, which is smaller than v's
        elif(res[0].ratings_of_user_in_genre > v.ratings_of_user_in_genre)
            res = res # keep previous users
    # emit all distinct users with maximum number of ratings in genre -> flatmap in rdd code
    for r in res:
        emit(key, r)

# rearrange with user_id as key
map4(key, value):
    # key = genre, value = (user_id, ratings_of_user_in_genre)
    newKey = value.user_id
    val = (key, value.ratings_of_user_in_genre)
    emit(newKey, val)

reduce4(key, values):
    # key = user_id, values = genre, ratings_of_user_in_genre
    # len(values) == 1
    for v in values:
        emit(key, v)

# create the dataset to find most and least favorite movies of user in the genre
MR stage 5
# # join stage 4 data with stage 0.4 data (ratings2)
# -> result (user_id, (genre, num_ratings, movie_id, rating))

map6(kay, value):
    # key = user_id, value = (genre, num_ratings, movie_id, rating)
    newKey = value.movie_id
    var = (value.genre, key, value.rating, value.num_ratings)
    emit(newKey, var)

reduce6(key, value):
    # key = movie_id, value = (category, user_id, rating, num_ratings)
    # len(values) == 1
    for v in values:
        emit(key, v)

MR stage 7
# # join stage 6 data with stage 0.1 data (genres)
# -> result (movie_id, genre, user_id, rating, num_ratings, category)
#  genre = genre in witch user has max ratings

# filter to keep only the genre in witch user has max ratings
map8(kay, value):
    # key = movie_id, value = (movie_id, genre, user_id, rating, num_ratings, category)
    if(value.genre == value.category)
        emit(newKey, value)

reduce8(key, value):
    # key = movie_id, value = (movie_id, genre, user_id, rating, num_ratings, category)
    # len(values) == 1
    for v in values:
        emit(key, v)

MR stage 9
# join stage 8 data with stage 0.2 data (movies)
# -> result (movie_id, category, user_id, rating, num_ratings, movie_name, popularity)

map10(kay, value):
    # key = movie_id, 
    # value = (category, user_id, rating, num_ratings, movie_name, popularity)
    newKey = (category, user_id, num_ratings)
    var = (movie_id, movie_name, rating, popularity)
    emit(newKey, var)

reduce10(key, value):
    # key = (category, user_id, num_ratings)
    # values = list of (movie_id, movie_name, rating, popularity)
    # len(values) == 1
    for v in values:
        emit(key, v)

# at this point all essential data have been selected
# continue by calculating most favorite and least favorite movie
# use popularity as tie breaker

# take data of stage 10
map11.1(key, value): #most_favorite
    # key = (category, user_id, num_ratings)
    # value = (movie_id, movie_name, rating, popularity)
    emit(key, value)

reduce11.1(key, values):
    # key = (category, user_id, num_ratings)
    # values = list of (movie_id, movie_name, rating, popularity)
    res = tuple() # always one value inside res
    for v in values:
        if(resIsEmptyTuple() or res.rating > v.rating):
            res = res
        elif (res.rating < v.rating):
            res = v
        elif (res.rating == v.rating):
            res = res if(res.popularity >= v.popularity) else v
    emit(key, res)

# take data of stage 10
map11.2(key, value): #least_favorite
    # key = (category, user_id, num_ratings)
    # value = (movie_id, movie_name, rating, popularity)
    emit(key, value)

reduce11.2(key, values):
    # key = (category, user_id, num_ratings)
    # values = list of (movie_id, movie_name, rating, popularity)
    res = tuple() # always one value inside res
    for v in values:
        if(resIsEmptyTuple() or res.rating > v.rating):
            res = v
        elif (res.rating < v.rating):
            res = res
        elif (res.rating == v.rating):
            res = res if(res.popularity >= v.popularity) else v
    emit(key, res)

MR stage 12
# join stage 11.1 data with stage 11.2 data
# -> (category, user_id, $ratings, most_fav_movie_id, most_fav_movie_name, rating, least_fav_movie_id, least_fav_movie_name, rating)
