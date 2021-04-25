# read from file and extract only Drama movies
map1(key, value):
    # key = null, value = line from movie_genres.csv
    x = value.split(',')
    # x = movie_id. genre
    if(x.genre == 'Drama'):
        emit(x.movie_id, x.genre)

reduce1(key, values):
    # key = movie_id, values = list of (genre)
    # len(value) == 1
    for v in values:
        emit(key, v)
# All essential data from movie_genres.csv are gathered

# read from file
map2(key, value):
    # key = null, value = line from movies.csv
    x = value.split(',')
    #  x = (
    #     movie_id, title, abstract, release_date, 
    #     movie_length, production_cost, earnings, publicity
    #  )
    emit(x.movie_id, (x.release_date, x.abstract))

reduce2(key, values):
    # key = movie.id, values = list of (release_date, abstract)
    # len(values) == 1
    for v in values:
        emit(key, v)

# filter with conditions
map3(key, value):
    # key = movie_id, value = (release_date, abstract)
    year = value.release_date.split('-')[0]
    if(value.abstract and  year >= 2000):
        val = (year, len(value.abstract.split(), movie_counter=1)
        emit(key, val)

reduce3(key, values):
    # key = movie_id, values = list of (year, words_per_abstract, movie_count=1)
    # len(values) == 1
    for v in values:
        emit(key, v)
# All essential data from movies.csv are gathered

MR stage 4
# join stage 3 data with stage 1 data (join_key = key) to keep only Drama movies
# -> result in (movie_id, (release_year, abstract_words, movie_count=1, genre=Drama))

# rearrange with key as quinquennium and calculate total_abstract_length, total_drama_movies
map5(key, value):
    # key = movie_id, value = (release_year, abstract_words, movie_count=1, genre=Drama)
    temp = (x.realease_year//5)*5
    newKey = str(temp) + '-' + str(temp+4)
    val = (abstract_words, movies_count)
    emit(newKey, val)

reduce5(key, values):
    # key = quinquennium, values = list of (abstract_words, movies_count)
    total_abstract_length = 0
    total_drama_movies = 0
    for v in values:
        total_abstract_length += v.abstract_words
        total_drama_movies += v.movies_count
    emit(key, (total_abstract_length, total_drama_movies))

# calculate average_abstract_length for Drama genre
map6(key, value):
    # key = quinquennium, value = (total_abstract_length, total_drama_movies)
    avg_abstract_length = value.total_abstract_length / value.total_drama_movies
    emit(key, avg_abstract_length)

sortByKey6()

reduce6(key, values):
    # key = quinquennium, values = list of (total_abstract_length, total_drama_movies)
    # len(values) == 1
    for v in values:
        emit(key, v)

# RESULT -> (Five Years, Average words in Abstract)