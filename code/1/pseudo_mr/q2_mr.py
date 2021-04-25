# read from file
map1(key, value):
    # key = null, value = line from ratings.csv
    x.split(',') # x = user_id, movie_id, rating, rating_timestamp
    emit(x.user_id, (x.rating, rating_counter=1))

# calculate number_of_ratings_per_user, sum_of_ratings_per_user
reduce1(key, values):
    # key = user_id, values = list of (rating, rating_counter=1)
    rating_sum = 0
    count_ratings = 0
    for v in values:
        rating_sum += v.rating
        count_ratings += v.rating_counter
    emit(key, (rating_sum, count_ratings))

# calculate average_rating_per_user
map2(key, value):
    # key = user_id, value = (rating_sum, count_ratings)
    val = value.rating_sum / value.count_ratings
    emit(key, val)

reduce2(key, values):
    # key = user_id, values = list of (average_rating_per_user)
    # len(values) == 1
    for avg_rating in values:
        emit(key, avg_rating)

# use results of stage 2 to calculate total users
# COUNT
map3.1(key, value):
    # key = user_id, value = avg_rating
    constant = "count_users"
    emit(constant, total_users_counter=1)

reduce3.1(key, values):
    # key = constant, values = list of (total_users_counter=1)
    total_users = 0
    for v in values:
        total_users += v.total_users_counter
    emit(total_users, null)

# use results from stage2. Filter users with avg_rating> 3.0
map3.2(key, value):
    # key = user_id, value = avg_rating
    if(value > 3.0):
        emit(key, value)

reduce3.2(key, values):
    # key = user_id, values = list of (avg_rating)
    # len(values) == 1
    for v in values:
        emit(ket, v)

# use results from stage 3.2 to calculate users with avg_rating> 3.0
# COUNT
map4(key, value):
    # key = user_id, value = avg_rating
    constant = "count_users"
    emit(constant, users_counter=1)

reduce4(key, values):
    # key = constant, values = list of (users_counter=1)
    users = 0
    for v in values:
        users += v.users_counter
    emit(users, null)

# Use the results of stages 3.1 and 4 to calculate percentage
# RESULT -> Percentage