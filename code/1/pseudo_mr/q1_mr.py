# read from file
map1(key, value):
    # key = null, value = line from movies.csv
    x = value.split(',') 
    #  x = (
    #     movie_id, title, abstract, release_date, 
    #     movie_length, production_cost, earnings, publicity
    #  )
    movie_info = tuple(x[1:])
    emit(x.movie_id, movie_info)
        
reduce1(key, values):
    # key = movie_id, values = list of (movie_info)
    # len(values == 1)
    for movie_info in values:
        emit(key, movie_info)

# filter with conditions
map2(key, value)
    # key = movie_id, value = movie_info
    release_year = value.release_date.split('-')[0]
    if(release_year >= 2000 and value.production_cost and value.earnings):
        earnings_percentage = ((earnings - production_cost)*100 / production_cost)
        emit(release_year, (key, value.title, earnings_percentage))

sortByKey2()

#  calculate max percentage
reduce2(key, values):
    # key = release_year, values = list of (movie_id, title, earnings_percentage)
    res = tuple()
    for v in values:
        if(resIsEmptyTuple() or res.earnings_percentage > v.earnings_percentage):
            res = res
        elif(res.earnings_percentage < v.earnings_percentage):
            res = v
        elif(res.earnings_percentage == v.earnings_percentage):
            if(res.movie_id < v.movie_id):
                res = res
            elif(res.movie_id > v.movie_id):
                res = v

# RESULT -> (Year, (Movie_id, Title, Profit))