import os

# Keep first 100 line of movie_genres.csv

if not os.path.exists('../../movies/reduced_genres.csv'):
    os.makedirs('../../movies/reduced_genres.csv')
else:
    os.remove('../../movies/reduced_genres.csv')

with open('../../movies/movie_genres.csv', 'r') as read_file:
    lines = read_file.readlines()

count = 0
with open('../../movies/reduced_genres.csv', 'a') as write_file:
    while(count <= 99):
        write_file.write(lines[count])
        count += 1
