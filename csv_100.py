import csv 

output_file = open("../files/movie_genres_100.csv", 'w')
csv_writer = csv.writer(output_file)

with open('../files/movie_genres.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    lines = 0
    for row in csv_reader:
        if lines >= 100:
            break
        else:
            csv_writer.writerow(row)
            lines += 1

output_file.close()