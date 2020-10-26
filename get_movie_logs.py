def get_movie_logs(input_file, output_file):
    with open(input_file, 'r') as file:
        lines = file.readlines()

    with open(output_file, 'w') as file:
        for i in range(10000):
            file.write(lines[i])


if __name__ == '__main__':
    get_movie_logs('records_test.txt', 'records_sample.txt')