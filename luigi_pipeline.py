import luigi
from get_movie_logs import get_movie_logs
from clean_movie_log import clean_movie_log


class FetchLogsTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget('records_sample.txt')

    def run(self):
        get_movie_logs('records_test.txt', 'records_sample.txt')


class CleanLogsTask(luigi.Task):
    def requires(self):
        return [FetchLogsTask()]

    def run(self):
        clean_movie_log('records_sample.txt', 'KafkaData', 'users.txt', 'movies.txt')


if __name__ == '__main__':
    luigi.run()
