from mrjob.job import MRJob
from mrjob.step import MRStep

class MRM(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.get_movies_ocurrences,
                   reducer=self.reducer_movies_ocurrences),
            MRStep(reducer=self.reducer_output)
        ]

    def get_movies_ocurrences(self, key, value):
        (user_id,movie_id,rating,genero,date) = value.split(',');
        yield date,int(movie_id)
        
    def reducer_movies_ocurrences(self, date, ocurrences):
        yield None, (sum(1 for _ in ocurrences), date)
    
    def reducer_output(self, _, values):
        # max value from th queue of tuple
        yield min(values)

        
if __name__ == '__main__':
    MRM.run()
                    
