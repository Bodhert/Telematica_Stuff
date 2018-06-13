from mrjob.job import MRJob
from mrjob.step import MRStep

class MRM(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.get_movies_ocurrences,
                   reducer=self.reducer_movies_ocurrences)
        ]

    def get_movies_ocurrences(self, key, value):
        (user_id,movie_id,rating,genero,date) = value.split(',');
        yield user_id,int(rating)
        
    def reducer_movies_ocurrences(self, user_id, ocurrences):
	lratings = list(ocurrences)
	contador = len(lratings)
	promedio = sum(lratings) / contador
        yield user_id, (contador,promedio)
    
        
        
if __name__ == '__main__':
    MRM.run()
                    
