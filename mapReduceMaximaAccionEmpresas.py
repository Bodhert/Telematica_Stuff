from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):
    
    def mapper(self, key, value):
        (empresa,valor,fecha) = value.split(',');
        yield empresa,valor
            
    def reducer(self, empresa,valor):
        yield empresa, max(valor)

        
if __name__ == '__main__':
    MRWordFrequencyCount.run()
                    
