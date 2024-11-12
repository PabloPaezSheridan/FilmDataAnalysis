from mrjob.job import MRJob

class LanguageBudgetCountries(MRJob):
    
    def to_float(self,number):
        try:
            return float(number)
        except:
            return 0
        
    
    def mapper(self, _, line):
        fields = line.split('|')
        language = fields[1]
        country = fields[3] 
        budget = self.to_float(fields[4]) 

        if language not in['','-1'] and country != ['','-1'] and budget != 0:
            yield language, (country, budget)
            
    def reducer(self, language, values):
        countries = []
        total_budget = 0
        for country, budget in values:
            if (country not in countries):
                countries.append(country)
            total_budget += budget
        yield language, (countries, total_budget)
        
if __name__ == '__main__':
    LanguageBudgetCountries.run()