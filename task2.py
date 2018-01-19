from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import re
import operator


class MRWordCounter(MRJob):
    def steps(self):
        return [
                MRStep(mapper=self.mapper,
                       
                       reducer=self.reducer_step1),
                MRStep(reducer=self.reducer_alpha)
            ]
    def mapper(self, key, line):
        line_split = re.compile(r"[A-Za-z]+").findall(line)
        for word in line_split:
            yield word,1            
            
    def reducer_step1(self, word, counts):
        yield None, (sum(counts),word)
    
    def reducer_alpha(self, _, pairs):
        lst = []
        for v in pairs:
            lst.append(v)
        lst.sort(reverse=True)
        for i in range(0,10,1):
            
            yield lst[i]

if __name__ == '__main__':
    #run: python count_word.py -o 'output_dir' --no-output 'location_input_file or files'
    #e.g. python count_word.py -o 'results_count_word' --no-output 'data_count_word/*.txt'
    st = time.time()
    MRWordCounter.run()
    end = time.time()
    print (end - st)
