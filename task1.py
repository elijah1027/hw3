from mrjob.job import MRJob
import time
import re
import operator



class MRWordCounter(MRJob):
    def mapper(self, key, line):
        line_split = re.compile(r"[A-Za-z]+").findall(line)
        for word in line_split:
            yield word[0].upper(),1            
            

    def reducer(self, word, counts):
        total = sum(counts)
        yield word, total

if __name__ == '__main__':
    #run: python count_word.py -o 'output_dir' --no-output 'location_input_file or files'
    #e.g. python count_word.py -o 'results_count_word' --no-output 'data_count_word/*.txt'
    st = time.time()
    MRWordCounter.run()
    end = time.time()
    print (end - st)
