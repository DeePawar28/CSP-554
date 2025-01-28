from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRWordCount(MRJob):

	def mapper(self, _, line):
		for word in WORD_RE.findall(line): 
			a_to_n = "abcdefghijklmn" 
			if any(word[0] ==w for w in a_to_n): 
				yield "a_to_n", 1	
			else: 
				yield "other", 1

    	def combiner(self, word, counts):
        	yield word, sum(counts)

    	def reducer(self, word, counts):
        	yield word, sum(counts)

if __name__ == '__main__':
    MRWordCount.run()


