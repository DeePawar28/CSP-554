from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRWordBigramCount(MRJob):

    def mapper(self, _, line):
        # Find all words in the line, converting to lowercase
        words = WORD_RE.findall(line.lower())
        
        # Iterate through the words and emit bigrams
        for i in range(len(words) - 1):
            bigram = words[i]+''+words[i+1]"
            yield bigram, 1

    def combiner(self, bigram, counts):
        # Combine the counts of bigrams
        yield bigram, sum(counts)

    def reducer(self, bigram, counts):
        # Reduce to get the final count of each bigram
        yield bigram, sum(counts)

if __name__ == '__main__':
    MRWordBigramCount.run()
