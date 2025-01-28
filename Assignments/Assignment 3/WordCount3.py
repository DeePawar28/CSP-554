from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRWordLengthCount(MRJob):

    def mapper(self, _, line):
        # Find all words in lowercase
        words = WORD_RE.findall(line.lower())
        # Yield the length of each word
        for word in words:
            yield len(word), 1

    def combiner(self, word_length, counts):
        # Combine counts for words of the same length
        yield word_length, sum(counts)

    def reducer(self, word_length, counts):
        # Reduce counts for words of the same length
        yield word_length, sum(counts)

if __name__ == '__main__':
    MRWordLengthCount.run()
