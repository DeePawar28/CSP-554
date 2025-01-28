from mrjob.job import MRJob

class MRSalaries2(MRJob):

    def mapper(self, _, line):
        # Split the line into the relevant fields
        (name, jobTitle, agencyID, agency, hireDate, annualSalary, grossPay) = line.split('\t')
        
        # Convert annual salary to float
        try:
            salary = float(annualSalary)
        except ValueError:
            # Skip if the salary can't be converted to float
            return

        # Categorize the salary into High, Medium, or Low
        if salary >= 100000.00:
            yield "High", 1
        elif 50000.00 <= salary < 100000.00:
            yield "Medium", 1
        else:
            yield "Low", 1

    def combiner(self, salary_level, counts):
        # Combine the counts for each salary level
        yield salary_level, sum(counts)

    def reducer(self, salary_level, counts):
        # Reduce the counts to the final number for each salary level
        yield salary_level, sum(counts)


if __name__ == '__main__':
    MRSalaries2.run()
