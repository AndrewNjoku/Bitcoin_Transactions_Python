'''
Created on 3 Dec 2018

@author: lolo
'''

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from datetime import datetime
from mr3px.csvprotocol import CsvProtocol

TRANSACTION_RE = re.compile(",")



class TransactionCount(MRJob):
    
   
    
    #HADOOP_OUTPUT_FORMAT = 'org.apache.hadoop.mapred.SequenceFileOutputFormat'
    
    #LIBJARS = ['hadoop.jar']
    
    def steps(self):
        return [
         MRStep(mapper=self.mapper_get_transactions,
                combiner=self.combiner_count_transactions,
                reducer=self.reducer_count_transactions)
              ]
    
        
 
    def mapper_get_transactions(self, _, line):
        
            # [0] Transaction ID (in the form of a hash)
            # [1] Block ID
            # [2] Date/Time ( in the form of unix seconds malacky)
              
        # yield each word in the line
        transaction = re.split(',', line)
    #transactionID = transaction[0] 
        timeStamp =float(transaction[2])
        #will return a date object instead of the string timestamp
        
        dt = datetime.utcfromtimestamp(timeStamp)
         
        stringDate = dt.strftime('%Y-%m') 
        
        #We are simply counting the amount of transactions so we dont really
        #need and value just a key we can use a combinator together with a simple 
        #one to count the amount of transactions for each month
            
        yield(stringDate, 1)
        

    def combiner_count_transactions(self, YearMonth, counts):
        # sum the words we've seen so far
        yield (YearMonth, sum(counts))

    def reducer_count_transactions(self, YearMonth, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        
        newKey = YearMonth.strip('"/')
          
          
        yield (newKey ,sum(counts))
        
 


    

if __name__ == '__main__':
    TransactionCount.run()

