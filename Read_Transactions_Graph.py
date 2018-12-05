'''
Created on 4 Dec 2018

@author: lolo
This module is intended to read a sequencefile containing the X and Y coordinates: being the date and the quantity
of transactions
'''

import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np


class plotTransactionGraph():
    
    
    #Will read the sequence file containing the neccesssery transaction plot information
    #put the path here , Default is for my localmachine test Hadoop setup.
    #When running on ITL machines use the following HDFS path instead: hdfs://studoop.eecs.qmul.ac.uk/user/aan32/output
    
    TRANSACTION_FILE_LOCATION ='hdfs://localhost:9000/Andria/Output/outpuTransactioFile/transaction.csv'



if __name__ == '__main__':
    
   
    #Read transaction csv
    df = pd.read_csv('transactions.csv' , sep='\t',  header=None)
    #create the two columns needed
    df.columns = ["Date","Amount"]
    
    dateColumn = df['Date']
    
    AmountColumn = df['Amount']
    
    
    fig = plt.figure()  # an empty figure with no axes
    
    fig.suptitle('Bitcoin Transactions')

    fig, ax_lst = plt.subplots()  # a figure with a 2x2 grid of Axes
 
    #plot these two colums
    ax_lst.plot(df["Date"],df['Amount'])
    
    
    start, end = ax_lst.get_xlim()
    ax_lst.xaxis.set_ticks(np.arange(start, end, 6))
  #  ax_lst.xaxis.set_major_formatter(ticker.FormatStrFormatter('%0.1f'))
      
    plt.xlabel('Date(Month)')
   
   #show them on a graph
    plt.show()
   
    print(df[:10])
     
    pass