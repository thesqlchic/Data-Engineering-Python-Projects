# #importing pandas
import pandas as pd

# #reading the data csv file. the nrows specify the number of rows to read
# df = pd.read_csv("C:/Users/DELL/Desktop/Work/python_lesson/data.csv",nrows = 100)

# print(df.head())

#Working with dataframes
data = {'name':['ade','collins','bola','modupe'], 'age':[12,25,39,24]}

df = pd.DataFrame(data)

df.to_csv('fromdf.csv',index=False)