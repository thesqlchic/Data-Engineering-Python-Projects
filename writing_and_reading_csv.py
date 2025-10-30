#importing faker to get dummy data
from faker import Faker
import csv
output = open('data.csv','w')
fake = Faker()

header = ['name','age','street','city','state','zip','lng','lat']

mywriter = csv.writer(output)

mywriter.writerow(header)

#Looping to insert the number of records in 1000s
for r in range(1000):
    mywriter.writerow([fake.name(),fake.random_int(min=18,max=80,step=1),fake.street_address(),fake.city(),fake.state(),fake.zipcode(),fake.longitude(),fake.latitude()])

output.close()

#reading a csv file
with open('data.csv') as f:
    myreader = csv.DictReader(f) #the DictReader allows the calling of fields by name instead of positions
    headers= next(myreader)
    for row in myreader:
        print(row['name'])
        