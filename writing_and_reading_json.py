from faker import Faker
import json
#writing to a JSON file called json_data
output = open('json_data.JSON','w')

#creating a variable called fake that holds the dummy data from Faker
fake = Faker()

#creating an empty dictionary variable called alldata
alldata = {}
alldata['records'] = []

#looping the data from faker to a record holder in 1000s
for x in range(1000):
    data={"name":fake.name(), "age":fake.random_int(min = 18, max =80, step = 1),
          "street":fake.street_address(),"city":fake.city(),"state":fake.state(),
          "zip":fake.zipcode(),"lat": float(fake.latitude()), "lng": float(fake.longitude())
          }
    alldata['records'].append(data)

#moving the records from the record holder to the json_data file
json.dump(alldata,output)

#closing the writing mode
output.close()

#reading the data in the json_data file
def reading_function():
    with open("json_data.JSON","r") as f:
        data = json.load(f)
        return  data['records'][0]['name']

print(reading_function())