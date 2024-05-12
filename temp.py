from faker import Faker
faker=Faker()
data=[]
for i in range(5):
    obj={
        "name":faker.name(),
        "address":faker.address()
    }
    data.append(obj)
print(data)