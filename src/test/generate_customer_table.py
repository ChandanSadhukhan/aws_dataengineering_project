import random
import os
import csv
from faker import Faker
from datetime import datetime, timedelta
from resource.dev import config

fake = Faker('en_IN')
start_date = datetime(2020, 1, 1)
end_date = datetime(2023, 8, 20)

file_location = config.local_directory + "customer_data_mart"
csv_file_path = os.path.join(file_location, "customer_data.csv")

def generate_randome_date(start_date, end_date):
    random_day = random.randint(0, (end_date - start_date).days)
    random_date = start_date + timedelta(days=random_day)
    return random_date.strftime("%Y-%m-%d")


with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["first_name", "last_name", "address", "pincode", "phone_number", "customer_joining_date"])

    for _ in range(25):
       first_name = fake.first_name()
       last_name = fake.last_name()
       address = 'Delhi'
       pincode = '122009'
       phone_number = '91' + ''.join([str(random.randint(0, 9)) for _ in range(8)])
       joining_date = generate_randome_date(start_date, end_date)

       csvwriter.writerow([first_name, last_name, address, pincode, phone_number, joining_date])

print("sales_data CSV file generated successfully.")