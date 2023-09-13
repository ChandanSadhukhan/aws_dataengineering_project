import os
import csv
import random
from datetime import datetime

from resource.dev import config

customer_ids = list(range(1, 21))
store_ids = list(range(121, 124))
product_data = {
    "quaker oats": 212,
    "sugar": 50,
    "maida": 20,
    "besan": 52,
    "refined oil": 110,
    "clinic plus": 1.5,
    "dantkanti": 100,
    "nutrella": 40
}
sales_persons = {
    121: [1, 2, 3],
    122: [4, 5, 6],
    123: [7, 8, 9]
}

file_location = config.local_directory + "sales_partition_data"

if not os.path.exists(file_location):
    os.makedirs(file_location)

date_time = datetime.now();
sales_dt = date_time.strftime("%Y-%m-%d")

csv_file_path = os.path.join(file_location, f"sales_data_{date_time.strftime('%Y-%m-%d-%s')}.csv")
with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost", "payment_mode"])

    for _ in range(500000):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = sales_dt
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        price = product_data[product_name]
        total_cost = price * quantity
        payment_mode = random.choice(["cash", "UPI"])

        csvwriter.writerow(
            [customer_id, store_id, product_name, sales_date, sales_person_id, price, quantity, total_cost, payment_mode])

    print("CSV file generated successfully:", csv_file_path)