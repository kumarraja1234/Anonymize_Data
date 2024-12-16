import csv
import random
from faker import Faker

def generate_csv(file_name, num_rows):
    fake = Faker()
    with open(file_name, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['first_name', 'last_name', 'address', 'date_of_birth'])
        for _ in range(num_rows):
            writer.writerow([
                fake.first_name(),
                fake.last_name(),
                fake.address(),
                fake.date_of_birth()
            ])

# # Generate a smaller file for testing (1 million rows ~200 MB)
# generate_csv('test.csv', 1_000_000)


generate_csv('large.csv', 30_000_000)
