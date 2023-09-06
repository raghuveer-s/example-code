import datetime
import pandas as pd
import random

import sqlalchemy

# Create a connection string
connection_string = "mysql+pymysql://root:mysql123@localhost/websitecodedb"

# Create an engine
engine = sqlalchemy.create_engine(connection_string)

# Create a session
session = engine.connect()

# Generate 10,000 users
users = pd.DataFrame({
    "user_id": range(10000),
    "username": [f"user{i}" for i in range(10000)],
    "created_at": pd.to_datetime("2000-01-01"),
})

# Write users DataFrame to MySQL table
users.to_sql("Users", engine, index=False, if_exists="append")

# Generate between 1-10 purchases for each user
purchase_id = 1
end_date = datetime.date.today()
dates = [end_date - datetime.timedelta(i)  for i in range(30)]
for i in range(len(users)):
    number_of_purchases = random.randint(0, 10)
    purchases = []
    for j in range(number_of_purchases):
        value = random.randint(1, 1000)
        purchases.append({
            "txn_id": purchase_id,
            "user_id": users.loc[i, "user_id"],
            "value": value,
            "purchase_date": dates[random.randint(0, len(dates)-1)].strftime("%Y-%m-%d")
        })
        purchase_id = purchase_id + 1

    # Write purchases DataFrame to MySQL table
    purchases_df = pd.DataFrame(purchases)
    purchases_df.to_sql("Purchases", engine, index=False, if_exists="append")

# Close the session
session.close()
