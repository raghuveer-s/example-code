import datetime
import os
import sqlalchemy

from sqlalchemy import text

def compute():
    task_index = int(os.environ["CLOUD_RUN_TASK_INDEX"])
    task_count = int(os.environ["CLOUD_RUN_TASK_COUNT"])

    user = os.environ["MYSQL_USER"]
    password = os.environ["MYSQL_PASSWORD"]
    db_name = os.environ["MY_DATABASE"]

    num_users = 10000
    batch_size = num_users / task_count
    start_user_id, end_user_id = [int(batch_size * task_index), int(batch_size * (task_index + 1) - 1)]

    # Create a connection string
    connection_string = f"mysql+pymysql://{user}:{password}@localhost/{db_name}"

    # Create an engine
    engine = sqlalchemy.create_engine(connection_string)

    # Create a session
    session = engine.connect()

    # Retrieve user ids
    user_ids = session.execute(
        text(f"""
        SELECT user_id
        FROM Users
        WHERE user_id >= {start_user_id} and user_id <= {end_user_id}
        """)
    )

    # Create a list of user ids
    user_id_list = []
    for row in user_ids:
        user_id_list.append(row[0])

    # Retrieve purchases for each user id
    todays_date = datetime.date.today()
    purchases = {}
    for user_id in user_id_list:
        user_purchases = session.execute(
            text(f"""
            SELECT *
            FROM Purchases
            WHERE user_id = {user_id} and purchase_date = '{todays_date}'
            """)
        )
        purchases[user_id] = sum([purchase[2] for purchase in user_purchases])

    # Calculate total revenue
    for user_id, total_revenue in purchases.items():
        session.execute(text(f"INSERT INTO Revenue(user_id, total_revenue, revenue_date) VALUES ({user_id}, {total_revenue}, '{todays_date}')"))

    session.commit()

    # Close the session
    session.close()

    # Some return value
    return "ok"