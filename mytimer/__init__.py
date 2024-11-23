import random
import logging
import pyodbc
import azure.functions as func
import os

CONNECTION_STRING = os.getenv("CONNECTION_STRING")

def main(mytimer: func.TimerRequest) -> None:
    if mytimer.past_due:
        logging.warning("The timer is past due!")

    sensor_data = {
        "temperature": round(random.uniform(15, 30), 2),
        "humidity": random.randint(30, 90),
    }

    logging.info(f"Generated sensor data: {sensor_data}")

    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        logging.info("Connected to the database.")

        cursor = conn.cursor()
        insert_query = """
        INSERT INTO SensorData (Temperature, Humidity)
        VALUES (?, ?);
        """
        cursor.execute(insert_query, (sensor_data["temperature"], sensor_data["humidity"]))
        conn.commit()
        logging.info("Sensor data inserted successfully.")

    except pyodbc.Error as e:
        logging.error(f"Database error occurred: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Database connection closed.")
