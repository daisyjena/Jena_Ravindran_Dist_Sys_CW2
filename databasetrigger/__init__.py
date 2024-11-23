import logging
import pyodbc
import os

CONNECTION_STRING = os.getenv("CONNECTION_STRING")

def main(databasetrigger) -> None:
    logging.info("Database Trigger Initiated")

    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        logging.info("Connected to the database.")

        cursor = conn.cursor()
        
        
        select_query = """
        SELECT 
            MAX(Temperature) AS MaxTemperature, 
            MIN(Temperature) AS MinTemperature, 
            AVG(Temperature) AS AvgTemperature,
            MAX(Humidity) AS MaxHumidity,
            MIN(Humidity) AS MinHumidity, 
            AVG(Humidity) AS AvgHumidity
        FROM SensorData
        """
        cursor.execute(select_query)
        result = cursor.fetchone()

        if result:
            logging.info(f"Temperature - Max: {result.MaxTemperature}, Min: {result.MinTemperature}, Avg: {result.AvgTemperature}")
            logging.info(f"Humidity - Max: {result.MaxHumidity}, Min: {result.MinHumidity}, Avg: {result.AvgHumidity}")
        else:
            logging.warning("No data found for the requested query.")
        
        conn.commit()

    except pyodbc.Error as e:
        logging.error(f"Error receiving data: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Database connection closed.")
    
    logging.info("SQL Trigger function executed.")