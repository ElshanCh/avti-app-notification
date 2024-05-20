from datetime import datetime, date, timedelta
from twilio.rest import Client
import time
import pytz

import os
from dotenv import load_dotenv
load_dotenv('./.env')

from sqlalchemy import create_engine, exc, update, MetaData, Table, Column, Integer
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import UUID

import pandas as pd
import numpy as np


from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
import re


# PostgreSQL connection details
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

# Twilio account details
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER")
TWILIO_MESSAGING_SERVICE_SID = os.getenv("TWILIO_MESSAGING_SERVICE_SID")

# Create the SQLAlchemy engine
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# Function to check and add country code
def add_country_code(phone_number):
    if re.match(r"^\+\d{1,}", phone_number):
        return phone_number  # Country code already present
    else:
        if len(phone_number) == 10:
            return "+39" + phone_number
        elif len(phone_number) > 10:
            return "+" + phone_number  # Add country code

# Check the state of the engine by attempting a simple query
def check_engine_state():
    try:
        # Attempt a simple query to check the connection
        with engine.connect():
            pass
        return True  # Connection is active
    except (exc.DBAPIError, exc.OperationalError):
        return False  # Connection is closed or in a disconnected state

# Schedule a message based on the provided row
def schedule_message(row):
    to_number = row["MobilePhone"]
    sending_time = row["NotifyDateTimeUTC"]
    notification_enabled = row["Notification"]
    body = f"Your scheduled event at AVTI: https://avti.app/beta/navigation?id={row['Id_Ev']}&eventId={row['EventId']}" 


    if notification_enabled:
        try:
            if validate_sending_time(sending_time):
                # print("validate_sending_time", validate_sending_time(sending_time))
                message = client.messages \
                    .create(
                        messaging_service_sid = TWILIO_MESSAGING_SERVICE_SID,
                        to = to_number,
                        body = body,
                        schedule_type = 'fixed',
                        send_at = sending_time
                    )
                print(f"The message for the EventId:{row['Id_Ev']} has been scheduled with the following message sid: {message.sid}")
                df_calendar.at[row.name, "messagesid"] = message.sid
                df_calendar.at[row.name, "notifysend"] = 1
                # print(row['EventId'])
                # print(sending_time)
            else: pass
        except TwilioRestException as e:
            print(f"An error occurred while sending the message to following UUID({row['UUID']}) with following EventId({row['EventId']}): {str(e)}")
    else:
        print(f"Skipping notification for {to_number} as 'Notification' is False.")
    print()

def update_event_status(row):
    # Update message status
    message_sid = row['messagesid']
    print()
    
    try:
        message = client.messages(message_sid).update(status='canceled')

        # Update "notifysend" in DataFrame
        index = row.name
        df_calendar_notifysend_1.at[row.name, "notifysend"] = 0
        print("Cancelled message sid: ",message_sid)
    except TwilioRestException as e:
        print(f"An error occurred while cancelling the message with following EventId({row['EventId']}): {str(e)}")
        return

    
    
# Validate the sending time and return the sending time if it meets the conditions
def validate_sending_time(sending_time):
    current_time = datetime.utcnow()
    # sending_time = datetime(2023, 6, 30, 11, 35, 10)  # Replace with your sending_time

    time_difference = sending_time - current_time
    # print(time_difference)

    if time_difference > timedelta(minutes=15) and time_difference < timedelta(days=7):
        return sending_time
    else:
        return False

def convert_to_iso8601(datetime_string):
    # Create a datetime object from the string
    # italian_datetime = datetime.strptime(datetime_string, "%Y-%m-%d %H:%M:%S")

    # Define the timezone for Italy (Europe/Rome)
    italian_timezone = pytz.timezone('Europe/Rome')

    # Localize the Italian datetime to the Italian timezone
    localized_datetime = italian_timezone.localize(datetime_string)

    # Convert the localized datetime to UTC
    utc_datetime = localized_datetime.astimezone(pytz.utc)

    # Format the UTC datetime in ISO 8601 format
    iso_8601_format = utc_datetime.strftime('%Y-%m-%d %H:%M:%S')

    return iso_8601_format

Base = declarative_base()
class Calendar(Base):
    __tablename__ = 'Calendar'
    __table_args__ = {'schema': 'public'}
    
    Id = Column(Integer, primary_key=True)
    notifysend = Column(Integer)
    EventId = Column(UUID(as_uuid=True))
    messagesid = Column(Integer)



while True:    
    max_attempts = 3  # Maximum number of attempts
    num_attempts = 0  # Counter variable
    # Get the current date and time
    current_datetime = datetime.now()
    # Format the date and time as a string
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    # Print the formatted date and time
    print(formatted_datetime)
    while num_attempts < max_attempts:
        try:
            if not check_engine_state():
                # Reopen the engine if the connection is closed or disconnected
                engine.dispose()
                engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
                
            # "df_calendar_all" is the table extracted from the database, which after will be devided into "df_calendar_notifysend_1" and "df_calendar" tables
            ##################################################################################################################################################
            # Get today's and tomorrow's dates
            today = date.today()
            tomorrow = today + timedelta(days=1)

            # Format the dates as strings
            today_str = today.strftime('%Y-%m-%d')
            # print(today_str)
            tomorrow_str = tomorrow.strftime('%Y-%m-%d')
            # print(tomorrow_str)
            # Create a DataFrame from the Calendar table with filters
            query = f'''
                        SELECT "Id" AS "Id_Ev", "UserId", "DateStartEvent", "HoursStartEvent", "DateEndEvent", "HoursEndEvent", "Notify", "StatoEvento", "EventId", "notifysend", "messagesid"
                        FROM "public"."Calendar"
                        WHERE "DateStartEvent" IN ('{today_str}', '{tomorrow_str}')
                    '''
            df_calendar_all = pd.read_sql_query(query, engine)
            ##################################################################################################################################################


            # "df_calendar_notifysend_1" this table should be used in order to figure out if the previously scheduled events got calncelled.
            ##################################################################################################################################################
            df_calendar_notifysend_1 = df_calendar_all[(df_calendar_all['notifysend'] == 1) & (df_calendar_all['StatoEvento'] == "Cancelled")]
            # print(df_calendar_notifysend_1)
            # Apply the function to each row of the DataFrame
            if not df_calendar_notifysend_1.empty:
                df_calendar_notifysend_1.apply(update_event_status, axis=1)
            # df_calendar_notifysend_1.apply(update_event_status, axis=1) 
            # Create a session
            # Replace 'your_connection_string' with your PostgreSQL connection string
            Session = sessionmaker(bind=engine)
            session = Session()

            try:
                # Update the rows in the "public"."Calendar" table
                for index, row in df_calendar_notifysend_1.iterrows():
                    event_id = row["EventId"]
                    notifysend_value = row["notifysend"]

                    # Define the update query
                    stmt = update(Calendar).where(Calendar.EventId == event_id).values(notifysend =notifysend_value)
                    session.execute(stmt)
                session.commit()
            
            except Exception as e:
                print("An error occurred:", str(e))

            session.close()
            ##################################################################################################################################################


            # "df_calendar" this table should be used to schedule the  new events
            ##################################################################################################################################################
            df_calendar = df_calendar_all[(df_calendar_all['notifysend'] == 0) & (df_calendar_all['StatoEvento'] == "Created")]
            df_calendar = df_calendar.rename(columns={'UserId': 'UUID'})

            # Create a DataFrame from the AspNetUsers table
            df_aspnetusers = pd.read_sql_query('SELECT "Id", "UserId" FROM "public"."AspNetUsers"', engine)
            df_aspnetusers = df_aspnetusers.rename(columns={"Id":"UUID"})

            # Perform left join
            df_calendar = pd.merge(df_calendar, df_aspnetusers, on='UUID', how='left')

            # Create a DataFrame from the Users table
            # df_users = pd.read_sql_query('SELECT "Id","Surname","Name", "MobilePhone", "Email", "StatusUser","Language", "Notify" FROM "public"."Users"', engine)
            df_users = pd.read_sql_query('SELECT "Id", "MobilePhone", "StatusUser","Language", "Notify" FROM "public"."Users"', engine)
            df_users = df_users.rename(columns={"Id":"UserId", "Notify":"Notification"})

            # Perform left join
            df_calendar = pd.merge(df_calendar, df_users, on='UserId', how='left')

            df_calendar = df_calendar[(df_calendar['StatusUser'] == "Active") & (df_calendar['Notification'] == True)]

            df_calendar["DateStartEvent"] = pd.to_datetime(df_calendar["DateStartEvent"])  # Convert to datetime if not already done

            # Convert 'HoursStartEvent' to string representation
            df_calendar["HoursStartEvent"] = df_calendar["HoursStartEvent"].apply(lambda x: x.strftime('%H:%M:%S'))

            # Concatenate 'DateStartEvent' and 'HoursStartEvent'
            df_calendar["DateTimeStartEvent"] = pd.to_datetime(df_calendar["DateStartEvent"].astype(str) + ' ' + df_calendar["HoursStartEvent"])

            # new column 'NotifyDateTime' containing the adjusted datetime values
            df_calendar["NotifyDateTime"] = df_calendar["DateTimeStartEvent"] - df_calendar["Notify"]

            # Get today's date
            # Filter the DataFrame based on the date part of 'NotifyDateTime'
            df_calendar = df_calendar[df_calendar['NotifyDateTime'].dt.date == today]
            df_calendar = df_calendar.reset_index(drop=True)
            df_calendar["NotifyDateTimeUTC"] = df_calendar["NotifyDateTime"].apply(convert_to_iso8601)
            df_calendar["NotifyDateTimeUTC"] = pd.to_datetime(df_calendar["NotifyDateTimeUTC"])

            # Apply the function to the "MobilePhone" column
            df_calendar["MobilePhone"] = df_calendar["MobilePhone"].apply(add_country_code)
            # Replace None values with NaN in the "MobilePhone" column
            df_calendar["MobilePhone"].fillna(np.nan, inplace=True)
            df_calendar = df_calendar.dropna(subset=["MobilePhone"])

            # Schedule the Event in the Twilio
            df_calendar.apply(schedule_message, axis=1)

            # Updating the Postgresql Calendar table
            filtered_df = df_calendar[df_calendar["notifysend"] == 1]
            

            # Replace 'your_connection_string' with your PostgreSQL connection string
            Session = sessionmaker(bind=engine)
            session = Session()
           
            try:
                # Update the rows in the "public"."Calendar" table
                for index, row in filtered_df.iterrows():
                    event_id = row["EventId"]
                    notifysend_value = row["notifysend"]
                    new_messagesid = row["messagesid"]

                    stmt = update(Calendar).where(Calendar.EventId == event_id).values(messagesid=new_messagesid, notifysend =notifysend_value)
                    session.execute(stmt)
                session.commit()

            except Exception as e:
                print("An error occurred:", str(e))
            
            session.close()

            break
        
        except exc.OperationalError as e:
            num_attempts += 1
            print(f"Attempt {num_attempts} failed. Error: {str(e)}")
            if num_attempts == max_attempts:
                # If the maximum number of attempts is reached, raise the exception
                raise
            else:
                # Wait for some time before retrying
                # You can adjust the sleep time according to your needs
                time.sleep(1)  # Import the time module for this approach
    
    # Sleep for 5 minutes
    time.sleep(120)