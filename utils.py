
from datetime import datetime, timedelta
import pytz

from dotenv import load_dotenv
load_dotenv('./.env')

from sqlalchemy import  exc, Column, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base
from twilio.base.exceptions import TwilioRestException
import re


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
def check_engine_state(engine):
    try:
        # Attempt a simple query to check the connection
        with engine.connect():
            pass
        return True  # Connection is active
    except (exc.DBAPIError, exc.OperationalError):
        return False  # Connection is closed or in a disconnected state

# Schedule a message based on the provided row
def schedule_message(row,client):
    to_number = row["MobilePhone"]
    sending_time = row["NotifyDateTimeUTC"]
    notification_enabled = row["Notification"]
    body = f"Your scheduled event at AVTI: https://avti.app/beta/navigation?id={row['UserId']}&eventId={row['EventId']}" 


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
                print(f"The message for the EventId:{row['EventId']} has been scheduled with the following message sid: {message.sid}")
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

def update_event_status(row,client):
    # Update message status
    message_sid = row['messagesid']
    
    try:
        message = client.messages(message_sid).update(status='canceled')

        # Update "notifysend" in DataFrame
        index = row.name
        df_calendar_notifysend_1.at[row.name, "notifysend"] = 0
        print("Cancelled message sid: ",message_sid)
    except TwilioRestException as e:
        print(f"An error occurred while cancelling the message to following UUID({row['UUID']}) with following EventId({row['EventId']}): {str(e)}")

    
    
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