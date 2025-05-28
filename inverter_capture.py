#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# This is part of Enphase-Inverter-Analyzer <https://github.com/rbroders/Enphase-Inverter-Analyzer>
# Copyright (C) 2025 RBroders!
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 3 as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
This program connects to an Enphase® IQ Gateway and copies inverter production
data from the /api/v1/production/inverters endpoint to a 
SQLite or MySQL®/MariaDB® database
"""

# pylint: disable=locally-disabled,line-too-long

import sys      # We use this to exit the program.
import signal   # We use this to handle CTRL+C, and ensure other signals are ignored.
import datetime # We output the current date/time for debugging.
import argparse # We support command line arguments.
import json     # Used to load the credentials file (most JSON parsing is done in enphase_api by requests).
import os       # getpid
import os.path  # We check whether a file exists.
import time     # We use the current epoch seconds for reading times and to delay.
import inspect  # We use this to get the line number of exceptions (func_line).
from contextlib import suppress
import sqlite3
db_exception: type = sqlite3.Error
db_data_error: type = sqlite3.DataError
db_integrity_error: type = sqlite3.IntegrityError
db_interface_error: type = sqlite3.InterfaceError

import requests # requests is used by enphase_api and we need to import this to handle Exceptions
# All the shared Enphase® functions are in these packages.
from enphase_api.cloud.authentication import Authentication
from enphase_api.local.gateway import Gateway

try:
    import mysql.connector # Third party library; "pip install mysql-connector-python".
except ImportError:
    print(f'{datetime.datetime.now()} mysql.connector not found.  If using MySQL (instead of SQLite) install it with "pip install mysql-connector-python"', file=sys.stderr, flush=True)

def func_line() -> str:
    """ Returns the function name and line number of the caller (for debugging) """
    callerframerecord: inspect.FrameInfo = inspect.stack()[1] # 0 represents this line, 1 represents line at caller
    the_frame = callerframerecord[0] # the_frame: frame = ... (both pylint and pylance do not know what frame is)
    info: inspect.Traceback = inspect.getframeinfo(the_frame)
    return info.function + '(' + str(info.lineno) + ')'

# global variable to control the main loop.  This is set to False when we receive a CTRL+C signal.  It is volatile.
loop_forever: bool = True # pylint: disable=invalid-name

def interrupt_handler(signum: int, frame) -> None: # frame: : None | inspect.FrameType
    """ SIGINT cause graceful shutdown.  All other signals are ignored. """
    global loop_forever # pylint: disable=global-statement
    if frame:
        info: inspect.Traceback = inspect.getframeinfo(frame)
        location: str = info.filename + '@' + info.function + '(' + str(info.lineno) + ')'
    else:
        location: str = 'Unknown'
    try:
        signame: str = signal.Signals(signum).name
    except ValueError:
        signame: str = '<unnamed>'
    print(f'{datetime.datetime.now()} Received signal {signame} interrupt ({signum}) in {location}', file=sys.stderr, flush=True)
    if signum == signal.SIGINT: # CTRL+C, kill -2
        loop_forever = False

# Previously these files were in Python/examples/configuration
CREDENTIALS_FILE: str = 'credentials.json'
CERTIFICATE_FILE: str = './gateway.cer' # ./ required for Gateway.trust_gateway() to create certificate file

MAX_REAPICALL_COUNT: int = 3 # Number of times to try to re-call the API if the gateway rejects us.
MAX_RELOGIN_COUNT: int  = 5 # Number of times to try to re-login if the gateway rejects us.
MAX_READDRESULTS_COUNT: int = 3 # Number of times to try to add the results if the database rejects us.
MAX_RECONNECT_COUNT: int = 5 # Number of times to try to re-connect if the database rejects us.

# Gateway message
END_POINT: str = '/api/v1/production/inverters'
TIME_BETWEEN_LOGS: int = 300 # log every 5 minutes

# SQL statements.
CREATE_INVERTER_READING_TABLE_MYSQL: str = (
"CREATE TABLE IF NOT EXISTS `APIV1ProductionInverters` ("
"`LastReportDate`   TIMESTAMP          NOT NULL COMMENT 'lastReportDate (timestamp of the report)',"
"`SerialNumber`     BIGINT   UNSIGNED  NOT NULL COMMENT 'serialNumber (12 digits)',"
"`Watts`            SMALLINT UNSIGNED  NOT NULL COMMENT 'lastReportWatts',"
"PRIMARY KEY (`LastReportDate`, `SerialNumber`)"
"COMMENT 'This table holds responses from the Enphase gateway /api/v1/production/inverters endpoint')"
)

CREATE_INVERTER_READING_TABLE_SQLITE: str = ( # Newlines are needed for commenting in SQLite
"CREATE TABLE IF NOT EXISTS `APIV1ProductionInverters` (\n"
"`LastReportDate`   TIMESTAMP          NOT NULL, --lastReportDate (timestamp of the report)\n"
"`SerialNumber`     BIGINT   UNSIGNED  NOT NULL, --serialNumber (12 digits)\n"
"`Watts`            SMALLINT UNSIGNED  NOT NULL, --lastReportWatts\n"
"PRIMARY KEY (`LastReportDate`, `SerialNumber`)  --This table holds responses from the Enphase gateway /api/v1/production/inverters endpoint (except MaxWatts)\n"
") WITHOUT ROWID;"
)

ADD_INVERTER_READING: str = (
'INSERT INTO `APIV1ProductionInverters` '
'(`LastReportDate`, `SerialNumber`, `Watts`) '
'VALUES (?, ?, ?) '
)

GET_LATEST_INVERTER_READINGS: str = (
'SELECT `LastReportDate`, `SerialNumber`, `Watts` '
'  FROM `APIV1ProductionInverters` I1'
' WHERE I1.LastReportDate = (SELECT MAX(`LastReportDate`) from `APIV1ProductionInverters` I2 WHERE I1.`SerialNumber` = I2.`SerialNumber`)'
)

def update_bind_placeholders(bind_placeholder: str) -> None:
    """
    This function replaces the bind variable in the SQL statement with the correct one for the database.
    """
    global ADD_INVERTER_READING # pylint: disable=global-statement
    ADD_INVERTER_READING = ADD_INVERTER_READING.replace('?', bind_placeholder)

inverters_last_report_date: dict[int, datetime.datetime] = {} # this dictionary contains the last report date for each inverter
inverters_production: dict[int, int] = {} # this dictionary contains the last production value (watts) for each inverter

def insert_inverter_reading(database_cursor, last_report_date: datetime.datetime, serial_number: int, last_watts: int) -> int:
    """
    This function is used to insert inverter readings into the SQLite database.
    It takes a database_cursor, the last report date, serial number and last watts as arguments and returns the number of rows inserted (usually 1).
    It ignores DataError, IntegrityError or InterfaceError if there is a problem with the data.
    It raises Error (or other errors) so the caller can try to reconnect to the database.
    """

    try:
        database_cursor.execute(ADD_INVERTER_READING, (last_report_date, serial_number, last_watts))
        # database_connection.commit() # database_connection.autocommit = True # autocommit is set to True in the connection
    except (db_data_error, db_integrity_error, db_interface_error) as e: # type: ignore
        print(f'{datetime.datetime.now()} {type(e)}@{func_line()} {getattr(e, 'message', repr(e))}', file=sys.stderr, flush=True)
        print(f'Data/Integrity/Interface error while processing reading: {last_report_date}, {serial_number}, {last_watts}', file=sys.stderr, flush=True)
    except db_exception as e: # type: ignore
        print(f'{datetime.datetime.now()} {type(e)}@{func_line()} {getattr(e, 'message', repr(e))}', file=sys.stderr, flush=True)
        raise # Probably connection error, let main loop try to reconnect

    return database_cursor.rowcount # this is the number of rows affected by the last execute() call

def add_results_to_database(database_cursor, json_object) -> tuple[int, int, int]:
    """
    Adds inverter production values to a database.

    This function takes the following arguments and adds inverter production values to the database.
    To improve storage efficiency, the database is only updated if the values have changed since the last update.

    Args:
        database_cursor (MySQLCursorPrepared):
            The cursor for inserting inverter production numbers.
        json_object (list):
            A JSON object containing inverter production.

    Raises:
        None: errors are handled in the insert_inverter_reading_function.
    
    Returns:
        tuple[int, int, int]: database rows added, reading resends, unchanged readings
    """
    global inverters_last_report_date # pylint: disable=global-variable-not-assigned
    global inverters_production # pylint: disable=global-variable-not-assigned
    inverter_reading_rowcount: int = 0 # number of inverter readings added to the database
    inverter_reading_resend_count: int = 0 # number of inverter readings that we have seen before
    inverter_reading_unchanged_count: int = 0 # number of inverter readings where the production was unchanged

    # Get data for each inverter
    for inverter_reading in json_object:
        serial_number: int = int(inverter_reading['serialNumber'])
        last_report_date: datetime.datetime = datetime.datetime.fromtimestamp(inverter_reading['lastReportDate'])
        dev_type: int = int(inverter_reading['devType'])
        if dev_type != 1:
            print(f'{datetime.datetime.now()} Invalid devType: {dev_type} in reading {inverter_reading}', file=sys.stderr, flush=True)
            continue
        last_watts: int = int(inverter_reading['lastReportWatts'])

        if inverters_last_report_date.get(serial_number) == last_report_date: # Re-send of old report, verify data and ignore it
            if inverters_production.get(serial_number) == last_watts: # data matches, just ignore it
                inverter_reading_resend_count += 1
                continue
            # Yikes, re-use of last_report_date, but production data doesn't match!
            print(f'{datetime.datetime.now()} Duplicate {serial_number}({last_report_date}): old {inverters_production.get(serial_number)} new {last_watts}', file=sys.stderr, flush=True)
            last_report_date = datetime.datetime.now().replace(microsecond=0) # we don't want to lose this important change so generate a new date

        if inverters_production.get(serial_number) == last_watts: # Production hasn't changed don't waste space in the database
            inverter_reading_unchanged_count += 1
            inverters_last_report_date[serial_number] = last_report_date # update the last report date for this inverter
        else:
            if insert_inverter_reading(database_cursor, last_report_date, serial_number, last_watts) == 1:
                inverter_reading_rowcount += 1 # increment the number of rows added to the database
                inverters_production[serial_number] = last_watts # update the last production value (watts) for this inverter
                inverters_last_report_date[serial_number] = last_report_date # update the last report date for this inverter
            else:
                print(f'{datetime.datetime.now()} No row inserted for inverter reading ({last_report_date}, SN{serial_number} {last_watts}W)', file=sys.stderr, flush=True)

    return (inverter_reading_rowcount, inverter_reading_resend_count, inverter_reading_unchanged_count)

def get_secure_gateway_session(credentials: dict) -> Gateway:
    """
    Establishes a secure session with the Enphase® IQ Gateway API.

    This function manages the authentication process to establish a secure session with
    an Enphase® IQ Gateway.

    It handles JWT validation and initialises the Gateway API wrapper for subsequent interactions.

    It also downloads and stores the certificate from the gateway for secure communication.

    Args:
        credentials (dict): A dictionary containing the required credentials.

    Returns:
        Gateway: An initialised Gateway API wrapper object for interacting with the gateway.

    Raises:
        ValueError: If the token is missing/expired/invalid, or if there's an issue with login.
    """

    # Do we have a valid JSON Web Token (JWT) to be able to use the service?
    if not (credentials.get('gateway_token') and
            Authentication.check_token_valid(token=credentials['gateway_token'], gateway_serial_number=credentials.get('gateway_serial_number'))):
        # It is either not present or not valid.
        raise ValueError('No or expired token.')

    host = credentials.get('gateway_host')

    # Download and store the certificate from the gateway so all future requests are secure.
    if not os.path.exists(CERTIFICATE_FILE):
        Gateway.trust_gateway(host=host, cert_file=CERTIFICATE_FILE)

    # Instantiate the Gateway API wrapper (with the default library hostname if None provided).
    gateway = Gateway(host=host, cert_file=CERTIFICATE_FILE)

    # Are we not able to login to the gateway?
    if not gateway.login(credentials['gateway_token']):
        # Let the user know why the program is exiting.
        raise ValueError(f'Unable to login to the gateway (bad, expired or missing gateway_token in {CREDENTIALS_FILE}).')

    # Return the initialised gateway object.
    return gateway

def main():
    """
    Main function for collecting and storing Enphase® meter readings to a MySQL®/MariaDB® database.

    This function loads credentials from a JSON file, initializes a secure session with the
    Enphase® Gateway API, retrieves meter reports, connects to a MySQL®/MariaDB® database, and
    stores the collected data in the database.

    Args:
        None

    Returns:
    """
    # Create an instance of argparse to handle any command line arguments.
    parser = argparse.ArgumentParser(prefix_chars='/-', add_help=False, description='This program connects to an Enphase gateway and copies api_v1_production_inverters data to a SQLite or MySQL®/MariaDB® database.')

    # Arguments to control the database connection.
    sqllite_group = parser.add_argument_group('SQLite DB')
    sqllite_group.add_argument('/DBFile', '-DBFile', '--DBFile', dest='database_file', help='SQLite DB file (alternative to MySQL DB parameters).')

    database_group = parser.add_argument_group('MySQL DB')
    database_group.add_argument('/DBHost', '-DBHost', '--DBHost', dest='database_host', help='Database server host (defaults from credentials file then "localhost").')
    database_group.add_argument('/DBPort', '-DBPort', '--DBPort', dest='database_port', help='Database server port (defaults from credentials file then "3306").')
    database_group.add_argument('/DBUsername', '-DBUsername', '--DBUsername', dest='database_username', help='Database username (defaults from credentials file then "root").')
    database_group.add_argument('/DBPassword', '-DBPassword', '--DBPassword', dest='database_password', help='Database password (defaults from credentials file then blank).')
    database_group.add_argument('/DBDatabase', '-DBDatabase', '--DBDatabase', dest='database_database', help='Database schema (defaults from credentials file then "Enphase").')

    # Arguments to control how the program generally behaves.
    general_group = parser.add_argument_group('General')
    general_group.add_argument('/PollSecs', '-PollSecs', '--PollSecs', type=float, default=60.0, dest='poll_secs', help='Poll gateway for new data every n seconds.  Defaults to 60.0.')

    # We want this to appear last in the argument usage list.
    general_group.add_argument('/?', '/Help', '/help', '-h','--help','-help', action='help', help='Show this help message and exit.')

    # Handle any command line arguments.
    args = parser.parse_args()

    # Notify the user.
    print(f'{datetime.datetime.now()} Starting up as pid {os.getpid()} version 0.9.3', file=sys.stderr, flush=True)

    # Load credentials.
    try:
        with open(CREDENTIALS_FILE, mode='r', encoding='utf-8') as json_file:
            credentials: dict[str, str] = json.load(json_file)
    except FileNotFoundError:
        print(f'{datetime.datetime.now()} Unable to find file: {CREDENTIALS_FILE}.  Please create it with the required credentials.', file=sys.stderr, flush=True)
        sys.exit(1)
    print(f'{datetime.datetime.now()} Loaded {CREDENTIALS_FILE}', file=sys.stderr, flush=True)

    # Use a secure gateway initialization flow.
    try:
        gateway: Gateway = get_secure_gateway_session(credentials) # throws ValueError if unable to connect and login
    except ValueError as e:
        print(f'{datetime.datetime.now()} Unable to connect to Enphase gateway at {credentials.get('gateway_host')}: {getattr(e, 'message', repr(e))}', file=sys.stderr, flush=True)
        sys.exit(1)
    print(f'{datetime.datetime.now()} Connected to Enphase gateway at {credentials.get('gateway_host')}', file=sys.stderr, flush=True)
    relogin_count: int = MAX_RELOGIN_COUNT # number of times to try to re-login
    reapicall_count:int = MAX_REAPICALL_COUNT # number of times to try to re-call the API if the gateway rejects us

    global db_exception # pylint: disable=global-statement
    database_host = args.database_host if args.database_host else credentials.get('database_host', 'localhost')
    database_port = args.database_port if args.database_port else credentials.get('database_port', 3306)
    database_username = args.database_username if args.database_username else credentials.get('database_username', 'root')
    database_password = args.database_password if args.database_password else credentials.get('database_password', '')
    database_database = args.database_database if args.database_database else credentials.get('database_database', 'Enphase')

    if args.database_file: # if we have a database file, use SQLite
        # Connect to the SQLite database (database connection is solid and reliable)
        sqlite3.register_adapter(datetime.datetime, lambda dt: dt.strftime('%Y-%m-%d %H:%M:%S')) # register the datetime adapter
        sqlite3.register_converter('timestamp', lambda ts: datetime.datetime.strptime(ts.decode('utf-8'), '%Y-%m-%d %H:%M:%S')) # register the timestamp converter
        try:
            database_connection = sqlite3.connect(args.database_file, detect_types=sqlite3.PARSE_DECLTYPES, autocommit=True) # connect to the database
        except db_exception as e: # type: ignore
            print(f'{datetime.datetime.now()} Unable to connect to database {args.database_file}: {getattr(e, 'message', repr(e))}', file=sys.stderr, flush=True)
            sys.exit(2)
        database_cursor = database_connection.cursor() # get a cursor to the database
        database_cursor.execute('PRAGMA journal_mode=wal;')
        journal_mode = database_cursor.fetchone()
        database_cursor.execute('PRAGMA synchronous=NORMAL;') # Use NORMAL synchronous mode for faster writes
        print(f'{datetime.datetime.now()} Connected to SQLite database {args.database_file} (version={sqlite3.sqlite_version}, journal_mode={journal_mode[0]}).', flush=True)
        database_cursor.execute(CREATE_INVERTER_READING_TABLE_SQLITE) # make sure the table exists

    else: # Connect to the MySQL®/MariaDB® database
        db_exception = mysql.connector.errors.Error # type: ignore
        global db_data_error # pylint: disable=global-statement
        db_data_error = mysql.connector.errors.DataError # type: ignore
        global db_integrity_error # pylint: disable=global-statement
        db_integrity_error = mysql.connector.errors.IntegrityError # type: ignore
        global db_interface_error # pylint: disable=global-statement
        db_interface_error = mysql.connector.errors.InterfaceError # type: ignore
        update_bind_placeholders('%s') # MySQL uses %s as the bind variable placeholder
        try:
            database_connection = mysql.connector.connect(host=database_host, # type: ignore
                                                          port=database_port,
                                                          user=database_username,
                                                          password=database_password,
                                                          database=database_database,
                                                          autocommit=True)
            print(f'{datetime.datetime.now()} Connected to MySQL database {database_database} on {database_host}:{database_port} as {database_username} (version {database_connection.get_server_info()}).', flush=True)
        except db_exception as e: # type: ignore
            print(f'{datetime.datetime.now()} Unable to connect to database {database_database} on {database_host}:{database_port} as {database_username}: {getattr(e, 'message', repr(e))}', file=sys.stderr, flush=True)
            sys.exit(2)
        database_cursor = database_connection.cursor(prepared=True)
        database_cursor.execute(CREATE_INVERTER_READING_TABLE_MYSQL) # make sure the table exists
    readdresults_count: int = MAX_READDRESULTS_COUNT # number of times to try to add the results
    reconnect_count: int = MAX_RECONNECT_COUNT # number of times to try to re-connect to the database

    global inverters_production # pylint: disable=global-variable-not-assigned
    global inverters_last_report_date # pylint: disable=global-variable-not-assigned
    # fill the inverters_production/timestamp dictionarys with the last value for each inverter from the database
    database_cursor.execute(GET_LATEST_INVERTER_READINGS)
    for row in database_cursor: # type: ignore # row[0] is lastReportDate (datetime.datetime), row[1] is serialnumber (int), row[2] is watts (int)
        inverters_last_report_date[row[1]] = row[0]
        inverters_production[row[1]] = row[2]
        # print(f'Inverter {row[1]} was producing {row[2]} watts at timestamp {row[0]}.', flush=True)
    print(f'{datetime.datetime.now()} Found {len(inverters_production)} inverters in the database producing {sum(inverters_production.values())} watts', flush=True)

    inverter_reading_rowcount: int = 0 # number of inverter readings added to the database
    inverter_reading_resend_count: int = 0 # number of inverter readings resent by the gateway
    inverter_reading_unchanged_count: int = 0 # number of inverter readings with no production changes
    json_msgs_received: int = 0 # number of messages received from the gateway
    prev_latest_last_report_datetime: datetime.datetime = datetime.datetime(datetime.MINYEAR, 1, 1)
    msgs_with_no_new_data: int = 0 # number of messages with no new data

    print(f'{datetime.datetime.now()} Collecting inverter readings. To exit press CTRL+C', flush=True)
    next_fetch_time: float = time.monotonic() + args.poll_secs
    next_log_time: float = time.monotonic() + TIME_BETWEEN_LOGS
    old_cum_prod_time: float = time.monotonic()
    cumulative_power: float = 0.0
    last_log_time: float = time.monotonic()
    last_log_cum_power: float = 0.0
    response_json: list[dict[str, str | int]] = []
    global loop_forever # pylint: disable=global-variable-not-assigned
    try:
        while loop_forever: # Interrupt handler for CTRL+C will set this flag to False asynchronously (loop_forever is volatile)
            # Request the data from the meter reports (reconnect if host rejects us)
            while loop_forever and relogin_count > 0 and reapicall_count > 0: # gateway api_call loop
                try:
                    reapicall_count -= 1 # used up an api_call attempt
                    response_json = gateway.api_call(END_POINT) # type: ignore
                    reapicall_count = MAX_REAPICALL_COUNT # reset the reapicall count every time we make api_call successfully
                    break # successfully made the api call, break out of the gateway api_call loop
                except (requests.exceptions.RequestException, ValueError) as e:
                    print(f'{datetime.datetime.now()} {type(e)}@{func_line()} {getattr(e, 'message', repr(e))}', file=sys.stderr, flush=True)
                    while loop_forever and relogin_count > 0: # relogin to the gateway loop
                        time.sleep(10 * (MAX_RELOGIN_COUNT - relogin_count)) # wait before trying again
                        print(f'{datetime.datetime.now()} Attempting to re-login to the gateway', file=sys.stderr, flush=True)
                        try:
                            relogin_count -= 1 # used up a relogin attempt
                            if gateway.login(credentials['gateway_token']):
                                print(f'{datetime.datetime.now()} Gateway re-login successful', file=sys.stderr, flush=True)
                                relogin_count = MAX_RELOGIN_COUNT # reset the relogin count every time we make re-login successfully
                                break # successfully login to the gateway, break out of the relogin loop
                            else:
                                print(f'{datetime.datetime.now()}: Unable to re-login to the gateway', file=sys.stderr, flush=True)
                        except requests.exceptions.RequestException as e2:
                            print(f'{datetime.datetime.now()} {type(e2)}@{func_line()} {getattr(e2, 'message', repr(e2))}', file=sys.stderr, flush=True)
            if relogin_count == 0:
                print(f'{datetime.datetime.now()} Unable to re-login to the gateway, exiting', file=sys.stderr, flush=True)
                break # break out of the loop_forever loop if we can't re-login to the gateway
            if reapicall_count == 0:
                print(f'{datetime.datetime.now()} Unable to re-call the API, exiting', file=sys.stderr, flush=True)
                break # break out of the loop_forever loop if we can't re-call the API

            json_msgs_received += 1 # increment the number of messages received from the gateway
            latest_last_report_datetime: datetime.datetime = max((datetime.datetime.fromtimestamp(float(inverter_reading['lastReportDate'])) for inverter_reading in response_json), default=prev_latest_last_report_datetime) # get the latest lastReportDate from the response
            if prev_latest_last_report_datetime == latest_last_report_datetime:
                msgs_with_no_new_data += 1
            prev_latest_last_report_datetime = latest_last_report_datetime

            # Add this result to the database.
            status: tuple[int, int, int] = (0, 0, 0) # database rows added, reading resends, unchanged readings
            while loop_forever and reconnect_count > 0 and readdresults_count > 0: # add_results_to_database loop
                try:
                    readdresults_count -= 1 # used up a readdresults attempt
                    status = add_results_to_database(database_cursor, response_json)
                    readdresults_count = MAX_RECONNECT_COUNT # reset the readdresults count every time we add results to database successfully
                    break # successfully added to the database, break out of the add_results_to_database loop
                except db_exception as e: # type: ignore
                    print(f'{datetime.datetime.now()} {type(e)}@{func_line()} {getattr(e, 'message', repr(e))}', file=sys.stderr, flush=True)
                    while loop_forever and reconnect_count > 0: # reconnect to database loop
                        time.sleep(10 * (MAX_RECONNECT_COUNT - reconnect_count)) # wait before trying again
                        print(f'{datetime.datetime.now()} Attempting to reconnect to the database', file=sys.stderr, flush=True)
                        with suppress (db_exception): # cleanup before reconnect; ignore all database errors
                            database_cursor.close()
                            database_connection.close()
                        try:
                            reconnect_count -= 1 # used up a reconnect attempt
                            if args.database_file: # if we have a database file, use SQLite
                                database_connection = sqlite3.connect(args.database_file, detect_types=sqlite3.PARSE_DECLTYPES, autocommit=True) # connect to the database
                                database_cursor = database_connection.cursor() # get a cursor to the database
                            else: # Connect to the MySQL®/MariaDB® database
                                database_connection = mysql.connector.connect(host=database_host, # type: ignore
                                                                              port=database_port,
                                                                              user=database_username,
                                                                              password=database_password,
                                                                              database=database_database,
                                                                              autocommit=True)
                                database_cursor = database_connection.cursor(prepared=True)
                            reconnect_count = MAX_RECONNECT_COUNT # reset the reconnect count every time we make connect to db successfully
                            print(f'{datetime.datetime.now()} Database reconnect successful', file=sys.stderr, flush=True)
                            # fill the inverters_production/timestamp dictionarys with the last value for each inverter from the database
                            inverters_last_report_date.clear() # clear the last report date dictionary
                            inverters_production.clear() # clear the production dictionary
                            database_cursor.execute(GET_LATEST_INVERTER_READINGS)
                            for row in database_cursor: # type: ignore # row[0] is lastReportDate (datetime.datetime), row[1] is serialnumber (int), row[2] is watts (int)
                                inverters_last_report_date[row[1]] = row[0]
                                inverters_production[row[1]] = row[2]
                            print(f'{datetime.datetime.now()} Found {len(inverters_production)} inverters in the database producing {sum(inverters_production.values())} watts', flush=True)
                            break # successfully reconnected, break out of the reconnect to database loop
                        except db_exception as e2: # type: ignore
                            print(f'{datetime.datetime.now()} {type(e2)}@{func_line()} {getattr(e2, 'message', repr(e2))}', file=sys.stderr, flush=True)
                            print(f'{datetime.datetime.now()} Unable to reconnect to the database', file=sys.stderr, flush=True)
            if reconnect_count == 0:
                print(f'{datetime.datetime.now()} Unable to reconnect to the database, exiting', file=sys.stderr, flush=True)
                break # exit the loop_forever if we can't reconnect to the database
            if readdresults_count == 0:
                print(f'{datetime.datetime.now()} Unable to add results to the database, exiting', file=sys.stderr, flush=True)
                break # exit the loop_forever if we can't add the results to the database

            inverter_reading_rowcount += status[0]
            inverter_reading_resend_count += status[1]
            inverter_reading_unchanged_count += status[2]

            new_cum_prod_time = time.monotonic()
            cumulative_power += sum(inverters_production.values()) * (new_cum_prod_time - old_cum_prod_time) # net power in watt-seconds
            old_cum_prod_time = new_cum_prod_time

            if time.monotonic() > next_log_time:
                log_time = time.monotonic()
                next_log_time += TIME_BETWEEN_LOGS # try to make the log interval as close to TIME_BETWEEN_LOGS as possible
                if time.monotonic() > next_log_time: # if we are behind, however, catch up
                    next_log_time = time.monotonic() + TIME_BETWEEN_LOGS
                print(f'{datetime.datetime.now().replace(microsecond=0)} {json_msgs_received} msgs ({msgs_with_no_new_data} stale) stored {inverter_reading_rowcount} readings (ignored {inverter_reading_resend_count} resends, {inverter_reading_unchanged_count} unchanged) {(cumulative_power-last_log_cum_power)/(log_time-last_log_time):.0f} watts', flush=True)
                if (args.database_file and inverter_reading_rowcount == 0): # if we are using SQLite and there are no rows added, checkpoint the database
                    with suppress(db_exception): # ignore any database errors during checkpoint
                        database_cursor.execute('PRAGMA wal_checkpoint(TRUNCATE);')
                inverter_reading_rowcount = 0
                inverter_reading_resend_count = 0
                inverter_reading_unchanged_count = 0
                json_msgs_received = 0
                msgs_with_no_new_data = 0
                last_log_cum_power = cumulative_power
                last_log_time = log_time


            # Sleep until its time to fetch the next reading.
            try: # sleep will throw a ValueError if the time to sleep is negative
                time.sleep(next_fetch_time - time.monotonic())
                next_fetch_time += args.poll_secs
            except ValueError: # If we have a negative sleep time, reset the next fetch time to poll_secs from now.
                next_fetch_time = time.monotonic() + args.poll_secs
    except KeyboardInterrupt: # this should never happen as the interrupt handler should set loop_forever to False
        print(f'{datetime.datetime.now()} KeyboardInterrupt exception!  Shutting down.', file=sys.stderr, flush=True)
    except Exception as e:
        print(f'{datetime.datetime.now()} Unhandled exception occurred: {type(e)}@{func_line()} {getattr(e, 'message', repr(e))}', file=sys.stderr, flush=True)
        database_connection.close()
        raise # Re-raise.

    database_connection.close()

# Launch the main method if invoked directly.
if __name__ == '__main__':
    print(f'{datetime.datetime.now()} Capturing {len(signal.valid_signals())} signals', flush=True)
    for sig_num in signal.valid_signals():
        try:
            sig_name: str = signal.Signals(sig_num).name
        except ValueError:
            sig_name: str = '<unnamed>' # pylint: disable=invalid-name
        # print(f'{datetime.datetime.now()} Capturing {sig_name} interrupt ({sig_num})', flush=True)
        try:
            signal.signal(sig_num, interrupt_handler)
        except (ValueError, OSError):
            print(f'{datetime.datetime.now()} Unable to capture {sig_name} interrupt', file=sys.stderr, flush=True)
    main()
    print(f'{datetime.datetime.now()} Goodbye.', flush=True)
