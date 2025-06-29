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
This program connects to a SQLite or MySQL®/MariaDB® database containing 
inverter production data and reports the total power generated by each inverter
It also reports the amount of power that was above the maximum continuous power
and the amount of power that was shaved off due to the inverter power limit.
It estimates shaved power by fitting a parabola to the data and calculating the 
difference between the data and the fitted parabola.  To improve the parabola fit, 
it filters out cloudy data points by fitting an initial parabola to the data and 
removing points that are below the fitted parabola minus a threshold.  A second 
initial parabola fit is done to further filter out cloudy data points.
It can also plot the data and the fitted polynomials for visual analysis.
"""

# pylint: disable=locally-disabled,global-statement,line-too-long

import sys
import datetime
from enum import IntEnum, auto
from typing import Iterator
import argparse # We support command line arguments.
import json     # Used to load the credentials file
from urllib.parse import ParseResult, urlunparse
import sqlite3
try:
    import mysql.connector # Third party library; "pip install mysql-connector-python"
    import mysql.connector.cursor
except ImportError:
    print(f'{datetime.datetime.now()} mysql.connector not found.  If using MySQL (instead of SQLite) install it with "pip install mysql-connector-python"', file=sys.stderr, flush=True)

import numpy as np # Third party library; "pip install numpy"
import matplotlib.pyplot as plt # Third party library; "pip install matplotlib"

INVERTER_DATA_DELTA_SECS: int = 331 # The time between data points.
CREDENTIALS_FILE: str = 'credentials.json'

class PlotMode(IntEnum): # IntEnum used for relative comparison of enum members
    """Enum for plot modes."""
    ALL = auto() # Always plot
    GOOD_DATA = auto() # Only plot if the data is good
    NOT_CLOUDY = auto() # Only plot if the data is not cloudy
    EXCEEDANCE = auto() # Only plot if the inverter exceeded the max continuous power
    SHAVED = auto() # Only plot if the inverter shaved actual power
    NONE = auto() # Never plot

    def __str__(self) -> str:
        """Returns the name of the enum member."""
        return self.name

    @staticmethod
    def from_string(s: str) -> 'PlotMode':
        """Converts a string to a PlotMode enum member."""
        try:
            return PlotMode[s]
        except KeyError as exc:
            raise ValueError() from exc

# The SQL statement to get the inverter production
GET_INVERTER_PRODUCTION_SQL = (
'SELECT '
'`LastReportDate`, ' # TIMESTAMP NOTE: if Watts did not change no new row was inserted
'`SerialNumber`,   ' # BIGINT UNSIGNED (12 digits)',
'`Watts`           ' # SMALLINT UNSIGNED 
'FROM `APIV1ProductionInverters` '
'WHERE `LastReportDate` BETWEEN ? AND ? ' # TIMESTAMP
'ORDER BY `LastReportDate`, `SerialNumber`'
)

def get_results_from_database(database_cursor, start_date: datetime.date, end_date: datetime.date) -> Iterator[tuple[datetime.date, dict[int, list[tuple[int, int]]]]]:
    """
    Gets inverter data from the database.

    This iterator function retrieves inverter readings from the database using database_cursor
    starting from the specified start_date and ending at end_date.
    It then processes the retrieved data and appends relevant information to the inverter_data dictionary.
    NOTE: For efficient storage, unchanged readings are not stored in the database.
          This function recreates the missing readings based on the time delta.
    When the date rolls over, it generates a new set of data and yields it as a tuple.
    Returns:
        Iterator[tuple[datetime.date, dict[int, list[tuple[int,int]]]]]: A iterator of tuples containing the date & dictionary with inverter and list of tuples (seconds_since_midnight, watts).
    """
    database_cursor.execute(GET_INVERTER_PRODUCTION_SQL, (start_date, end_date)) # Get the inverter production from the database

    inverter_data: dict[int, list[tuple[int, int]]] = {} # data by serial_number (secs_past_midnight, watts)
    previous_report_day: datetime.date = datetime.date(datetime.MINYEAR, 1, 1)

    report_day: datetime.date = datetime.date(datetime.MINYEAR, 1, 1)

    # Take each of the new records and add them to the lists.
    report_datetime: datetime.datetime
    serial_number: int
    watts: int
    for (report_datetime, serial_number, watts) in database_cursor:
        report_day = report_datetime.date()
        report_secs: int = report_datetime.hour * 3600 + report_datetime.minute * 60 + report_datetime.second # The seconds past midnight of the report
        if report_day != previous_report_day:
            # We have a new day, so yield the data for the previous day.
            if len(inverter_data) > 0:
                yield (previous_report_day, inverter_data)
            previous_report_day = report_day
            inverter_data = {} # Reset the inverter data for the new day.  .clear() unsafe since we don't know if caller is still using the old data.

        # If we have not seen this serial number before, create a new list for it
        if serial_number not in inverter_data:
            inverter_data[serial_number] = []
            previous_report_secs: int = -1 # no previous report for this serial number
            previous_report_watts: int = 0
        else:
            previous_report_secs: int = inverter_data[serial_number][-1][0]
            previous_report_watts: int = inverter_data[serial_number][-1][1]

        if previous_report_secs != -1 and (report_secs - previous_report_secs) * 2 > INVERTER_DATA_DELTA_SECS * 3: # delta 50% more than expected delta
            # We have a gap in the data, so add copies of the previous report until we are up to date.
            delta_secs: int = report_secs - previous_report_secs
            num_gaps: int = round(delta_secs / INVERTER_DATA_DELTA_SECS) # The number of gaps to fill
            gap_delta_secs: int = round(delta_secs / num_gaps) # The time between each gap
            for i in range(1, num_gaps):
                inverter_data[serial_number].append((previous_report_secs + gap_delta_secs * i, previous_report_watts))

        # Add the new data to the list of tuples for this serial number.
        inverter_data[serial_number].append((report_secs, watts))

    # Yield the data for the final day
    if len(inverter_data) > 0:
        yield (report_day, inverter_data)

def secs_to_time(secs: int) -> datetime.time:
    """
    Converts seconds past midnight to a time object.  
    """
    hours, hrem = divmod(secs, 3600)
    minutes, seconds = divmod(hrem, 60)
    return datetime.time(hours, minutes, seconds)

MAX_START_POWER: int = 20 # The maximum power of the inverter at startup (if higher than this, we probably missed too much data for proper analysis)
MAX_END_POWER: int = 0 # The maximum power of the inverter at shutdown (if higher than this, we probably missed too much data for proper analysis)
MIN_DATA_POINTS: int = 50 # The minimum number of data points to analyze the inverter (if less than this, we probably missed too much data for proper analysis)
FIT_MIN_WATTS: int = 75 # The minimum watts to fit the polynomial to (data below this level not very parabolic)
CLOUD_THRESHOLD: int = 5 # The threshold for determining if the inverter is in the shade

def analyze_day(the_day: datetime.date, serial_number: int, max_continuous: int, data: list[tuple[int, int]], plot_mode: PlotMode = PlotMode.NONE, plot_limit: float = 0) -> tuple[int, None | int, None | int]:
    """
    Analyzes inverter (serial_number) data for a single day (the_day).
    This checks the data: list of tuples(seconds_since_midnight: int, watts: int).
    The data should start with < 15W and end with 0W.
    It also checks the data contains at least 50 data points and the time between records is consistent.
    It calculates the total power generated by the inverter in watt seconds.
    It also calculates the amount of power the inverter produced above the maximum continuous power
    and the estimated amount of power that was shaved off due to the inverter power limit.
    It can also plot the data and the fitted polynomial for visual analysis.
    This function does two initial parabola fits to the data to filter cloudy data points.
    Returns total generated power in watt seconds, the amount of power above the maximum continuous power
    and the estimated amount of power that was shaved off due to the inverter power limit.
    """
    double_power_generated: int = 0 # Double the total power generated by the inverter (watt seconds).  Tracking "double" avoids division by 2 (and conversion to float)
    shave_count: int = 0 # The number of data points where the inverter output may have been limited by "max continuous power"
    previous_report_secs: int = data[0][0] # The secs past midnight of the previous record
    previous_watts: int = data[0][1] # The power of the inverter from the previous record
    min_delta: int = 86400 # The minimum time between records
    max_delta: int = 0 # The maximum time between records
    report_secs: int # The secs past mignight of the report
    watts: int # The power of the inverter at the time of the report
    shave_count: int = len([watts for _, watts in data if watts >= max_continuous]) # The number of data points where the inverter output may have been limited by "max continuous power"
    for report_secs, watts in data[1:]: # Calculate the power generated by the inverter
        delta_secs: int = report_secs - previous_report_secs
        double_power_generated += delta_secs * (previous_watts + watts) # this is double the actual power

        if delta_secs > max_delta:
            max_delta = delta_secs
        if delta_secs < min_delta:
            min_delta = delta_secs

        previous_report_secs = report_secs # The seconds past midnight of the previous record
        previous_watts = watts # The power of the inverter from the previous record

    avg_delta: int = round((data[-1][0] - data[0][0]) / (len(data) - 1)) if len(data) > 1 else INVERTER_DATA_DELTA_SECS # The average time between records
    if max_delta * 2 > avg_delta * 3: # max delta more than 50% than average delta
        print(f'{the_day} SN{serial_number} max delta too high: {max_delta} secs (avg delta: {avg_delta} secs)', file=sys.stderr)
    if min_delta * 2 < avg_delta: # min delta less than 50% of average delta
        print(f'{the_day} SN{serial_number} min delta too low: {min_delta} secs (avg delta: {avg_delta} secs)', file=sys.stderr)

    if data[0][1] > MAX_START_POWER:
        print(f'{the_day} SN{serial_number} startup power too high: {data[0][1]} W', file=sys.stderr)
        if plot_mode >= PlotMode.GOOD_DATA:
            return double_power_generated // 2, None, None
    if len(data) < MIN_DATA_POINTS:
        print(f'{the_day} SN{serial_number} insufficient data for analysis: {len(data)} records', file=sys.stderr)
        if plot_mode >= PlotMode.GOOD_DATA:
            return double_power_generated // 2, None, None
    if data[-1][1] > MAX_END_POWER:
        print(f'{the_day} SN{serial_number} shutdown power too high: {data[-1][1]} W', file=sys.stderr)
        if plot_mode >= PlotMode.GOOD_DATA:
            return double_power_generated // 2, None, None
    if shave_count == 0 and plot_mode >= PlotMode.EXCEEDANCE:
        return double_power_generated // 2, None, None

    ox_nparray = np.array([report_secs for report_secs, watts in data if watts <= FIT_MIN_WATTS]) # x values for data outside our extrapolation range
    oy_nparray = np.array([watts for _, watts in data if watts <= FIT_MIN_WATTS]) # y values for data outside our extrapolation range
    x_nparray = np.array([report_secs for report_secs, watts in data if FIT_MIN_WATTS < watts]) # x values for data we extrapolate across
    #y_nparray = np.array([watts for _, watts in data if FIT_MIN_WATTS < watts]) # y values for data we extrapolate over
    xshaved_nparray = np.array([report_secs for report_secs, watts in data if watts >= max_continuous]) # x values for shaved data
    yshaved_nparray = np.array([watts for _, watts in data if watts >= max_continuous]) # y values for shaved data
    xunshaved_nparray = np.array([report_secs for report_secs, watts in data if FIT_MIN_WATTS < watts < max_continuous]) # x values for data that was not shaved
    yunshaved_nparray = np.array([watts for _, watts in data if FIT_MIN_WATTS < watts < max_continuous]) # y values for data that was not shaved
    cloud_parabola: np.polynomial.polynomial.Polynomial = np.polynomial.polynomial.Polynomial.fit(xunshaved_nparray, yunshaved_nparray, 2)
    cloud_flags = np.array([FIT_MIN_WATTS < watts < cloud_parabola(report_secs) - CLOUD_THRESHOLD for report_secs, watts in data])
    #xunshaved_cloudy_nparray = np.array([report_secs for (report_secs, watts), cloudy in zip(data, cloud_flags) if FIT_MIN_WATTS < watts < max_continuous and cloudy])
    #yunshaved_cloudy_nparray = np.array([watts for (_, watts), cloudy in zip(data, cloud_flags) if FIT_MIN_WATTS < watts < max_continuous and cloudy])
    xunshaved_uncloudy_nparray = np.array([report_secs for (report_secs, watts), cloudy in zip(data, cloud_flags) if FIT_MIN_WATTS < watts < max_continuous and not cloudy])
    yunshaved_uncloudy_nparray = np.array([watts for (_, watts), cloudy in zip(data, cloud_flags) if FIT_MIN_WATTS < watts < max_continuous and not cloudy])
    cloud_parabola2: np.polynomial.polynomial.Polynomial = np.polynomial.polynomial.Polynomial.fit(xunshaved_uncloudy_nparray, yunshaved_uncloudy_nparray, 2)
    cloud_flags2 = np.array([FIT_MIN_WATTS < watts < cloud_parabola2(report_secs) - CLOUD_THRESHOLD for report_secs, watts in data])
    xunshaved_cloudy2_nparray = np.array([report_secs for (report_secs, watts), cloudy in zip(data, cloud_flags2) if FIT_MIN_WATTS < watts < max_continuous and cloudy])
    yunshaved_cloudy2_nparray = np.array([watts for (_, watts), cloudy in zip(data, cloud_flags2) if FIT_MIN_WATTS < watts < max_continuous and cloudy])
    xunshaved_uncloudy2_nparray = np.array([report_secs for (report_secs, watts), cloudy in zip(data, cloud_flags2) if FIT_MIN_WATTS < watts < max_continuous and not cloudy])
    yunshaved_uncloudy2_nparray = np.array([watts for (_, watts), cloudy in zip(data, cloud_flags2) if FIT_MIN_WATTS < watts < max_continuous and not cloudy])
    if len(xunshaved_uncloudy2_nparray) < MIN_DATA_POINTS:
        print(f'{the_day} SN{serial_number} too cloudy, only : {len(xunshaved_uncloudy2_nparray)} normal data points', file=sys.stderr)
        if plot_mode >= PlotMode.NOT_CLOUDY:
            return double_power_generated // 2, None, None
    fit_parabola: np.polynomial.polynomial.Polynomial = np.polynomial.polynomial.Polynomial.fit(xunshaved_uncloudy2_nparray, yunshaved_uncloudy2_nparray, 2)
    cloud_flags3 = np.array([FIT_MIN_WATTS < watts < fit_parabola(report_secs) - CLOUD_THRESHOLD for report_secs, watts in data])
    # calculate the difference between the data and the polyfit and report shaved power...
    previous_report_secs: int = data[0][0] # The secs past midnight of the previous record
    previous_watts: int = data[0][1] # The power of the inverter from the previous record
    previous_watts_est: float = float(data[0][1]) # The estimated power of the previous record
    double_over_max_power: float = 0.0
    double_over_max_power_est: float = 0.0
    for report_secs, watts in data[1:]:
        delta_secs: int = report_secs - previous_report_secs
        watts_est: float = float(watts) if watts < max_continuous else float(fit_parabola(report_secs)) # The estimated power of the current record
        if watts >= max_continuous or previous_watts >= max_continuous: # We are in the shaved period
            if previous_watts < max_continuous: # becoming over max power
                t_cross: float = previous_report_secs + (max_continuous - previous_watts) * delta_secs / (watts - previous_watts)
                double_over_max_power += (report_secs - t_cross) * (watts - max_continuous)
            elif watts < max_continuous: # becoming under max power
                t_cross: float = previous_report_secs + (max_continuous - previous_watts) * delta_secs / (watts - previous_watts)
                double_over_max_power += (t_cross - previous_report_secs) * (previous_watts - max_continuous)
            else: # both previous and current watts over max power
                double_over_max_power += delta_secs * (previous_watts - max_continuous + watts - max_continuous)
        if watts_est >= max_continuous or previous_watts_est >= max_continuous: # We are in the shaved period
            if previous_watts_est < max_continuous: # becoming over max power
                t_cross = previous_report_secs + (max_continuous - previous_watts_est) * delta_secs / (watts_est - previous_watts_est)
                double_over_max_power_est += (report_secs - t_cross) * (watts_est - max_continuous)
            elif watts_est < max_continuous: # becoming under max power
                t_cross = previous_report_secs + (max_continuous - previous_watts_est) * delta_secs / (watts_est - previous_watts_est)
                double_over_max_power_est += (t_cross - previous_report_secs) * (previous_watts_est - max_continuous)
            else: # both previous and current watts over max power
                double_over_max_power_est += delta_secs * (previous_watts_est - max_continuous + watts_est - max_continuous)
        previous_report_secs = report_secs # The seconds past midnight of the previous record
        previous_watts = watts
        previous_watts_est = watts_est
    if (plot_mode != PlotMode.EXCEEDANCE or double_over_max_power >= plot_limit * 2) and \
       (plot_mode != PlotMode.SHAVED or double_over_max_power_est - double_over_max_power >= plot_limit * 2) and \
       (plot_mode != PlotMode.NONE):
        plt.title(f'{the_day} SN{serial_number}: {double_power_generated / 7200.0:.2f}Whr')
        fit_min_hline = plt.axhline(y = FIT_MIN_WATTS, linewidth=0.3, color='orange') # pylint: disable=unused-variable
        if shave_count > 0:
            plt.text(data[0][0], max_continuous + 1, f'{double_over_max_power / 7200.0:.2f}Whr Exceedance', color='red')
            if double_over_max_power_est > double_over_max_power:
                plt.text(data[0][0], max_continuous - 11, f'{(double_over_max_power_est - double_over_max_power) / 7200.0:.2f}Whr Est Shaved', color='purple')
            max_continuous_hline = plt.axhline(y = max_continuous, linewidth=0.3, color='red') # pylint: disable=unused-variable
            first_exceedance_secs: int = next(report_secs for report_secs, watts in data if watts >= max_continuous) # The seconds past midnight of the first exceedance
            first_exceedance_vline = plt.axvline(x = first_exceedance_secs, linewidth=0.3, color='red') # pylint: disable=unused-variable
            last_exceedance_secs: int = next(report_secs for report_secs, watts in data[::-1] if watts >= max_continuous) # The seconds past midnight of the last exceedance
            last_exceedance_vline = plt.axvline(x = last_exceedance_secs, linewidth=0.3, color='red') # pylint: disable=unused-variable
            plt.text(first_exceedance_secs, 0, f'{(last_exceedance_secs - first_exceedance_secs) / 3600.0:.2f}Hrs Exceedance', color='red')
            plt.scatter(xshaved_nparray, yshaved_nparray, label=f"Exceedance Data Points ({len(xshaved_nparray)})", color="red")
            off, scl = fit_parabola.mapparms()
            max_output_secs_est: float = (fit_parabola.coef[1] / (-2 * fit_parabola.coef[2]) - off) / scl # The seconds past midnight of the estimated maximum production
            max_output_vline = plt.axvline(x = max_output_secs_est, linewidth=0.3, color='green') # pylint: disable=unused-variable
            plt.text(max_output_secs_est, 20, f'{secs_to_time(int(max_output_secs_est))} Est Peak Time', color='green')
            max_output_est: float = float(fit_parabola(max_output_secs_est)) # The estimated maximum production
            plt.text(last_exceedance_secs, max_output_est + 1, f'{max_output_est:.2f}W Est Peak Power', color='green')
            max_output_hline = plt.axhline(y = max_output_est, linewidth=0.3, color='green') # pylint: disable=unused-variable

        plt.gca().get_xaxis().set_visible(False) # x-axis is "seconds since midnight" and the number is not useful
        plt.scatter(xunshaved_uncloudy2_nparray, yunshaved_uncloudy2_nparray, label=f"Normal Data Points ({len(xunshaved_uncloudy2_nparray)})", color="blue")
        plt.plot(x_nparray, cloud_parabola(x_nparray) - CLOUD_THRESHOLD, label=f"Cloud Limit ({sum(cloud_flags)})", color="gray")
        plt.plot(x_nparray, cloud_parabola2(x_nparray) - CLOUD_THRESHOLD, label=f"Cloud Limit 2 ({sum(cloud_flags2)})", color="silver")
        plt.scatter(xunshaved_cloudy2_nparray, yunshaved_cloudy2_nparray, label=f"Cloudy Data Points ({len(xunshaved_cloudy2_nparray)})", color="gray")
        plt.plot(x_nparray, fit_parabola(x_nparray), label=f"Best Fit ({sum(cloud_flags3)})", color="lime")
        plt.scatter(ox_nparray, oy_nparray, label=f"Low Power Data Points ({len(ox_nparray)})", color="orange")
        legend = plt.legend()
        legend.set_draggable(True)
        plt.show()

    return double_power_generated // 2, round(double_over_max_power / 2) if shave_count > 0 else None, round((double_over_max_power_est - double_over_max_power) / 2) if double_over_max_power_est > double_over_max_power else None

def validate_date(date_string: str) -> datetime.date:
    """Ensure that the date is valid and in the correct format."""
    try:
        return datetime.date.fromisoformat(date_string)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_string}. Use YYYY-MM-DD.") from exc

def main():
    """
    Main function for connecting to a SQLite or MySQL®/MariaDB® database and analyzing inverter data.

    This function is the main entry point of the script. It handles command line arguments,
    connects to the database, starts the generator function that retrieves inverter data, 
    and calls analyze_day for each day's data (which will plot data if requested).
    It prints the total power generated by each inverter, the amount of power that 
    was above the maximum continuous power, and the estimated amount of power that 
    was shaved off due to the inverter power limit.

    Data is printed to standard output in the format:
    <date> SN<serial_number> <total_power_generated>Whr generated, <exceedance_power>Whr exceedance, <shaved_power>Whr shaved
    Errors and warnings are printed to standard error.

    Returns:
        None
    """

    # Create an instance of argparse to handle any command line arguments.
    parser = argparse.ArgumentParser(prefix_chars='/-', add_help=False, description='A program that connects to a SQLite or MySQL®/MariaDB® database and reports inverter data.')

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
    general_group.add_argument('/MaxContinuous', '-MaxContinuous', '--MaxContinuous', dest='max_continuous', default=349, type=int, help='The maximum continuous power output of the inverters (defaults to 349).')
    general_group.add_argument('/StartDate', '-StartDate', '--StartDate', dest='start_date', default=datetime.date(2006, 1, 1), type=validate_date, help='The date to start the report from (defaults to 2006-01-01).')
    general_group.add_argument('/EndDate', '-EndDate', '--EndDate', dest='end_date', default=datetime.date(datetime.MAXYEAR, 12, 31), type=validate_date, help='The date to end the report at (defaults to 9999-12-31).')
    general_group.add_argument('/PlotMode', '-PlotMode', '--PlotMode', dest='plot_mode', default=PlotMode.NONE, type=PlotMode.from_string, choices=list(PlotMode), help='What data sets to plot (defaults to "NONE").')
    general_group.add_argument('/PlotLimit', '-PlotLimit', '--PlotLimit', dest='plot_limit', default=0, type=float, help='Plot data set limit (WHr; defaults to "0").')
    general_group.add_argument('/Detail', '-Detail', '--Detail', dest='detail', default=False, type=bool, help='Report daily inverter details (defaults to False).')

    # We want this to appear last in the argument usage list.
    general_group.add_argument('/?', '/Help', '/help', '-h','--help','-help', action='help', help='Show this help message and exit.')

    # Handle any command line arguments.
    args: argparse.Namespace = parser.parse_args()

    # Load credentials.
    credentials: dict[str, str] = {}
    try:
        with open(CREDENTIALS_FILE, mode='r', encoding='utf-8') as json_file:
            credentials: dict[str, str] = json.load(json_file)
        print(f'{datetime.datetime.now()} Loaded {CREDENTIALS_FILE}', file=sys.stderr, flush=True)
    except FileNotFoundError:
        pass # No credentials file, we will use defaults

    database_host = args.database_host if args.database_host else credentials.get('database_host', 'localhost')
    database_port = args.database_port if args.database_port else credentials.get('database_port', 3306)
    database_username = args.database_username if args.database_username else credentials.get('database_username', 'root')
    database_password = args.database_password if args.database_password else credentials.get('database_password', '')
    database_database = args.database_database if args.database_database else credentials.get('database_database', 'Enphase')

    if args.database_file: # if we have a database file, use SQLite
        sqlite3.register_adapter(datetime.datetime, lambda dt: dt.strftime('%Y-%m-%d %H:%M:%S')) # register the datetime adapter
        sqlite3.register_converter('timestamp', lambda ts: datetime.datetime.strptime(ts.decode('utf-8'), '%Y-%m-%d %H:%M:%S')) # register the timestamp converter
        url: str = urlunparse(ParseResult(scheme='file', netloc='', path=args.database_file, params='', query='mode=ro', fragment=''))
        database_connection = sqlite3.connect(url, detect_types=sqlite3.PARSE_DECLTYPES, uri=True, autocommit=True) # connect to the database in read-only mode
        database_cursor = database_connection.cursor() # type: ignore # get a cursor to the database
        print(f'{datetime.datetime.now()} Connected to SQLite database {args.database_file} (version {sqlite3.sqlite_version}).', flush=True)
    else: # Connect to the MySQL®/MariaDB® database
        database_connection = mysql.connector.connect(user=database_username, # type: ignore
                                                      password=database_password,
                                                      host=database_host,
                                                      port=database_port,
                                                      database=database_database)
        print(f'{datetime.datetime.now()} Connected to database {database_database} on {database_host}:{database_port} as {database_username} (version {database_connection.get_server_info()}).', file=sys.stderr, flush=True)
        # database_connection.start_transaction(readonly=True) # Throws ValueError MySQL server version (5, 5, 5) does not support this feature
        # Get the database cursor we will use generator to get lots of data so we do not want buffered cursor
        database_cursor: mysql.connector.cursor.MySQLCursor = database_connection.cursor(buffered=False) # type: ignore
        global GET_INVERTER_PRODUCTION_SQL
        GET_INVERTER_PRODUCTION_SQL = GET_INVERTER_PRODUCTION_SQL.replace('?', '%s') # MySQL®/MariaDB® uses %s for parameters, SQLite uses ?

    days: int = 0
    inverter_days: int = 0
    total_generated_power: int = 0
    max_generated_power: int = 0
    max_generated_power_inverter: int = 0 # The serial number of the inverter with the maximum generated power
    max_generated_power_day: datetime.date = datetime.date(datetime.MINYEAR, 1, 1) # The day with the maximum generated power
    total_exceedance_power: int = 0
    max_exceedance_power: int = 0
    max_exceedance_power_inverter: int = 0 # The serial number of the inverter with the maximum exceedance power
    max_exceedance_power_day: datetime.date = datetime.date(datetime.MINYEAR, 1, 1) # The day with the maximum exceedance power
    total_shaved_power: int = 0
    max_shaved_power: int = 0
    max_shaved_power_inverter: int = 0 # The serial number of the inverter with the maximum shaved power
    max_shaved_power_day: datetime.date = datetime.date(datetime.MINYEAR, 1, 1) # The day with the maximum shaved power

    # Start the generator to get the inverter production from the database
    results: Iterator[tuple[datetime.date, dict[int, list[tuple[int, int]]]]] = get_results_from_database(database_cursor, args.start_date, args.end_date)

    day: datetime.date
    inverter_data: dict[int, list[tuple[int, int]]] # The data for each inverter for the day
    for day, inverter_data in results: # For each day of data
        days += 1
        for serial_number, data in inverter_data.items(): # For each serial number of data
            inverter_days += 1
            result = analyze_day(day, serial_number, args.max_continuous, data, args.plot_mode, args.plot_limit * 3600) # Analyze the day
            total_generated_power += result[0]
            if result[0] > max_generated_power:
                max_generated_power = result[0]
                max_generated_power_inverter = serial_number
                max_generated_power_day = day
            if args.detail:
                print(f'{day} SN{serial_number} {result[0] / 3600.0:.2f}Whr generated', end='')
            if result[1] is not None:
                total_exceedance_power += result[1]
                if result[1] > max_exceedance_power:
                    max_exceedance_power = result[1]
                    max_exceedance_power_inverter = serial_number
                    max_exceedance_power_day = day
                if args.detail:
                    print(f', {result[1] / 3600.0:.2f}Whr exceedance', end='')
            if result[2] is not None:
                total_shaved_power += result[2]
                if result[2] > max_shaved_power:
                    max_shaved_power = result[2]
                    max_shaved_power_inverter = serial_number
                    max_shaved_power_day = day
                if args.detail:
                    print(f', {result[2] / 3600.0:.2f}Whr shaved', end='')
            if args.detail:
                print('')

    # Close the database connection (now that results iterator is exhausted).
    database_connection.close()

    print(f'Processed {days} days of data for {inverter_days/days} inverters with a total output of {total_generated_power / 3600.0:,.2f}Whr.')
    print(f'Average generated power per day: {total_generated_power / days / 3600.0:,.2f}Whr ({total_generated_power / inverter_days / 3600.0:,.2f}Whr per inverter)')
    print(f'Maximum inverter power: {max_generated_power / 3600.0:,.2f}Whr (by SN{max_generated_power_inverter} on {max_generated_power_day})')
    print(f'Total exceedance power: {total_exceedance_power / 3600.0:,.2f}Whr')
    print(f'Maximum exceedance power: {max_exceedance_power / 3600.0:,.2f}Whr (by SN{max_exceedance_power_inverter} on {max_exceedance_power_day})')
    print(f'Total shaved power: {total_shaved_power / 3600.0:,.2f}Whr')
    print(f'Maximum shaved power: {max_shaved_power / 3600.0:,.2f}Whr (by SN{max_shaved_power_inverter} on {max_shaved_power_day})')
    print(f'Shave ratio: {total_shaved_power / total_generated_power:.2%} (total shaved power / total generated power)')
# Launch the main method if invoked directly.
if __name__ == '__main__':
    main()
