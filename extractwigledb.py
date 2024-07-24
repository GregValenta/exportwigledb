import argparse
import csv
import datetime
import os
from pathlib import Path
import sqlite3

def check_filepath(filepath: str) -> bool:
    """Checks if file exists. If file exists, gives prompt to overwrite."""

    path = Path(filepath)
    if path.is_file():
        choice = input(f"File {filepath} exists! Overwrite file? (y/N): ")
        if choice.lower() != "y":
            print("File not overwritten.")
            return False
    return True

def observed_to_csv(
    cu: sqlite3.Cursor,
    filepath: str,
    force: bool = False):
    """Writes all observed networks to a csv.

    Args:
        cu:
            Sqlite3 cursor for database.
        filepath:
            Filepath of csv file to be written
        force:
            If true, skips overwrite prompt.
    """

    if not force and not check_filepath(filepath):
        exit

    chunk_size = 1000
    headers = ["MAC", "SSID", "AuthMode", "Time", "Frequency",
        "RSSI", "CurrentLatitude", "CurrentLongitude", "AltitudeMeters",
        "AccuracyMeters", "RCOIs", "MgfrId", "Type"]
    translate_type = {
        "B": "BT",
        "E": "BLE",
        "G": "GSM",
        "L": "LTE",
        "W": "WIFI"}

    output = str(headers).strip()

    with open(filepath, "w", newline = "") as file:
        writer = csv.writer(file)
        writer.writerow(headers)

        cu.execute("""SELECT
            location.bssid,
            network.ssid,
            network.capabilities,
            location.time,
            network.frequency,
            location.level,
            location.lat,
            location.lon,
            location.altitude,
            location.accuracy,
            network.rcois,
            location.mfgrid,
            network.type
            FROM network JOIN location on network.bssid = location.bssid;""")

        while True:
            rows = cu.fetchmany(chunk_size)

            translated_rows = []
            for row in rows:
                row = list(row)
                row[-1] = translate_type[row[-1]]
                translated_rows.append(row)

            if not rows:
                break

            writer.writerows(translated_rows)

    print(f"Observations successfully written to {filepath}")

def main():
    TIMESTAMP = datetime.datetime.now(datetime.UTC).strftime('%Y%m%d%H%M%S')

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--csv",
        action = "store_true",
        default = False,
        help = ("Export observed networks to a csv file. This is the default " +
            "behavior if no other format is specified."))

    # TODO: finish me!
    # parser.add_argument(
    #     "--kml",
    #     action = "store_true",
    #     default = False,
    #     help = "Export observed networks to a kml file.")

    parser.add_argument(
        "--force",
        action = "store_true",
        default = False,
        help = ("If specified, files with the same name are overwritten " +
            "without prompt."))

    parser.add_argument(
        "filepath",
        type = str,
        help = "Filepath to Wigle Wifi sqlite database.")

    parser.add_argument(
        "output",
        type = str,
        nargs = "?",
        help = ("Filepath to directory where all files will be output. " +
            "Default is current directory."))

    args = parser.parse_args()

    cx = sqlite3.connect(args.filepath)
    cu = cx.cursor()

    output_dir = args.output if args.output else os.getcwd()

    if args.csv:
        observed_to_csv(
            cu,
            f"{output_dir}/observed{TIMESTAMP}.csv",
            args.force)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        exit
