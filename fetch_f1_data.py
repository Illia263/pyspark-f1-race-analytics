import fastf1
import argparse
import sys
import pandas
import shutil
parser = argparse.ArgumentParser()
parser.add_argument('--year', type=int, help='Select f1 season from given range(2018-2026)', required=True)
parser.add_argument('--gp', type=str, help='Select a track', required=True)
args = parser.parse_args()
if args.year < 2018 or args.year > 2026:
    print("Selected year is out of range")
    sys.exit(1)
print(f"Fetching data for {args.year} {args.gp}...")

session = fastf1.get_session(args.year, args.gp, 'R')
shutil.rmtree(f"data/lake/raw/laps/year={args.year}/gp={args.gp}", ignore_errors=True)
session.load()
laps_df = session.laps
laps_df['year'] = args.year
laps_df['gp'] = args.gp
laps_df.to_parquet("data/lake/raw/laps", partition_cols=['year', 'gp'], coerce_timestamps='us', allow_truncated_timestamps=True)
shutil.rmtree(f"data/lake/raw/telemetry/year={args.year}/gp={args.gp}", ignore_errors=True)
print("Extracting telemetry for all drivers (this might take a minute)...")
telemetry_frames = []
for driver in session.laps['DriverNumber'].unique():
    try:
        driver_laps = session.laps.pick_drivers(driver)
        driver_tel = driver_laps.get_telemetry()
        driver_tel['DriverNumber'] = driver
        telemetry_frames.append(driver_tel)
    except Exception as e:
        print(f"Skipping driver {driver}: {e}")


telemetry_df = pandas.concat(telemetry_frames, ignore_index=True)

telemetry_df['year'] = args.year
telemetry_df['gp'] = args.gp
telemetry_df.to_parquet("data/lake/raw/telemetry/", partition_cols=['year', 'gp'], coerce_timestamps='us', allow_truncated_timestamps=True)