import fastf1
import argparse
import sys
import pandas
import shutil
import os
parser = argparse.ArgumentParser()
parser.add_argument('--year', type=int, help='Select f1 season from given range(2018-2026)', required=True)
parser.add_argument('--track', type=str, help='Select a track', required=True)
args = parser.parse_args()
if args.year < 2018 or args.year > 2026:
    print("Selected year is out of range")
    sys.exit(1)
print(f"Fetching data for {args.year} {args.track}...")
cache_dir = '/opt/airflow/data/cache'
os.makedirs(cache_dir, exist_ok=True)
fastf1.Cache.enable_cache(cache_dir)
session = fastf1.get_session(args.year, args.track, 'R')
base_path = "/opt/airflow/data/lake/raw"
session.load()
shutil.rmtree(f"{base_path}/laps/year={args.year}/track={args.track}", ignore_errors=True)
winner = session.results.loc[session.results['Position'] == 1, 'LastName'].values[0]
fastest_driver_num = session.laps.pick_fastest()['DriverNumber']
fastest_driver = session.results.loc[session.results['DriverNumber'] == fastest_driver_num, 'LastName'].values[0]
laps_df = session.laps
laps_df['year'] = args.year
laps_df['track'] = args.track
laps_df['winner'] = winner
laps_df['fastest_lap_driver'] = fastest_driver
laps_df.to_parquet(f"{base_path}/laps", partition_cols=['year', 'track'], coerce_timestamps='us', allow_truncated_timestamps=True)
shutil.rmtree(f"data/lake/raw/telemetry/year={args.year}/track={args.track}", ignore_errors=True)
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
telemetry_df['track'] = args.track

telemetry_df.to_parquet(f"{base_path}/telemetry/", partition_cols=['year', 'track'], coerce_timestamps='us', allow_truncated_timestamps=True)