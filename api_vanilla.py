import os
import zipfile
import pandas as pd
from haversine import haversine, Unit
import json
import io
from http.server import BaseHTTPRequestHandler, HTTPServer

def calculate_distance(lat1, lon1, lat2, lon2):
    if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
        return 0
    return haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS)

def process_csv_file(file_obj, start_time, end_time):
    try:
        df = pd.read_csv(file_obj)
        df['tis'] = pd.to_datetime(df['tis'], unit='s', errors='coerce')
        df = df.dropna(subset=['tis'])
        df = df[(df['tis'] >= pd.to_datetime(start_time, unit='s')) & (df['tis'] <= pd.to_datetime(end_time, unit='s'))]

        if df.empty:
            return 0, 0, 0, ""

        license_number_plate = df["lic_plate_no"].iloc[0]
        total_speed = df["spd"].sum()
        speed_violations = df["osf"].sum()
        total_distance = 0

        prev_lat, prev_lon, prev_speed = None, None, None
        df = df.sort_values("tis")
        for _, row in df.iterrows():
            lat, lon = row['lat'], row['lon']


            if lat is None or lon is None:
                continue

            if prev_lat is not None and prev_lon is not None:
                total_distance += calculate_distance(prev_lat, prev_lon, lat, lon)

            prev_lat, prev_lon = lat, lon

        return total_distance, total_speed, speed_violations, license_number_plate
    except Exception as e:

        print(f"Error processing CSV file: {e}")
        return 0, 0, 0, ""

def find_csv_file_in_zip(zip_file, vehicle_number):
    with zipfile.ZipFile(zip_file, 'r') as zf:
        for name in zf.namelist():
            if name.split('/')[-1] == f'{vehicle_number}.csv':
                return zf.read(name)

def generate_asset_report(start_time, end_time):
    report_data = []
    trip_info_df = pd.read_csv('Trip-Info.csv')
    trip_info_df['date_time'] = pd.to_datetime(trip_info_df['date_time'], format='%Y%m%d%H%M%S')

    start_datetime = pd.to_datetime(start_time, unit='s')
    end_datetime = pd.to_datetime(end_time, unit='s')

    filtered_trip_info = trip_info_df[(trip_info_df['date_time'] >= start_datetime) & (trip_info_df['date_time'] <= end_datetime)]
    grouped = filtered_trip_info.groupby("vehicle_number")

    for group_name, group_data in grouped:
        vehicle_number = group_name
        file_data = find_csv_file_in_zip('NU-raw-location-dump.zip', vehicle_number)
        if not file_data:
            continue
        file_obj = io.BytesIO(file_data)
        total_distance, total_speed, speed_violations, license_number_plate = process_csv_file(file_obj, start_datetime, end_datetime)


        if total_distance is None:
            continue

        total_speed = int(total_speed)
        speed_violations = int(speed_violations)

        avg_speed = total_speed / (group_data.shape[0] * 1000)
        transporter_name = group_data['transporter_name'].unique()[0]

        report_data.append({
            'License plate number': license_number_plate,
            'Distance': total_distance,
            'Number of Trips Completed': group_data.shape[0],
            'Average Speed': avg_speed,
            'Transporter Name': transporter_name,
            'Number of Speed Violations': speed_violations
        })

    return report_data

def save_report_as_csv(report_data, output_file):
    df = pd.DataFrame(report_data)
    df.to_csv(output_file, index=False)

class AssetReportHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data)

        if 'start_time' not in data or 'end_time' not in data:
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(
                json.dumps({'error': 'Start time and end time must be provided in epoch format.'}).encode('utf-8'))
            return

        start_time = data['start_time']
        end_time = data['end_time']

        report_data = generate_asset_report(start_time, end_time)

        if not report_data:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'error': 'No data available for the specified time period.'}).encode('utf-8'))
            return

        output_file = 'asset_report.csv'
        save_report_as_csv(report_data, output_file)

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(report_data).encode('utf-8'))


def run_server(server_class=HTTPServer, handler_class=AssetReportHTTPRequestHandler, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f"Starting server on port {port}...")
    httpd.serve_forever()


if __name__ == "__main__":
    run_server()
