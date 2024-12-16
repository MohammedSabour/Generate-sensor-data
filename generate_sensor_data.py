import random
import csv
from datetime import datetime, timedelta

# Config
num_readings = 100  # Volume de données
locations = ["Quartier-1", "Quartier-2", "Quartier-3"]
output_file = "sensor_data.csv"  # Fichier de sortie

def generate_sensor_data(timestamp):
    location = random.choice(locations)
    temperature = round(random.uniform(15.0, 35.0), 2)  # Température entre 15 et 35 degrés
    humidity = round(random.uniform(30.0, 90.0), 2)  # Humidité entre 30 et 90 %
    pm2_5 = random.randint(0, 500)  # Valeur PM2.5 entre 0 et 500 µg/m³
    noise_level = random.randint(30, 100)  # Niveau de bruit entre 30 et 100 dB
    water_usage = random.randint(0, 100)  # Utilisation d'eau entre 0 et 100 litres

    return [timestamp.isoformat() + "Z", location, temperature, humidity, pm2_5, noise_level, water_usage]

with open(output_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    
    writer.writerow(["timestamp", "location", "temperature", "humidity", "pm2_5", "noise_level", "water_usage"])
    
    start_time = datetime.utcnow()
    for i in range(num_readings):
        timestamp = start_time + timedelta(seconds=i)
        data = generate_sensor_data(timestamp)
        writer.writerow(data)

print(f"Données générées et enregistrées dans {output_file}")