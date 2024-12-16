import random
import csv
from datetime import datetime, timedelta

# Configuration
num_readings = 1440  # 1440 lectures pour couvrir 24 heures (1 par minute)
locations = ["Quartier-1", "Quartier-2", "Quartier-3", "Quartier-4", "Quartier-5"]  # 5 quartiers
output_file = "sensor_data_24h.csv"  # Fichier de sortie

# Génération de données réalistes en fonction des heures
def generate_sensor_data(timestamp):
    hour = timestamp.hour  # Obtenir l'heure pour simuler le cycle journalier
    location = random.choice(locations)

    # Générer une température réaliste (plus chaud en journée)
    temperature = round(random.uniform(18.0, 25.0) if hour < 6 or hour > 18 else random.uniform(25.0, 35.0), 2)

    # Humidité inversement proportionnelle à la température
    humidity = round(random.uniform(70.0, 90.0) if temperature < 22.0 else random.uniform(30.0, 60.0), 2)

    # Pollution PM2.5 (variable selon quartier et heure de pointe)
    if location in ["Quartier-1", "Quartier-4"]:  # Urbains
        pm2_5 = random.randint(50, 200 if hour in range(7, 9) or hour in range(17, 19) else 100)
    elif location == "Quartier-2":  # Résidentiel
        pm2_5 = random.randint(10, 80)
    elif location == "Quartier-3":  # Semi-industriel
        pm2_5 = random.randint(50, 150)
    elif location == "Quartier-5":  # Ruraux
        pm2_5 = random.randint(5, 50)

    # Niveau sonore (réduit la nuit et selon localisation)
    if location in ["Quartier-1", "Quartier-4"]:  # Bruits urbains
        noise_level = random.randint(40, 50) if hour < 7 or hour > 22 else random.randint(60, 100)
    elif location == "Quartier-2":  # Résidentiel calme
        noise_level = random.randint(30, 50)
    elif location in ["Quartier-3", "Quartier-5"]:  # Semi-industriel et rural
        noise_level = random.randint(20, 60)

    # Utilisation d'eau (pics matinaux et soirées)
    water_usage = random.randint(0, 30) if hour < 6 or hour > 22 else random.randint(40, 100)

    return [timestamp.isoformat() + "Z", location, temperature, humidity, pm2_5, noise_level, water_usage]

# Écriture dans un fichier CSV
with open(output_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    
    # En-têtes du fichier CSV
    writer.writerow(["timestamp", "location", "temperature", "humidity", "pm2_5", "noise_level", "water_usage"])
    
    # Génération des relevés sur 24 heures
    start_time = datetime.utcnow()  # Temps de début
    for i in range(num_readings):
        timestamp = start_time + timedelta(minutes=i)  # Intervalle de 1 minute entre chaque lecture
        data = generate_sensor_data(timestamp)
        writer.writerow(data)

print(f"Données de 24 heures générées et enregistrées dans {output_file}")