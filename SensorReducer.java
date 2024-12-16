import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.naming.Context;

import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Text;

public class SensorReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Initialisation des listes pour chaque métrique
        List<Double> temperatures = new ArrayList<>();
        List<Double> humidities = new ArrayList<>();
        List<Double> pm2_5s = new ArrayList<>();
        List<Double> waterUsages = new ArrayList<>();

        // Parcourir toutes les valeurs pour la même clé (location)
        for (Text value : values) {
            String[] metrics = value.toString().split(",");
            try {
                // Extraction des métriques : [temperature, humidity, pm2_5, water_usage]
                temperatures.add(Double.parseDouble(metrics[0])); // Température
                humidities.add(Double.parseDouble(metrics[1])); // Humidité
                pm2_5s.add(Double.parseDouble(metrics[2])); // PM2.5
                waterUsages.add(Double.parseDouble(metrics[4])); // Utilisation d'eau
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                // Ignorer les erreurs de format ou les données incomplètes
                continue;
            }
        }

        // Calculs pour chaque métrique
        double correlation = calculateCorrelation(temperatures, humidities); // Correlation Temp-Humidité

        String results = String.format(
                "Temperature [Min: %.2f, Max: %.2f, Avg: %.2f], " +
                        "Humidity [Min: %.2f, Max: %.2f, Avg: %.2f], " +
                        "PM2.5 [Min: %.2f, Max: %.2f, Avg: %.2f, Sum: %.2f], " +
                        "Water Usage [Min: %.2f, Max: %.2f, Avg: %.2f, Sum: %.2f], " +
                        "Correlation Temp-Hum: %.4f",
                calculateMin(temperatures), calculateMax(temperatures), calculateAvg(temperatures),
                calculateMin(humidities), calculateMax(humidities), calculateAvg(humidities),
                calculateMin(pm2_5s), calculateMax(pm2_5s), calculateAvg(pm2_5s), calculateSum(pm2_5s),
                calculateMin(waterUsages), calculateMax(waterUsages), calculateAvg(waterUsages),
                calculateSum(waterUsages),
                correlation);

        // Émettre les résultats sans somme pour Température et Humidité
        context.write(key, new Text(results));
    }

    // Méthodes utilitaires pour calculs (Min, Max, Moyenne et Somme)
    private double calculateMin(List<Double> values) {
        return values.stream().mapToDouble(v -> v).min().orElse(0.0);
    }

    private double calculateMax(List<Double> values) {
        return values.stream().mapToDouble(v -> v).max().orElse(0.0);
    }

    private double calculateSum(List<Double> values) {
        return values.stream().mapToDouble(v -> v).sum();
    }

    private double calculateAvg(List<Double> values) {
        return values.isEmpty() ? 0.0 : calculateSum(values) / values.size();
    }

    // Calcul de la corrélation entre température et humidité
    private double calculateCorrelation(List<Double> x, List<Double> y) {
        if (x.size() != y.size() || x.size() == 0) {
            return 0.0; // Retourne 0 en cas d'erreur
        }

        int n = x.size();
        double sumX = 0.0, sumY = 0.0, sumXY = 0.0;
        double sumX2 = 0.0, sumY2 = 0.0;

        for (int i = 0; i < n; i++) {
            double xi = x.get(i);
            double yi = y.get(i);

            sumX += xi;
            sumY += yi;
            sumXY += xi * yi;
            sumX2 += xi * xi;
            sumY2 += yi * yi;
        }

        double numerator = n * sumXY - sumX * sumY;
        double denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

        return (denominator == 0) ? 0.0 : numerator / denominator;
    }
}