import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.naming.Context;

import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Text;

public class SensorReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Double> temperatures = new ArrayList<>();
        List<Double> humidities = new ArrayList<>();

        // Parcourir toutes les valeurs pour la même clé (location)
        for (Text value : values) {
            String[] metrics = value.toString().split(",");
            try {
                // Extraction
                temperatures.add(Double.parseDouble(metrics[0])); // Température
                humidities.add(Double.parseDouble(metrics[1])); // Humidité
            } catch (NumberFormatException e) {
                continue;
            }
        }

        // Calculer la corrélation entre température et humidité
        double correlation = calculateCorrelation(temperatures, humidities);

        // Émettre les résultats
        context.write(key, new Text("Correlation: " + correlation));
    }

    /**
     * Calcul de corrélation de Pearson entre deux listes de doubles.
     * 
     * @param x Liste des températures
     * @param y Liste des humidités
     * @return Coefficient de corrélation
     */
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
