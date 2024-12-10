import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Text;

public class SensorMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.startsWith("timestamp")) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length < 7) {
            return;
        }

        String location = fields[1]; // Quartier
        String metrics = String.join(",", fields[2], fields[3], fields[4], fields[5], fields[6]);

        context.write(new Text(location), new Text(metrics));
    }
}