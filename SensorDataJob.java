import javax.tools.Tool;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Text;

public class SensorDataJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // Check if correct number of arguments are provided
        if (args.length != 2) {
            System.err.println("Usage: SensorDataJob <input path> <output path>");
            return -1;
        }

        // Retrieve the configuration from the cluster
        Configuration conf = getConf();

        // Create a new Job
        Job job = Job.getInstance(conf, "Sensor Data Correlation Analysis");

        // Set the driver class as the main class for the job
        job.setJarByClass(SensorDataJob.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(SensorMapper.class);
        job.setReducerClass(SensorReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths from command line arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for it to complete
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Main method to run the MapReduce job
     * 
     * @param args Command line arguments (input path, output path)
     * @throws Exception If there's an error running the job
     */
    public static void main(String[] args) throws Exception {
        // Use ToolRunner to run the job with Hadoop configuration
        int exitCode = ToolRunner.run(new Configuration(), new SensorDataJob(), args);

        // Exit with the job's status code
        System.exit(exitCode);
    }
}