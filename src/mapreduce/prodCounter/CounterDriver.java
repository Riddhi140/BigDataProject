package src.mapreduce.prodCounter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

// Determine and list all the unique products and its total count

public class CounterDriver {

    private final static Logger log = Logger.getLogger(CounterDriver.class);
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration configuration = new Configuration();
        try {
            long timeMillisStart = System.currentTimeMillis();

            Job prodCounterJob = new Job(configuration, "CounterDriver");
            prodCounterJob.setJarByClass(CounterDriver.class);

            prodCounterJob.setMapperClass(CounterMapper.class);
            prodCounterJob.setReducerClass(CounterReducer.class);

            prodCounterJob.setInputFormatClass(TextInputFormat.class);
            prodCounterJob.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(prodCounterJob, new Path(args[0]));
            FileOutputFormat.setOutputPath(prodCounterJob, new Path(args[1]));

            prodCounterJob.setMapOutputKeyClass(Text.class);
            prodCounterJob.setMapOutputValueClass(IntWritable.class);

            prodCounterJob.setOutputKeyClass(Text.class);
            prodCounterJob.setOutputValueClass(IntWritable.class);

            prodCounterJob.setNumReduceTasks(1);
            long timeMillisEnd = System.currentTimeMillis();
            log.info("Execution time in seconds : " + (timeMillisEnd - timeMillisStart)/1000);
            System.exit(prodCounterJob.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            System.out.println("Exception in CounterDriver class: ");
            log.error(e.getMessage());
        }

    }
}
