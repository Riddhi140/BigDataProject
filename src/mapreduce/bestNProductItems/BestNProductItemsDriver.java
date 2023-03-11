package src.mapreduce.bestNProductItems;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

//Determine best 10 or N products based on number of reviews
//Executed secondary sorting technique

public class BestNProductItemsDriver {
    final static Logger log = Logger.getLogger(BestNProductItemsDriver.class);
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        try {
            long timeMillisStart = System.currentTimeMillis();
            Job bestNProductsJob = Job.getInstance(conf, "Best N Products");
            bestNProductsJob.setJarByClass(BestNProductItemsDriver.class);
            int N = 10;
            bestNProductsJob.getConfiguration().setInt("N", N);
            bestNProductsJob.setInputFormatClass(TextInputFormat.class);
            bestNProductsJob.setOutputFormatClass(TextOutputFormat.class);
            bestNProductsJob.setMapperClass(BestNProductItemsMapper.class);
            bestNProductsJob.setSortComparatorClass(BestNProdCountComparator.class);
            bestNProductsJob.setReducerClass(BestNProductItemsReducer.class);
            bestNProductsJob.setNumReduceTasks(1);
            bestNProductsJob.setMapOutputKeyClass(IntWritable.class);
            bestNProductsJob.setMapOutputValueClass(Text.class);
            bestNProductsJob.setOutputKeyClass(IntWritable.class);
            bestNProductsJob.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(bestNProductsJob, new Path(args[0]));  // input is output file path of prodCounter
            FileOutputFormat.setOutputPath(bestNProductsJob, new Path(args[1]));
            if (fileSystem.exists(new Path(args[1]))) {
                fileSystem.delete(new Path(args[1]), true);
            }
            bestNProductsJob.waitForCompletion(true);
            long timeMillisEnd = System.currentTimeMillis();
            log.info("Execution time in seconds : " + (timeMillisEnd - timeMillisStart)/1000);
        } catch (Exception e) {
            System.out.println("Exception in BestNProductItemsDriver class: ");
            log.error(e.getMessage());

        }
    }
}
