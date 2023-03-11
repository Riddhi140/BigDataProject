package src.mapreduce.binningProductsRatingWise;

import mapreduce.AverageRatingOfProduct.AverageRatingOfProductDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class RatingBinDriver{
    final static Logger log = Logger.getLogger(AverageRatingOfProductDriver.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration confi = new Configuration();
        FileSystem fs = FileSystem.get(confi);

        try {
            long timeMillisStart = System.currentTimeMillis();

            Job ratingBins = Job.getInstance(confi, "Generating Rating Bins");
            ratingBins.setJarByClass(RatingBinDriver.class);

            ratingBins.setMapperClass(RatingBinMapper.class);
            ratingBins.setMapOutputKeyClass(Text.class);
            ratingBins.setMapOutputValueClass(NullWritable.class);
            ratingBins.setNumReduceTasks(1);

            FileInputFormat.setInputPaths(ratingBins, new Path(args[0]));
            FileOutputFormat.setOutputPath(ratingBins, new Path(args[1]));
            if (fs.exists(new Path(args[1]))) {
                fs.delete(new Path(args[1]), true);
            }

            MultipleOutputs.addNamedOutput(ratingBins, "bins", TextOutputFormat.class, Text.class, NullWritable.class);
            MultipleOutputs.setCountersEnabled(ratingBins, true);

            long timeMillisEnd = System.currentTimeMillis();

            log.info("Execution time in seconds : " + (timeMillisEnd - timeMillisStart)/1000);
            System.exit(ratingBins.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e ) {
            System.out.println("Error in RatingBinDriver class: ");
            log.error(e.getMessage());
        }
    }
}

