package src.mapreduce.BinningVerifiedPurchase;

import mapreduce.binningProductsRatingWise.RatingBinDriver;
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

public class VerifiedPurchaseDriver {
    final static Logger log = Logger.getLogger(VerifiedPurchaseDriver.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration confi = new Configuration();
        FileSystem fs = FileSystem.get(confi);

        try {
            long timeMillisStart = System.currentTimeMillis();

            Job verifiedPurchaseBins = Job.getInstance(confi, "Generating Verified purchase Bins");
            verifiedPurchaseBins.setJarByClass(RatingBinDriver.class);

            verifiedPurchaseBins.setMapperClass(VerifiedPurchaseMapper.class);
            verifiedPurchaseBins.setMapOutputKeyClass(Text.class);
            verifiedPurchaseBins.setMapOutputValueClass(NullWritable.class);
            verifiedPurchaseBins.setNumReduceTasks(1);

            FileInputFormat.setInputPaths(verifiedPurchaseBins, new Path(args[0]));
            FileOutputFormat.setOutputPath(verifiedPurchaseBins, new Path(args[1]));
            if (fs.exists(new Path(args[1]))) {
                fs.delete(new Path(args[1]), true);
            }

            MultipleOutputs.addNamedOutput(verifiedPurchaseBins, "bins", TextOutputFormat.class, Text.class, NullWritable.class);
            MultipleOutputs.setCountersEnabled(verifiedPurchaseBins, true);

            long timeMillisEnd = System.currentTimeMillis();

            log.info("Execution time in seconds : " + (timeMillisEnd - timeMillisStart)/1000);
            System.exit(verifiedPurchaseBins.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e ) {
            System.out.println("Error in VerifiedPurchaseDriver class: ");
            log.error(e.getMessage());
        }
    }
}
