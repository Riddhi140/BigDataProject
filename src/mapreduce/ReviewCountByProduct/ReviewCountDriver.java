package src.mapreduce.ReviewCountByProduct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReviewCountDriver {

public static void main(String[]args)throws Exception{
    Configuration configuration = new Configuration();
    Job reviewCounter = Job.getInstance(configuration, "review counter");
    reviewCounter.setJarByClass(ReviewCountDriver.class);

    reviewCounter.setMapperClass(ReviewCountMapper.class);
    reviewCounter.setReducerClass(ReviewCountReducer.class);

    reviewCounter.setOutputKeyClass(Text.class);
    reviewCounter.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(reviewCounter, new Path(args[0]));
    FileOutputFormat.setOutputPath(reviewCounter, new Path(args[1]));

    System.exit(reviewCounter.waitForCompletion(true) ? 0 : 1);
 }
}
