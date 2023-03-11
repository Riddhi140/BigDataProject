package src.mapreduce.TotalHelpfulReviewsPerProduct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class HelpfulVoteDriver {

    final static Logger log = Logger.getLogger(HelpfulVoteDriver.class);

    public static void main(String[] args) throws Exception
    {
        Configuration configuration = new Configuration();
        try {
            Job helpfulVoteDriver = new Job(configuration, "HelpfulVoteDriver");
            long timeMillisStart = System.currentTimeMillis();
            helpfulVoteDriver.setJarByClass(HelpfulVoteDriver.class);

            FileInputFormat.addInputPath(helpfulVoteDriver, new Path(args[0]));
            FileOutputFormat.setOutputPath(helpfulVoteDriver, new Path(args[1]));

            helpfulVoteDriver.setMapperClass(HelpfulVoteMapper.class);
            helpfulVoteDriver.setCombinerClass(HelpfulvoteReducer.class);
            helpfulVoteDriver.setReducerClass(HelpfulvoteReducer.class);


            helpfulVoteDriver.setMapOutputKeyClass(Text.class);
            helpfulVoteDriver.setMapOutputValueClass(HelpfulVoteWritable.class);

            helpfulVoteDriver.setNumReduceTasks(1);

            helpfulVoteDriver.setOutputKeyClass(Text.class);
            helpfulVoteDriver.setOutputValueClass(HelpfulVoteWritable.class);

            helpfulVoteDriver.waitForCompletion(true);
            long timeMillisEnd = System.currentTimeMillis();
            log.info("Execution time in seconds : " + (timeMillisEnd - timeMillisStart)/1000);

        } catch (Exception e) {
            System.out.println("Exception in HelpfulVoteDriver class : ");
            log.error(e.getMessage());
        }
    }
}
