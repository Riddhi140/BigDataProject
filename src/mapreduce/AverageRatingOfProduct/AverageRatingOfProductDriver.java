package src.mapreduce.AverageRatingOfProduct;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

//	What is the avg rating of individual product ?

public class AverageRatingOfProductDriver {

    final static Logger log = Logger.getLogger(AverageRatingOfProductDriver.class);

    public static void main(String[] args) throws Exception
    {
        try {
            long timeMillisStart = System.currentTimeMillis();
            Job averageRatingJob = Job.getInstance();
            averageRatingJob.setJarByClass(AverageRatingOfProductDriver.class);

            FileInputFormat.addInputPath(averageRatingJob, new Path(args[0]));
            FileOutputFormat.setOutputPath(averageRatingJob, new Path(args[1]));

            averageRatingJob.setMapperClass(AverageRatingOfProductMapper.class);
            averageRatingJob.setReducerClass(AverageRatingOfProductReducer.class);

            averageRatingJob.setMapOutputKeyClass(Text.class);
            averageRatingJob.setMapOutputValueClass(AverageCountComposite.class);

            averageRatingJob.setNumReduceTasks(1);

            averageRatingJob.setOutputKeyClass(Text.class);
            averageRatingJob.setOutputValueClass(AverageCountComposite.class);

            averageRatingJob.waitForCompletion(true);
            long timeMillisEnd = System.currentTimeMillis();
            log.info("Execution time in seconds : " + (timeMillisEnd - timeMillisStart)/1000);

        } catch (Exception e) {
            System.out.println("Exception in AverageRatingOfProductDriver class : ");
            log.error(e.getMessage());
        }
    }
}
