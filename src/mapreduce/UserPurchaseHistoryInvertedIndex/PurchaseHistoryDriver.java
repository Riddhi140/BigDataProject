package src.mapreduce.UserPurchaseHistoryInvertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

//Leveraging inverted index pattern to determine what all products a particular user has bought
public class PurchaseHistoryDriver {

    final static Logger log = Logger.getLogger(PurchaseHistoryDriver.class);
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        try {
            long timeMillisStart = System.currentTimeMillis();
            Job purchaseHistory = Job.getInstance(conf, "Purchase history");
            purchaseHistory.setJarByClass(PurchaseHistoryDriver.class);
            purchaseHistory.setMapperClass(PurchaseHistoryMapper.class);
            purchaseHistory.setReducerClass(PurchaseHistoryReducer.class);
            purchaseHistory.setInputFormatClass(TextInputFormat.class);
            purchaseHistory.setOutputFormatClass(TextOutputFormat.class);
            purchaseHistory.setMapOutputKeyClass(Text.class);
            purchaseHistory.setMapOutputValueClass(Text.class);
            purchaseHistory.setOutputKeyClass(Text.class);
            purchaseHistory.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(purchaseHistory, new Path(args[0]));
            FileOutputFormat.setOutputPath(purchaseHistory, new Path(args[1]));
            if (fs.exists(new Path(args[1]))) {
                fs.delete(new Path(args[1]), true);
            }
            purchaseHistory.waitForCompletion(true);
            long timeMillisEnd = System.currentTimeMillis();
            log.info("Execution time in seconds : " + (timeMillisEnd - timeMillisStart)/1000);

        } catch (Exception e) {
            System.out.println("Exception in PurchaseHistoryDriver class: ");
            log.error(e.getMessage());
        }
    }
}
