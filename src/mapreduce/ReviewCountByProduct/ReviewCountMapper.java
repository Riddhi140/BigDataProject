package src.mapreduce.ReviewCountByProduct;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReviewCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    IntWritable intWritable = new IntWritable(1);
    Text reviewCategory = new Text();
    boolean isFirstRecord =  true;
    protected void map(Object key, Text text, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        if(!isFirstRecord){
            String[] inputSplit = text.toString().split(",");
            String prodID = inputSplit[2].trim();
            System.out.println("Mapping prodID " + prodID);
            reviewCategory.set(prodID);
            context.write(reviewCategory, intWritable);
        }
        else{
            isFirstRecord = false;
        }
    }
}
