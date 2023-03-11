package src.mapreduce.ReviewCountByProduct;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReviewCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    protected void reduce(Text key, Iterable<IntWritable> intWritableIterable, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        int t_count = 0;
        IntWritable finalSum = new IntWritable(0);

        for(IntWritable v:intWritableIterable) {
            t_count += v.get();
        }
        finalSum.set(t_count);
        System.out.println("Reducing key: "+ key + " final t_count: " + t_count);
        context.write(key, finalSum);
    }
}
