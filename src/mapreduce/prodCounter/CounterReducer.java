package src.mapreduce.prodCounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class CounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final static Logger log = Logger.getLogger(CounterReducer.class);

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {

        try {
            int t_sum = 0;
            for(IntWritable value: values) {
                t_sum += value.get();
            }

            IntWritable t_count = new IntWritable(t_sum);
            context.write(key, t_count);

        } catch (Exception e) {
            System.out.println("Exception in CounterReducer class: ");
            log.error(e.getMessage());
        }
    }
}
