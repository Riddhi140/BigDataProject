package src.mapreduce.prodCounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class CounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static Logger log = Logger.getLogger(CounterMapper.class);
    private Text text = new Text();
    private IntWritable intWritable = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            String splitInput[] = value.toString().split("\\t");
            String prodId = splitInput[3].trim();

            text.set(prodId);
            context.write(text, intWritable);
        } catch (Exception e) {
            System.out.println("Exception in CounterMapper class: ");
            log.error(e.getMessage());
        }

    }
}
