package src.mapreduce.bestNProductItems;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class BestNProductItemsReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    final static Logger log = Logger.getLogger(BestNProductItemsReducer.class);

    int currentCurrent = 0;
    private int total = 10;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.total = context.getConfiguration().getInt("N", 10);
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException{
        try {
            for(Text text: value){
                if(currentCurrent < total)
                {
                    context.write(key,text);
                }
                currentCurrent++;
            }

        } catch (Exception e) {
            System.out.println("Exception in BestNProductItemsReducer class: ");
            log.error(e.getMessage());
        }
    }
}
