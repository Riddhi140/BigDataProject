package src.mapreduce.bestNProductItems;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class BestNProductItemsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    final static Logger log = Logger.getLogger(BestNProductItemsMapper.class);

    public void map(LongWritable key, Text value, Context context){

        String[] inputSplit = value.toString().split("\\t");
        String prodId = inputSplit[0].trim();
        int t_count = Integer.parseInt(inputSplit[1].trim());
        try{
            Text text = new Text(prodId);
            IntWritable reviewCount = new IntWritable(t_count);
            context.write(reviewCount, text);

        }catch(Exception e){
            System.out.println("Exception in BestNProductItemsMapper class: ");
            log.error(e.getMessage());
        }
    }
}
