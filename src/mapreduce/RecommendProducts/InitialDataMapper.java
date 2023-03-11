package src.mapreduce.RecommendProducts;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InitialDataMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        try {
            if(key.get()==0){
                return;
            }

            else{
                String[] inputSplit = value.toString().split("\\t");
                Text concatedUserData = new Text();
                concatedUserData.set(inputSplit[1] + "," + inputSplit[4] + "," + inputSplit[7]); // CustomerID, ProdID, Rating

                context.write(NullWritable.get(), concatedUserData);
            }

        } catch(Exception  e){
            System.out.println("Exception in InitialDataMapper class ");
        }
    }
}
