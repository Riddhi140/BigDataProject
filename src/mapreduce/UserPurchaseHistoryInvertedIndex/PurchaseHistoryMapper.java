package src.mapreduce.UserPurchaseHistoryInvertedIndex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PurchaseHistoryMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text productPurchase = new Text();
    private Text customerID = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if(key.get()==0){
            return;
        }
        try{
            String[] inputSplit = value.toString().split("\\t");
            customerID.set(inputSplit[1]);
            productPurchase.set(inputSplit[3]);
            context.write(customerID, productPurchase);

        } catch(Exception  e){
            System.out.println("Exception in PurchaseHistoryMapper class: ");
        }
    }
}
