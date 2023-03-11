package src.mapreduce.BinningVerifiedPurchase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class VerifiedPurchaseMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private MultipleOutputs<Text, NullWritable> binOutputs = null;
    @Override
    protected void setup(Context context){
        binOutputs = new MultipleOutputs(context);
    }

    @Override
    protected void map(LongWritable longWritable, Text value, Context context) throws IOException, InterruptedException{

        try {
            if(longWritable.get()==0) { //Skip Header
                return;
            }
            String[] inputSplit = value.toString().split("\\t");
            String verified = inputSplit[11].trim();
            if(verified.equals("Y")){
                binOutputs.write("bins", value, NullWritable.get(), "Verified"); }
            if(verified.equals("N")){
                binOutputs.write("bins", value, NullWritable.get(), "NotVerified"); }

        } catch (Exception e) {
            System.out.println("Exception in VerifiedPurchaseMapper: ");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        //Close all the output stream
        binOutputs.close();
    }
}


