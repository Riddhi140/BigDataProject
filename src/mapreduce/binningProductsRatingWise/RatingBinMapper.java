package src.mapreduce.binningProductsRatingWise;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class RatingBinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

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
            String star_rating = inputSplit[7].trim();
            if(star_rating.equals("1")){
                binOutputs.write("bins", value, NullWritable.get(), "Star1"); }
            if(star_rating.equals("2")){
                binOutputs.write("bins", value, NullWritable.get(), "Star2"); }
            if(star_rating.equals("3")){
                binOutputs.write("bins", value, NullWritable.get(), "Star3"); }
            if(star_rating.equals("4")){
                binOutputs.write("bins", value, NullWritable.get(), "Star4"); }
            if(star_rating.equals("5")){
                binOutputs.write("bins", value, NullWritable.get(), "Star5"); }
        } catch (Exception e) {
            System.out.println("Exception in RatingBinMapper: ");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        //Close all the output stream
        binOutputs.close();
    }
}

