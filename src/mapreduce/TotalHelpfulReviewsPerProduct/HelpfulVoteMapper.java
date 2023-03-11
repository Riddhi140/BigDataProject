package src.mapreduce.TotalHelpfulReviewsPerProduct;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HelpfulVoteMapper extends Mapper<Object, Text, Text, HelpfulVoteWritable> {
    private HelpfulVoteWritable helpfulVoteWritable = new HelpfulVoteWritable();
    private Text textID = new Text();
    boolean isFirstRecord = false;
    protected void map(Object key, Text value, Mapper<Object, Text, Text, HelpfulVoteWritable>.Context context) {

        if (!isFirstRecord) {
            try {
                String[] inputSplit = value.toString().split("\\t");
                String prodID = inputSplit[3].trim();
                int helpful = Integer.parseInt(inputSplit[8].trim());
                int total = Integer.parseInt(inputSplit[9].trim());
                helpfulVoteWritable.setHelpfulVotes(helpful);
                helpfulVoteWritable.setTotalVotes(total);
                textID.set(prodID);
                context.write(textID, helpfulVoteWritable);
            } catch (Exception e) {
                System.out.println("Exception in HelpfulVoteMapper class: ");
            }
        }
        else {
            isFirstRecord = false;
        }
    }

}
