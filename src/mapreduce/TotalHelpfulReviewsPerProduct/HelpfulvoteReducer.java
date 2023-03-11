package src.mapreduce.TotalHelpfulReviewsPerProduct;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HelpfulvoteReducer extends Reducer<Text, HelpfulVoteWritable, Text, HelpfulVoteWritable> {

    HelpfulVoteWritable helpfulVoteWritable = new HelpfulVoteWritable();
    public void reduce(Text key, Iterable<HelpfulVoteWritable> values, Context context) {
        try {
            int t_helpful = 0;
            int t_total = 0;

            for(HelpfulVoteWritable helpfulVoteWritable: values) {
                t_helpful += helpfulVoteWritable.getHelpfulVotes();
                t_total += helpfulVoteWritable.getTotalVotes();
            }

            helpfulVoteWritable.setHelpfulVotes(t_helpful);
            helpfulVoteWritable.setTotalVotes(t_total);
            context.write(key, helpfulVoteWritable);
        } catch (Exception e) {
            System.out.println("Exception in HelpfulvoteReducer class : ");
        }
    }
}
