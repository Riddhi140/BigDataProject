package src.mapreduce.TotalHelpfulReviewsPerProduct;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HelpfulVoteWritable implements Writable {
    int helpfulVotes;
    int totalVotes;
    public HelpfulVoteWritable() {

    }
    public HelpfulVoteWritable(int helpfulVotes, int totalVotes) {
        this.helpfulVotes = helpfulVotes;
        this.totalVotes = totalVotes;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(helpfulVotes);
        dataOutput.writeInt(totalVotes);    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        helpfulVotes = dataInput.readInt();
        totalVotes = dataInput.readInt();
    }

    public int getHelpfulVotes() {
        return helpfulVotes;
    }

    public void setHelpfulVotes(int helpfulVotes) {
        this.helpfulVotes = helpfulVotes;
    }

    public int getTotalVotes() {
        return totalVotes;
    }

    public void setTotalVotes(int totalVotes) {
        this.totalVotes = totalVotes;
    }

    @Override
    public String toString() {
        return helpfulVotes +
                "\t" + totalVotes;
    }
}
