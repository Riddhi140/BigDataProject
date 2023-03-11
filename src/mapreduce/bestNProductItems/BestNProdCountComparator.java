package src.mapreduce.bestNProductItems;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class BestNProdCountComparator extends WritableComparator {

    protected BestNProdCountComparator() {

        super(IntWritable.class,true);
    }

    public int compare(WritableComparable w1, WritableComparable w2) {
        IntWritable val1 = (IntWritable) w1;
        IntWritable val2 = (IntWritable) w2;

        int diff = val1.get() < val2.get() ? 1 : val1.get() == val2.get() ? 0 : -1;
        return diff;
    }
}
