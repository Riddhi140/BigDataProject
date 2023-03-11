package src.mapreduce.AverageRatingOfProduct;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AverageCountComposite implements Writable {

    private Long t_count;
    private Float t_average;

    public AverageCountComposite(){

    }

    public AverageCountComposite(Long c, Float a) {
        this.t_count = c;
        this.t_average = a;
    }

    public Long getT_count() {
        return t_count;
    }

    public void setT_count(Long t_count) {
        this.t_count = t_count;
    }

    public Float getT_average() {
        return t_average;
    }

    public void setT_average(Float t_average) {
        this.t_average = t_average;
    }

    public void readFields(DataInput dinp) throws IOException {
        t_count = dinp.readLong();
        t_average = dinp.readFloat();
    }
    public void write(DataOutput dOut) throws IOException {
        dOut.writeLong(t_count);
        dOut.writeFloat(t_average);
    }

    @Override
    public String toString() {
        return (new StringBuilder().append(t_count).append("\t").append(t_average).toString());
    }
}
