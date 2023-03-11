package src.mapreduce.AverageRatingOfProduct;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;


public class AverageRatingOfProductMapper extends Mapper<LongWritable, Text, Text, AverageCountComposite> {

    final static Logger log = Logger.getLogger(AverageRatingOfProductMapper.class);

    private AverageCountComposite averageCount = new AverageCountComposite();
    private Text textID = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {

        try {

            String splitInput[] = value.toString().split("\\t");
            String prodId = splitInput[3].trim();

            if (!prodId.isEmpty()) {
                textID.set((prodId));
                averageCount.setT_count(Long.valueOf(1));
                averageCount.setT_average(Float.valueOf(splitInput[7].trim()));
                context.write(textID, averageCount);
            }

        } catch (Exception e) {
            System.out.println("Exception in AverageRatingOfProductMapper class: ");
            log.error("Exception in AverageRatingOfProductMapper class: " + e.getMessage());
        }

    }
}
