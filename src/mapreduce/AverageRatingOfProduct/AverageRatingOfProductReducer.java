package src.mapreduce.AverageRatingOfProduct;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class AverageRatingOfProductReducer extends Reducer<Text, AverageCountComposite, Text, AverageCountComposite> {

    final static Logger log = Logger.getLogger(AverageRatingOfProductMapper.class);
    private AverageCountComposite countComposite = new AverageCountComposite();

    public void reduce(Text key, Iterable<AverageCountComposite> value, Context context)
        throws IOException, InterruptedException {

        try {
            long t_count = 0;
            float totalSum = 0;

            for (AverageCountComposite averageCountComposite: value) {
                t_count += averageCountComposite.getT_count();
                totalSum += averageCountComposite.getT_count() * averageCountComposite.getT_average(); //(count*rating)/ totalcount
            }

            countComposite.setT_count(t_count);
            countComposite.setT_average(totalSum/t_count);
            context.write(key, countComposite);

        } catch (Exception e) {
            System.out.println("Exception in AverageRatingOfProductReducer class : ");
            log.error(e.getMessage());
        }
    }

}
