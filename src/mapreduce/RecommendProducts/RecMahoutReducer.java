package src.mapreduce.RecommendProducts;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public class RecMahoutReducer extends Reducer<Text, NullWritable, NullWritable, Text> {

    private String path = new String();
    private DataModel data_Model;
    private UserSimilarity user_Similarity;
    private UserNeighborhood user_Neighborhood;
    private Recommender generic_Recommender;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException, FileNotFoundException {

        try {
            this.path = context.getConfiguration().get("DataPath");
            String fname = "/part-r-00000";
            this.path = this.path + fname;

            this.data_Model = new FileDataModel(new File(path));

            this.user_Similarity = new PearsonCorrelationSimilarity(this.data_Model);

            this.user_Neighborhood = new NearestNUserNeighborhood(1000, this.user_Similarity, this.data_Model);

            // Create a generic user based recommender with the dataModel, the userNeighborhood and the userSimilarity
            this.generic_Recommender = new GenericUserBasedRecommender(this.data_Model,
                    this.user_Neighborhood, this.user_Similarity);

        } catch (FileNotFoundException ex) {
            System.out.println("Exception: " + ex.getMessage());
        }	catch (TasteException e) {
            e.printStackTrace();
        }	catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        try {

            Long userId = Long.valueOf(key.toString());
            List<RecommendedItem> recs = generic_Recommender.recommend(userId,2);

            if (!recs.isEmpty()) {

                Text res = new Text();
                for (RecommendedItem recommendedItem : recs) {

                    res.set(key.toString() + " Recommend Item Id: " + recommendedItem.getItemID() +
                            " Strength of preference: " + recommendedItem.getValue());
                }
                context.write(NullWritable.get(), res);
            }

        } catch (Exception e) {
            System.out.println("Exception in RecMahoutReducer: ");
            e.printStackTrace();
        }
    }
}

