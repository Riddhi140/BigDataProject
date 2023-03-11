package src.mapreduce.UserPurchaseHistoryInvertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PurchaseHistoryReducer extends Reducer<Text,Text,Text,Text> {

    private Text purchasedItemsList = new Text();

    @Override
    public void reduce(Text custIDKey, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        try {
            StringBuilder builder = new StringBuilder();
            boolean firstRecord = true;

            for(Text prodID: values){
                if(firstRecord){
                    firstRecord = false;
                }
                else{
                    builder.append("\t");
                }
                builder.append(prodID.toString());
            }

            purchasedItemsList.set(builder.toString());
            context.write(custIDKey, purchasedItemsList);

        } catch (Exception e) {
            System.out.println("Exception in PurchaseHistoryReducer class: ");
        }
    }
}

