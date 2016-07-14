import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sniper on 16-1-14.
 */
public class MyReducer extends Reducer<TemWritable, IntWritable, TemWritable, IntWritable> {
    @Override
    protected void reduce(TemWritable tem, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxVal = Integer.MIN_VALUE;
        for(IntWritable intWritable:values) {
            maxVal = Math.max(maxVal,Integer.parseInt(intWritable.toString()));
        }
        context.write(tem,new IntWritable(maxVal));
    }
}