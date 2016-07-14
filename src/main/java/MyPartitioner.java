import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by sniper on 16-1-14.
 */
public class MyPartitioner extends Partitioner<TemWritable , IntWritable> {
    @Override
    public int getPartition(TemWritable tem, IntWritable intWritable, int i) {
        if(tem!=null && tem.getCountry()!=null) {
            if (tem.getCountry().contains("中")) {
                return 0;
            } else if (tem.getCountry().contains("美")) {
                return 1;
            }
        }
        return 2;
    }
}