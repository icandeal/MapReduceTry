import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by sniper on 16-1-14.
 */
public class MyMapper extends Mapper<LongWritable,Text,TemWritable, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String [] wordArray = value.toString().split("\t");

        if(wordArray!=null && wordArray.length>2) {
            TemWritable tem = new TemWritable(wordArray[0],wordArray[1]);
            context.write(tem,new IntWritable(Integer.valueOf(wordArray[2].toString())));
        }
    }
}
