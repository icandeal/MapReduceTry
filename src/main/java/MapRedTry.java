import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
/**
 * Created by sniper on 16-1-14.
 */
public class MapRedTry {
    static final String HDFS_PATH = "http://gw4.bj.etiantian.net:31732";
    static final String YCF_HDFS_PATH = "hdfs://192.168.10.200:9000";

    public static void main(String[] args) throws Exception {
        HDFSUtils.deleteFile(HDFSUtils.getFileSystem(YCF_HDFS_PATH), new Path(YCF_HDFS_PATH+"/tmp/output/ccc"));

        Job job = Job.getInstance(new Configuration(), "my first mr");
        Path path = new Path(YCF_HDFS_PATH+"/tmp/ycf/aaa");

//        job.setOutputFormatClass(TableOutputFormat.class);
        //设置输入路径
        FileInputFormat.setInputPaths(job, path);

        job.setOutputFormatClass(NullOutputFormat.class);
        //设置任务数据的输入路径；
        //设置输入格式
        job.setInputFormatClass(TextInputFormat.class);

        //设置Mapper类
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(TemWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置分区
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(3);

        job.setGroupingComparatorClass(MyGroupingComparator.class);

        //设置过滤
        job.setCombinerClass(MyCombiner.class);

        //设置Reducer类
        job.setReducerClass(MyReducer.class);

        //设置输出类型
        job.setOutputKeyClass(TemWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(YCF_HDFS_PATH+"/tmp/output/ccc"));

        //执行任务
        job.waitForCompletion(true);
    }
}
