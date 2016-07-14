import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by sniper on 16-1-18.
 */
public class MyGroupingComparator implements RawComparator<TemWritable> {

    @Override
    public int compare(TemWritable o1, TemWritable o2) {
        //按照字典序比较
        return o1.getCountry().compareTo(o2.getCountry());
    }

    /**
     * @param arg0 表示第一个参与比较的字节数组
     * @param arg1 表示第一个参与比较的字节数组的起始位置
     * @param arg2 表示第一个参与比较的字节数组的偏移量
     *
     * @param arg3 表示第二个参与比较的字节数组
     * @param arg4 表示第二个参与比较的字节数组的起始位置
     * @param arg5 表示第二个参与比较的字节数组的偏移量
     */
    @Override
    public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
                       int arg4, int arg5) {
        return WritableComparator.compareBytes(arg0, arg1, arg2, arg3, arg4, arg5);
    }
}
