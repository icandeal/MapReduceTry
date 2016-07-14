import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * Created by sniper on 16-1-15.
 */
public class HDFSUtils {
    public static FileSystem getFileSystem(String fsPath) throws Exception {
        FileSystem fs = FileSystem.get(new URI(fsPath), new Configuration());
        return fs;
    }

    public static void listFiles(FileSystem fs, Path path) throws Exception{
        FileStatus[] fileStatusArray = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatusArray) {
            String isDir = fileStatus.isDir()?"文件夹":"文件";
            final String permission = fileStatus.getPermission().toString();
            final short replication = fileStatus.getReplication();
            final long len = fileStatus.getLen();
            final String p = fileStatus.getPath().toString();
            System.out.println(isDir+"\t"+permission+"\t"+replication+"\t"+len+"\t"+p);
            if(isDir.equals("文件夹"))
                listFiles(fs, new Path(p));
        }

    }

    /**
     * 获取文件数据
     * @param fs    文件系统对象
     * @param src  文件系统文件路径
     * @param des   目标文件路径
     * @return      是否成功
     */
    public static boolean getData(FileSystem fs, Path src, String des) {
        boolean rtn = false;
        if(fs == null || des == null) {
            return rtn;
        }
        try {
            if(!fs.exists(src)){
                throw new FileNotFoundException();
            }

            File file = new File(des);
            File f = new File(file.getParent());
            if(!f.exists()) {
                f.mkdirs();
            }

            FSDataInputStream in = fs.open(src);
            FileOutputStream out = new FileOutputStream(des);
            IOUtils.copyBytes(in, out, 2048, true);
            rtn = true;
        } catch(Exception e) {
            System.out.print(e.getMessage());
        } finally {
            return rtn;
        }
    }

    /**
     * 上传文件数据
     * @param fs    文件系统对象
     * @param src  文件系统文件路径
     * @param des   目标文件路径
     * @return      是否成功
     */
    public static boolean putData(FileSystem fs, String src, Path des) {
        boolean rtn = false;
        if(fs == null || src == null) {
            return rtn;
        }
        try {
            File file = new File(src);
            if(!file.exists()) {
                throw new FileNotFoundException();
            }

            deleteFile(fs, des);

            FSDataOutputStream out = fs.create(des);
            FileInputStream in = new FileInputStream(src);
            IOUtils.copyBytes(in, out, 2048, true);
            rtn = true;
        } catch(Exception e) {
            System.out.print(e.getMessage());
        } finally {
            return rtn;
        }
    }

    public static boolean deleteFile(FileSystem fs, Path path) {
        boolean rtn = false;
        try {
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
            rtn = true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            return rtn;
        }
    }
}
