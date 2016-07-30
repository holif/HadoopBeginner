import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
/**
 *  HDFS JavaAPI uses
 */
public class HDFSDao {
	private Configuration conf =null;
	public HDFSDao(){
		conf =new Configuration();
		//load config file
		conf.addResource(new Path("/hadoop/etc/hadoop/core-site.xml"));
	}
	public HDFSDao(Configuration conf){
		this.conf =conf;
	}	
	public static void main(String[] args) throws IOException {
		HDFSDao HDFSDao = new HDFSDao();
		HDFSDao.lsFile("/user/root/");
	}	
	/*Upload file to HDFS*/
	public boolean  uploadFile(String path,String localfile){
		File file=new File(localfile);
		if (!file.isFile()) {
			System.out.println(file.getName());
			return false;
		}
		try {			
			FileSystem fs =FileSystem.get(conf);
			InputStream in = new BufferedInputStream(new FileInputStream(localfile));
			OutputStream out = fs.create(new Path(path+"/"+file.getName()));
			IOUtils.copyBytes(in, out, 4096, true);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	/* Download file from HDFS */
	public boolean downloadFile(String hadfile,String localPath){
		try {
			FileSystem fs =FileSystem.get(conf);
			fs.copyToLocalFile(new Path(hadfile), new Path(localPath));
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	/*Create a folder*/
	public void mkdirFolder(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            System.out.println("Create: " + folder);
        }
        fs.close();
    }
    /* create a file by content */
    public void createFile(String file, String content) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {
            os = fs.create(new Path(file));
            os.write(buff, 0, buff.length);
            System.out.println("Create: " + file);
        } finally {
            if (os != null)
                os.close();
        }
        fs.close();
    }
	/* Delete file from HDFS */
	public boolean deleteFile(String hadfile){
		try {
			FileSystem hadoopFS =FileSystem.get(conf);
			Path hadPath=new Path(hadfile);
			Path p=hadPath.getParent();
			boolean rtnval= hadoopFS.delete(hadPath, true);
			FileStatus[] hadfiles= hadoopFS.listStatus(p);
			for(FileStatus fs :hadfiles){
				System.out.println(fs.toString());
			}
			return rtnval;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	/* Delete a folder */
    public void deleteFolder(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(path);
        System.out.println("Delete: " + folder);
        fs.close();
    }
	/* Rename a file of HDFS */
	public void renameFile(String src, String dst) throws IOException {
        Path name1 = new Path(src);
        Path name2 = new Path(dst);
        FileSystem fs = FileSystem.get(conf);
        fs.rename(name1, name2);
        System.out.println("Rename from " + src + " to " + dst);
        fs.close();
    }
	/* Show file list */
	public void lsFile(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] list = fs.listStatus(path);
        System.out.println("---ls: " + folder);
        if(list.length>0){
        	  for (FileStatus f : list) {
                  System.out.printf("Path: %s, idFile: %s, Size: %d\n", f.getPath(), f.isFile(), f.getLen());
                  if(!f.isFile()){
                  	String temppath = f.getPath().toString();
                  	String[] tempfolder = temppath.split("9000");
                  	lsFile(tempfolder[1]);
                  }
              }
        }
        fs.close();
    }
}