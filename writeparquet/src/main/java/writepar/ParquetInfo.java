package writepar;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;
import fr.client.utils.FRUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ParquetInfo {
    private FileSystem mFileSystem;

    private final String locationsFile;

    public ParquetInfo() {
        mFileSystem = FileSystem.Factory.get();
        locationsFile = System.getProperty("user.dir") + "/origin-locs.txt";

    }

    public void sendInfo(String filePath, String infoPath) throws IOException{
        long startTimeMs = CommonUtils.getCurrentMs();

        List<Long> offset = new ArrayList<>();
        List<Long> length = new ArrayList<>();
        FileInputStream is = new FileInputStream(infoPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String str;
        String [] temp;
        while (true) {
            str = reader.readLine();
            if(str!=null){
                temp = str.split(",");
                offset.add(Long.parseLong(temp[0]));
                length.add(Long.parseLong(temp[1]));
            } else{
                break;
            }
        }
        is.close();

        mFileSystem.sendParquetInfo(new AlluxioURI(filePath), offset, length);

        System.out.println("Send parquet file info. " + filePath + "; elapsed:" + (CommonUtils.getCurrentMs() - startTimeMs));

        WorkerNetAddress address = FRUtils.getFileLocation(new AlluxioURI(filePath), mFileSystem, FileSystemContext.get());
        if (address == null){
            System.err.println("File block address is null");
            return;
        }
        FileWriter fw = new FileWriter(locationsFile, true);
        fw.write(filePath + "," + address.getHost() + "\n");
        fw.close();

    }
}
