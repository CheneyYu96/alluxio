import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ParquetInfo {
    private FileSystem mFileSystem;

    private static final Logger LOG = LoggerFactory.getLogger(ParquetInfo.class);


    public ParquetInfo() {
        mFileSystem = FileSystem.Factory.get();
    }

    public void sendInfo(String localFilePath) throws IOException{
        long startTimeMs = CommonUtils.getCurrentMs();

        Runtime run = Runtime.getRuntime();

        String absPath = System.getProperty("user.dir");
        String outputPath = absPath + "/output.txt";
        String pythonPath = absPath + "/offsetParser.py";
        String infoPath = absPath + "/offset.txt";

        run.exec("parquet column-index " + localFilePath + " > " + outputPath);
        run.exec("python3 " + pythonPath + " " + outputPath);

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

        // Assume local path is the same with alluxio path
        mFileSystem.sendParquetInfo(new AlluxioURI(localFilePath), offset, length);

        LOG.info("Send parquet file info. " + localFilePath + "; elapsed:" + (CommonUtils.getCurrentMs() - startTimeMs));
    }
}
