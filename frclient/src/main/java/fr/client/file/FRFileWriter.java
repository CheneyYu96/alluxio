package fr.client.file;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
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
 * This executable is to write hot data segments to file system
 */
public class FRFileWriter {
    private FileSystem mFileSystem;
    private CreateFileOptions mWriteOptions;
    private AlluxioURI mFilePath;

    private static final Logger LOG = LoggerFactory.getLogger(FRFileWriter.class);


    public FRFileWriter(AlluxioURI filePath) {
        mFilePath = filePath;
        mFileSystem = FileSystem.Factory.get();

        // TODO set custom policy
        mWriteOptions = CreateFileOptions.defaults()
                .setWriteType(WriteType.MUST_CACHE);
    }

    public void setWriteOption(CreateFileOptions writeOptions) {
        mWriteOptions = writeOptions;
    }

    public void writeFile(byte[] buf) throws IOException, AlluxioException {
        long startTimeMs = CommonUtils.getCurrentMs();

        FileOutStream os = mFileSystem.createFile(mFilePath, mWriteOptions);
        os.write(buf);
        os.close();

        Runtime run = Runtime.getRuntime();
        run.exec("parquet column-index "+mFilePath+" > output.txt");
        run.exec("python3 offsetParser.py output.txt");

        List<Long> offset = new ArrayList<>();
        List<Long> length = new ArrayList<>();
        FileInputStream is = new FileInputStream("offset.txt");
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
        mFileSystem.sendParquetInfo(mFilePath,offset,length);

        LOG.info("Write to file:" + mFilePath.getPath() + "; elapsed:" + (CommonUtils.getCurrentMs() - startTimeMs));
    }

}
