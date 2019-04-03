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

import java.io.IOException;

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

        LOG.info("Write to file:" + mFilePath.getPath() + "; elapsed:" + (CommonUtils.getCurrentMs() - startTimeMs));
    }

}
