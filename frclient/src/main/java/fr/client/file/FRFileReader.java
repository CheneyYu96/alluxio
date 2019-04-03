package fr.client.file;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 * This executable is to read hot data segments from files
 */
public class FRFileReader {
    private FileSystem mFileSystem;
    private AlluxioURI mSourceFilePath;
    private OpenFileOptions mReadOptions;
    private static final Logger LOG = LoggerFactory.getLogger(FRFileReader.class);


    private byte[] mBuf;

    public FRFileReader(AlluxioURI filePath) {
        mSourceFilePath = filePath;

        mFileSystem = FileSystem.Factory.get();
        mReadOptions = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
    }

    public byte[] getBuf() {
        return mBuf;
    }

    public void setReadOption(OpenFileOptions readOptions) {
        mReadOptions = readOptions;
    }

    /**
     *
     * @param offset
     * @param length
     * @return actual read length
     * @throws IOException
     * @throws AlluxioException
     */
    public int readFile(long offset, long length) throws IOException, AlluxioException {
        long startTimeMs = CommonUtils.getCurrentMs();

        FileInStream is = mFileSystem.openFile(mSourceFilePath, mReadOptions);
        mBuf = new byte[(int) length];

        is.seek(offset);
        int toRead = is.read(mBuf, 0, (int) length);
        is.close();

        LOG.info("Read file:" + mSourceFilePath.getPath() + "; offset:" + offset + "; length:" + toRead + "; elapsed:" + (CommonUtils.getCurrentMs() - startTimeMs));
        return toRead;
    }

}
