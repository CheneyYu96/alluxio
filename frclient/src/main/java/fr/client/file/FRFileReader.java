package fr.client.file;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import fr.client.utils.OffLenPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

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

    public FRFileReader(AlluxioURI filePath, boolean requireTrans) {
        mSourceFilePath = filePath;

        mFileSystem = FileSystem.Factory.get();
        mReadOptions = OpenFileOptions
                .defaults()
                .setReadType(ReadType.NO_CACHE)
                .setRequireTrans(requireTrans);
    }

    public byte[] getBuf() {
        return mBuf;
    }

    public void setReadOption(OpenFileOptions readOptions) {
        mReadOptions = readOptions;
    }

    /**
     *
     * @return actual read length
     * @throws IOException
     * @throws AlluxioException
     */
    public int readFile(List<OffLenPair> pairs) throws IOException, AlluxioException {
        long startTimeMs = CommonUtils.getCurrentMs();

        FileInStream is = mFileSystem.openFile(mSourceFilePath, mReadOptions);

        long length = pairs
                .stream()
                .mapToLong( o -> o.length)
                .sum();
        mBuf = new byte[(int) length];

        int toRead = 0;
        for(OffLenPair pair : pairs){
            is.seek(pair.offset);
            toRead += is.read(mBuf, toRead, (int) pair.length);
        }
        is.close();

        LOG.info("Read file:" + mSourceFilePath.getPath() + "; length:" + toRead + "; elapsed:" + (CommonUtils.getCurrentMs() - startTimeMs));
        return toRead;
    }

}
