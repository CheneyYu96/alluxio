package alluxio.master.repl.meta;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.CommonUtils;
import fr.client.utils.OffLenPair;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Record intra-file access info
 *
 */
public class FileAccessInfo {
    private AlluxioURI mFilePath;
    private Map<OffLenPair, Long> offsetCount;


    private long queryNum;
    private long lastAccessTime; /* estimate query based on interval */
    private long recordInterval;

    public FileAccessInfo(AlluxioURI filePath) {
        mFilePath = filePath;
        offsetCount = new ConcurrentHashMap<>();

        queryNum = 0;
        lastAccessTime = 0;
        recordInterval = Configuration.getLong(PropertyKey.FR_RECORD_INTERVAL);
    }

    public FileAccessInfo(AlluxioURI mFilePath, OffLenPair accessPair) {
        this(mFilePath);
        incCount(accessPair);
    }

    public AlluxioURI getFilePath() {
        return mFilePath;
    }

    public Map<OffLenPair, Long> getOffsetCount() {
        return offsetCount;
    }

    public long getQueryNum() {
        return queryNum;
    }

    public void incCount(OffLenPair offLenPair){
        offsetCount.merge(offLenPair, (long) 1, Long::sum);
        long currentTime = CommonUtils.getCurrentMs();
        if (currentTime - lastAccessTime > recordInterval){
            queryNum++;
        }
        lastAccessTime = currentTime;
    }
}
