package alluxio.master.repl.meta;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.collections.ConcurrentHashSet;
import alluxio.util.CommonUtils;
import fr.client.utils.OffLenPair;

import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private Set<OffLenPair> offsetWithinQuery;

    public FileAccessInfo(AlluxioURI filePath) {
        mFilePath = filePath;
        offsetCount = new ConcurrentHashMap<>();

        queryNum = 0;
        lastAccessTime = 0;
        recordInterval = Configuration.getLong(PropertyKey.FR_RECORD_INTERVAL);

        offsetWithinQuery = new ConcurrentHashSet<>();
    }

    public FileAccessInfo(AlluxioURI mFilePath, OffLenPair accessPair) {
        this(mFilePath);
        incCount(accessPair);
    }

    public FileAccessInfo(AlluxioURI mFilePath, List<OffLenPair> allEmptyPair) {
        this(mFilePath);
        for (OffLenPair pair: allEmptyPair){
            offsetCount.put(pair, (long) 0);
        }
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

        // check if it belongs to a new query
        if (currentTime - lastAccessTime > recordInterval){
            queryNum++;
            offsetWithinQuery.clear();
        }
        else {
            if (offsetWithinQuery.contains(offLenPair)){
                queryNum++;
                offsetWithinQuery.clear();
            }
            else {
                offsetWithinQuery.add(offLenPair);
            }
        }

        lastAccessTime = currentTime;
    }
}
