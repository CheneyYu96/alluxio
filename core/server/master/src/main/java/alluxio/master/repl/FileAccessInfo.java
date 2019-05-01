package alluxio.master.repl;

import alluxio.AlluxioURI;
import com.google.common.collect.ImmutableMap;
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

    public FileAccessInfo(AlluxioURI mFilePath) {
        this.mFilePath = mFilePath;
        this.offsetCount = new ConcurrentHashMap<>();
    }

    public FileAccessInfo(AlluxioURI mFilePath, OffLenPair accessPair) {
        this.mFilePath = mFilePath;
        this.offsetCount = new ConcurrentHashMap<>(ImmutableMap.of(accessPair, (long) 1));
    }

    public AlluxioURI getFilePath() {
        return mFilePath;
    }

    public Map<OffLenPair, Long> getOffsetCount() {
        return offsetCount;
    }

    public void incCount(OffLenPair offLenPair){
        offsetCount.merge(offLenPair, (long) 1, Long::sum);
    }
}
