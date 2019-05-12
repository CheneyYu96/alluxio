package alluxio.master.repl.meta;

import alluxio.AlluxioURI;
import fr.client.utils.OffLenPair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Record offset information of a parquet file.
 */
public class FileOffsetInfo {
    private AlluxioURI mFilePath;
    private List<OffLenPair> offsetList;
    private Map<Long, OffLenPair> offLenPairMap;

    /**
     * Default Constructor with file path known.
     * @param path filePath
     */
    public FileOffsetInfo(AlluxioURI path){
        mFilePath = path;
        offsetList = new ArrayList<>();
        offLenPairMap = new ConcurrentHashMap<>();
    }

    /**
     * FileOffsetInfo Constructor
     * @param path filePath
     * @param offset column offset list
     * @param length corresponding length list
     */
    public FileOffsetInfo(AlluxioURI path, List<Long> offset, List<Long> length) {
        this(path);
        for (int i = 0; i < offset.size(); i++) {
            OffLenPair newOff = new OffLenPair(offset.get(i), length.get(i));
            offsetList.add(newOff);
            offLenPairMap.put(offset.get(i), newOff);
        }
    }

    public AlluxioURI getFilePath() {
        return mFilePath;
    }

    public List<OffLenPair> getOffsetList() {
        return offsetList;
    }

    public OffLenPair getOffset(int i){
        return offsetList.get(i);
    }

    public OffLenPair getPairByOffset(long offset){
        return offLenPairMap.get(offset);
    }

}
