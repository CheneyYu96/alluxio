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
    private long lastOff;

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
        genOffsetPair(offset, length);
        lastOff = offset.get(offset.size() - 1) + length.get(length.size() - 1);
    }

    private void genOffsetPair(List<Long> offsets, List<Long> lengths){
//        long startIndex = 4;
        for (int i = 0; i < offsets.size(); i++) {
//            if (i == 0 && startIndex < offsets.get(i)){
//                addNewOff(startIndex, offsets.get(i) - startIndex);
//            }
//
//            if (offsetList.size() > 0){
//                OffLenPair lastOff = offsetList.get(offsetList.size() - 1);
//
//                if (lastOff.offset + lastOff.length < offsets.get(i)){
//                    addNewOff(lastOff.offset + lastOff.length, offsets.get(i) - (lastOff.offset + lastOff.length));
//                }
//            }

            addNewOff(offsets.get(i), lengths.get(i));
        }
    }

    private void addNewOff(long offset, long length){
        OffLenPair newOff = new OffLenPair(offset, length);
        offsetList.add(newOff);
        offLenPairMap.put(offset, newOff);
    }

    public AlluxioURI getFilePath() {
        return mFilePath;
    }

    public List<OffLenPair> getPairsByOffLen(long off, long len){
        List<OffLenPair> pairs = new ArrayList<>();
        if(offLenPairMap.containsKey(off)){
            OffLenPair firstPair = offLenPairMap.get(off);
            pairs.add(firstPair);

            OffLenPair nextPair = firstPair;

            while (nextPair.offset + nextPair.length < len){
                nextPair = offLenPairMap.get(nextPair.offset + nextPair.length);
                pairs.add(nextPair);
            }
        }

        return pairs;
    }

    public OffLenPair getPairByOffset(long offset){
        return offLenPairMap.get(offset);
    }

    public void recordOffSet(long offset, long length){
        if (offset > 0 && offset < lastOff) {
            addNewOff(offset, length);
        }
    }

    @Override
    public String toString() {
        return "FileOffsetInfo{" +
                "mFilePath=" + mFilePath +
                ", offsetList=" + offsetList +
                '}';
    }
}
