package alluxio.master.repl.meta;

import alluxio.AlluxioURI;
import fr.client.utils.OffLenPair;

import java.util.ArrayList;
import java.util.List;

/**
 * Record offset information of a parquet file.
 */
public class FileOffsetInfo {
    private AlluxioURI mFilePath;
    private List<OffLenPair> offsetList;

    /**
     * Default Constructor with file path known.
     * @param path filePath
     */
    public FileOffsetInfo(AlluxioURI path){
        mFilePath = path;
        offsetList = new ArrayList<>();
    }

    /**
     * FileOffsetInfo Constructor
     * @param path filePath
     * @param offset column offset list
     * @param length corresponding length list
     */
    public FileOffsetInfo(AlluxioURI path, List<Long> offset, List<Long> length) {
        mFilePath = path;
        offsetList = new ArrayList<>();
        for (int i = 0; i < offset.size(); i++) {
            offsetList.add(new OffLenPair(offset.get(i), length.get(i)));
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

}
