package alluxio.master.stats;

import alluxio.wire.FileSegmentsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileSegmentsAccessRecorder implements AccessRecorder {
  private ConcurrentHashMap<FileSegmentsInfo, Long> mAccessHistory = new ConcurrentHashMap<FileSegmentsInfo, Long>();
  private static final Logger LOG = LoggerFactory.getLogger(FileSegmentsAccessRecorder.class);

  public FileSegmentsAccessRecorder(){}

  public void onAccess(FileSegmentsInfo fileSegmentsInfo) {
    // mTotalCount += 1;

    mAccessHistory.putIfAbsent(fileSegmentsInfo, 0L); // atomic operation
    mAccessHistory.put(fileSegmentsInfo, mAccessHistory.get(fileSegmentsInfo)+1);

    //if (mAccessHistory.containsKey(fileSegmentsInfo)) {
    //  mAccessHistory.put(fileSegmentsInfo, mAccessHistory.get(fileSegmentsInfo)+1);
    //}
    //else
    //   mAccessHistory.put(fileSegmentsInfo, 1L);
    // if(mTotalCount % mThreshold == 0) {
    // doLog();
    // }
    StringBuffer sBuffer = new StringBuffer("File segments accesss Info:\n");
    Iterator<Map.Entry<FileSegmentsInfo, Long>> entries = mAccessHistory.entrySet().iterator();
    while (entries.hasNext()) {

      Map.Entry<FileSegmentsInfo, Long> entry = entries.next();
      sBuffer.append("file: " + entry.getKey().getFilePath() + "  offset: "+ entry.getKey().getOffset()
          + "  length: " + entry.getKey().getLength() +",   Access Count = " + entry.getValue());
    }
    LOG.info(sBuffer.toString());

  }

  public ConcurrentHashMap<FileSegmentsInfo, Long> getFileSegmentsAccessRecorder(){
    return mAccessHistory;
  }
}
