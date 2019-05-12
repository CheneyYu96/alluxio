package alluxio.wire;


import com.google.common.base.Objects;

import java.io.Serializable;


public class FileSegmentsInfo implements Serializable {
  private String mUFSPath;
  private long mLength;
  private long mOffset;

  /**
   * Creates a new instance of {@link FileSegmentsInfo}.
   */
  public FileSegmentsInfo() {}

  public FileSegmentsInfo(String UFSPath, long offset, long length){
    mUFSPath = UFSPath;
    mOffset = offset;
    mLength = length;
  }

  /**
   * @return the UFSPath of the file
   */
  public String getFilePath() {
    return mUFSPath;
  }

  /**
   * @return the segment length
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @return the segment offset
   */
  public long getOffset() {
    return mOffset;
  }

  /**
   * @param UFSPath the UFSPath of the file
   * @return the segment information
   */
  public FileSegmentsInfo setFilePath(String UFSPath) {
    mUFSPath = UFSPath;
    return this;
  }

  /**
   * @param length the segment length to use
   * @return the segment information
   */
  public FileSegmentsInfo setLength(long length) {
    mLength = length;
    return this;
  }

  /**
   * @param offset the segment offset to use
   * @return the segment information
   */
  public FileSegmentsInfo setOffset(long offset) {
    mOffset = offset;
    return this;
  }

  public void addLength(long delta){
    mLength = mLength + delta;
  }

//  /**
//   * @return thrift representation of the segment information
//   */
//  public alluxio.thrift.BlockInfo toThrift() {
//    List<alluxio.thrift.BlockLocation> locations = new ArrayList<>();
//    for (BlockLocation location : mLocations) {
//      locations.add(location.toThrift());
//    }
//    return new alluxio.thrift.BlockInfo(mBlockId, mLength, locations);
//  }
//
//  /**
//   * Creates a new instance of {@link BlockInfo} from a thrift representation.
//   *
//   * @param blockInfo the thrift representation of a block information
//   * @return the instance
//   */
//  public static BlockInfo fromThrift(alluxio.thrift.BlockInfo blockInfo) {
//    return new BlockInfo()
//        .setBlockId(blockInfo.getBlockId())
//        .setLength(blockInfo.getLength())
//        .setLocations(map(BlockLocation::fromThrift, blockInfo.getLocations()));
//  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileSegmentsInfo)) {
      return false;
    }
    FileSegmentsInfo that = (FileSegmentsInfo) o;
    return mUFSPath.equals(that.mUFSPath) && mLength == that.mLength
        && mOffset == that.mOffset;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mUFSPath, mLength, mOffset);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("id", mUFSPath).add("length", mLength)
        .add("offset", mOffset).toString();
  }
}
