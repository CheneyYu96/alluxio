/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.Seekable;
import alluxio.annotation.PublicApi;
import alluxio.client.BoundedStream;
import alluxio.client.PositionedReadable;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.collections.Pair;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.UnavailableException;
import alluxio.network.netty.NettyRPC;
import alluxio.network.netty.NettyRPCContext;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.retry.CountingRetry;
import alluxio.util.CommonUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileSegmentsInfo;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A streaming API to read a file. This API represents a file as a stream of bytes and provides a
 * collection of {@link #read} methods to access this stream of bytes. In addition, one can seek
 * into a given offset of the stream to read.
 *
 * This class wraps the block in stream for each of the blocks in the file and abstracts the
 * switching between streams. The backing streams can read from Alluxio space in the local machine,
 * remote machines, or the under storage system.
 *
 * The internal bookkeeping works as follows:
 *
 * 1. {@link #updateStream()} is a potentially expensive operation and is responsible for
 * creating new BlockInStreams and updating {@link #mBlockInStream}. After calling this method,
 * {@link #mBlockInStream} is ready to serve reads from the current {@link #mPosition}.
 * 2. {@link #mPosition} can become out of sync with {@link #mBlockInStream} when seek or skip is
 * called. When this happens, {@link #mBlockInStream} is set to null and no effort is made to
 * sync between the two until {@link #updateStream()} is called.
 * 3. {@link #updateStream()} is only called when followed by a read request. Thus, if a
 * {@link #mBlockInStream} is created, it is guaranteed we read at least one byte from it.
 */
@PublicApi
@NotThreadSafe
public class FileInStream extends InputStream implements BoundedStream, PositionedReadable,
    Seekable {
  private static final Logger LOG = LoggerFactory.getLogger(FileInStream.class);
  private static final int MAX_WORKERS_TO_RETRY =
      Configuration.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_READ_RETRY);

//  private final URIStatus mStatus;
//  private final InStreamOptions mOptions;
  private URIStatus mStatus;
  private InStreamOptions mOptions;
  private final AlluxioBlockStore mBlockStore;
  private final FileSystemContext mContext;

  /* Convenience values derived from mStatus, use these instead of querying mStatus. */
  /** Length of the file in bytes. */
  private final long mLength;
  /** Block size in bytes. */
  private final long mBlockSize;

  /* Underlying stream and associated bookkeeping. */
  /** Current offset in the file. */
  private long mPosition;
  /** Underlying block stream, null if a position change has invalidated the previous stream. */
  private BlockInStream mBlockInStream;

  /** Cached block stream for the positioned read API. */
  private BlockInStream mCachedPositionedReadStream;

  /** The last block id for which async cache was triggered. */
  private long mLastBlockIdCached;

  /** A map of worker addresses to the most recent epoch time when client fails to read from it. */
  private Map<WorkerNetAddress, Long> mFailedWorkers = new HashMap<>();

  /** FR: record replica location info */
  private ReplicasInfo mReplicasInfo;

  private FileSegmentsInfo mCurrentSeg;
  private URIStatus mNewStatus;
  private InStreamOptions mNewOptions;
  private long mNewPosition;

  private long mNewLength;

  private long mNewBlockSize;
  private boolean mFirstRead;
  private boolean mReadData;

  protected FileInStream(URIStatus status, InStreamOptions options, FileSystemContext context) {
    mStatus = status;
    mOptions = options;
    mBlockStore = AlluxioBlockStore.create(context);
    mContext = context;

    mLength = mStatus.getLength();
    mBlockSize = mStatus.getBlockSizeBytes();

    mPosition = 0;
    mBlockInStream = null;
    mCachedPositionedReadStream = null;
    mLastBlockIdCached = 0;

    /** FR add*/
    mReplicasInfo = new ReplicasInfo();

    mNewStatus = status;
    mNewOptions = options;
    mNewPosition = 0;
    mNewLength = mNewStatus.getLength();
    mNewBlockSize = mNewStatus.getBlockSizeBytes();
    mCurrentSeg = new FileSegmentsInfo(status.getPath(), -1, Long.MIN_VALUE);
    mReadData = false;
    mFirstRead = true;

    LOG.info("Generate file in stream for file: {}", mStatus.getPath());

  }

  class ReplicasInfo{
    // TODO: fresh replicas info periodical
    private Map<FileSegmentsInfo, WorkerNetAddress> mReplicaLocations = new ConcurrentHashMap<>();
    private Map<FileSegmentsInfo, URIStatus> mReplicaStatus = new ConcurrentHashMap<>();

    private FileSystem localFileSystem;

    public ReplicasInfo() {
      localFileSystem = FileSystem.Factory.get(mContext);
    }

    public URIStatus getReplicaStatus(FileSegmentsInfo segmentsInfo){
      if (!mReplicaStatus.containsKey(segmentsInfo)){
        updateReplicaInfo(segmentsInfo);
      }
      return mReplicaStatus.get(segmentsInfo);
    }

    public WorkerNetAddress getFileSegLocation(FileSegmentsInfo segmentsInfo){
      if (!mReplicaLocations.containsKey(segmentsInfo)){
        updateReplicaInfo(segmentsInfo);
      }
      return mReplicaLocations.get(segmentsInfo);
    }

    private void updateReplicaInfo(FileSegmentsInfo segmentsInfo){
      try {
        URIStatus replicaStatus = localFileSystem.getStatus(new AlluxioURI(segmentsInfo.getFilePath()));
        long blockId = replicaStatus
                .getBlockIds()
                .get(Math.toIntExact(segmentsInfo.getOffset() / replicaStatus.getBlockSizeBytes()));

        BlockInfo info;
        try (CloseableResource<BlockMasterClient> masterClientResource =
                     mContext.acquireBlockMasterClientResource()) {
          info = masterClientResource.get().getBlockInfo(blockId);
        }

        WorkerNetAddress blockLocation = info
                .getLocations()
                .get(0)
                .getWorkerAddress();

        mReplicaLocations.put(segmentsInfo, blockLocation);
        mReplicaStatus.put(segmentsInfo, replicaStatus);

      } catch (IOException | AlluxioException e) {
        e.printStackTrace();
      }
    }
  }

  public void changeFileInStream(long offset, long length)
      throws FileDoesNotExistException, IOException, AlluxioException {

    FileSystemMasterClient masterClientResource = mContext.acquireMasterClient();

//    masterClientResource
//            .uploadFileSegmentsAccessInfo(new AlluxioURI("<segInfo>" + mStatus.getPath()), mCurrentSeg.getOffset(), mCurrentSeg.getLength());

    List<FileSegmentsInfo> allSegs = masterClientResource
            .uploadFileSegmentsAccessInfo(new AlluxioURI(mStatus.getPath()), offset, length);

    mContext.releaseMasterClient(masterClientResource);

    // decide segment to read
    WorkerNetAddress localWorker = mContext.getLocalWorker();

    // no replicas
    if(allSegs.size() == 1){
      // reading orginal table currently
      if (mNewStatus.getPath().equals(mStatus.getPath())){
        mNewPosition = mPosition;
        updateNewBlockStream();
      }
      else {
        updateMetadata(allSegs.get(0));
      }

      WorkerNetAddress workerToRead = mReplicasInfo.getFileSegLocation(allSegs.get(0));
      if (localWorker != null && localWorker.getHost().equals(workerToRead.getHost())){
        LOG.info("Read local worker. addr: {}; len: {}; originalFile: {}", localWorker.getHost(), length, mNewStatus.getPath());
      }
      else {
        LOG.info("Read nonlocal worker. addr: {}; len: {}; originalFile: {}", workerToRead.getHost(), length, mNewStatus.getPath());
      }
      return;
    }

    // no local worker, usually in master
    if (localWorker == null){
      int index = new Random().nextInt(allSegs.size());
      updateMetadata(allSegs.get(index));
      return;
    }

    List<Pair<FileSegmentsInfo, WorkerNetAddress>> allSegWithLoc = allSegs
            .stream()
            .map(seg -> new Pair<>(seg, mReplicasInfo.getFileSegLocation(seg)))
            .collect(Collectors.toList());

    Pair<FileSegmentsInfo, WorkerNetAddress> segToReadWithLoc = allSegWithLoc
            .stream()
            .filter( p -> localWorker.getHost().equals(p.getSecond().getHost()))
            .findFirst().orElse(null);


    // local worker exists
    if (segToReadWithLoc != null){
      FileSegmentsInfo segToRead = segToReadWithLoc.getFirst();

      if(segToRead.getFilePath().equals(mNewStatus.getPath())){
        mNewPosition = segToReadWithLoc.getFirst().getOffset();
        updateNewBlockStream();
      }
      else {
        updateMetadata(segToRead);
      }
      LOG.info("Read local worker. addr: {}; len: {}; newFile: {}", localWorker.getHost(), length, mNewStatus.getPath());
    }
    else {
      Pair<FileSegmentsInfo, WorkerNetAddress> sameSegToRead = allSegWithLoc
              .stream()
              .filter(p -> mNewStatus.getPath().equals(p.getFirst().getFilePath()))
              .findFirst()
              .orElse(null);

      WorkerNetAddress workerToRead;

      if (sameSegToRead != null){
        // choose the same seg
        mNewPosition = sameSegToRead.getFirst().getOffset();
        workerToRead = sameSegToRead.getSecond();
        updateNewBlockStream();
      }
      else {
        // select seg randomly
        int index = new Random().nextInt(allSegWithLoc.size());
        workerToRead = allSegWithLoc.get(index).getSecond();
        updateMetadata(allSegWithLoc.get(index).getFirst());
      }

      LOG.info("Read nonlocal worker. addr: {}; len: {}; newFile: {}", workerToRead.getHost(), length, mNewStatus.getPath());

    }

  }

  private void updateMetadata(FileSegmentsInfo segToRead) throws IOException {
    mNewStatus = mReplicasInfo.getReplicaStatus(segToRead);
    mNewOptions = OpenFileOptions.defaults().toInStreamOptions(mNewStatus);
    mNewPosition = segToRead.getOffset();
    mNewBlockSize = mNewStatus.getBlockSizeBytes();
    mNewLength = mNewStatus.getLength();
    updateNewBlockStream();
  }

  private void checkStreamUpdate(int len) throws IOException {
    if (mFirstRead){
      if(len > 10) {
        mReadData = true;
      }
      mFirstRead = false;
    }

    if (mReadData) {

      long startTimeMs = CommonUtils.getCurrentMs();
      try {
        changeFileInStream(mPosition, (long) len);
      } catch (AlluxioException e) {
        e.printStackTrace();
      }
      LOG.info("update file in stream. elapsed: {}. mPos: {}. mNewPos: {}. len: {}. fileToRead: {}",
              (CommonUtils.getCurrentMs() - startTimeMs),
              mPosition, mNewPosition, len, mNewStatus.getPath());
    }
    else {
      mNewPosition = mPosition;
      if(len > 100){
        LOG.info("Attemtion. pos: {}. len: {}. file: {}", mNewPosition, len, mNewStatus.getPath());
      }
    }

  }

  /* Input Stream methods */
  @Override
  public int read() throws IOException {
    if (mPosition == mLength) { // at end of file
      return -1;
    }
    CountingRetry retry = new CountingRetry(MAX_WORKERS_TO_RETRY);
    IOException lastException = null;

    if(mOptions.getOptions().isRequireTrans()) {
      checkStreamUpdate(1);
    }
//    LOG.info("read no parameter. mPos: " + mPosition + ". mNewPos: " + mNewPosition);

    while (retry.attempt()) {
      try {
        updateStream();
        int result = mBlockInStream.read();
        if (result != -1) {
          mPosition++;
          mNewPosition++;
        }

        return result;
      } catch (UnavailableException | DeadlineExceededException | ConnectException e) {
        lastException = e;
        if (mBlockInStream != null) {
          handleRetryableException(mBlockInStream, e);
          mBlockInStream = null;
        }
      }
    }
    throw lastException;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    if (len == 0) {
      return 0;
    }
    if (mPosition == mLength) { // at end of file
      return -1;
    }
    LOG.info("read to buffer. mPos: {}. mNewPos: {}. len: {}. path: {}", mPosition, mNewPosition, len, mNewStatus.getPath());

    if(mOptions.getOptions().isRequireTrans()) {
      // continuous access within the new segment
      checkStreamUpdate(len);
    }

    int bytesLeft = len;
    int currentOffset = off;
    CountingRetry retry = new CountingRetry(MAX_WORKERS_TO_RETRY);
    IOException lastException = null;

    while (bytesLeft > 0 && mPosition != mLength && retry.attempt()) {
      try {
        updateStream();
        int bytesRead = mBlockInStream.read(b, currentOffset, bytesLeft);
        if (bytesRead > 0) {
          bytesLeft -= bytesRead;
          currentOffset += bytesRead;
          mPosition += bytesRead;

          mNewPosition += bytesRead; // update position for replicas
        }
        retry.reset();
        lastException = null;
      } catch (UnavailableException | ConnectException | DeadlineExceededException e) {
        lastException = e;
        if (mBlockInStream != null) {
          handleRetryableException(mBlockInStream, e);
          mBlockInStream = null;
        }
      }
    }
    if (lastException != null) {
      throw lastException;
    }

    return len - bytesLeft;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

//    LOG.info("skip. mPos: " + mPosition + ". mNewPos: " + mNewPosition);


    long toSkip = Math.min(n, mLength - mPosition);
    seek(mPosition + toSkip);
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    closeBlockInStream(mBlockInStream);
    closeBlockInStream(mCachedPositionedReadStream);
  }

  /* Bounded Stream methods */
  @Override
  public long remaining() {
    return mLength - mPosition;
  }

  /* Positioned Readable methods */
  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    return positionedReadInternal(pos, b, off, len);
  }

  private int positionedReadInternal(long pos, byte[] b, int off, int len) throws IOException {
    if (pos < 0 || pos >= mLength) {
      return -1;
    }

//    LOG.info("positionedRead. mPos: " + mPosition + ". mNewPos: " + mNewPosition);

    int lenCopy = len;
    CountingRetry retry = new CountingRetry(MAX_WORKERS_TO_RETRY);
    IOException lastException = null;
    while (len > 0 && retry.attempt()) {
      if (pos >= mLength) {
        break;
      }
      long blockId = mStatus.getBlockIds().get(Math.toIntExact(pos / mBlockSize));
      try {
        // Positioned read may be called multiple times for the same block. Caching the in-stream
        // allows us to avoid the block store rpc to open a new stream for each call.
        if (mCachedPositionedReadStream == null) {
          mCachedPositionedReadStream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);
        } else if (mCachedPositionedReadStream.getId() != blockId) {
          closeBlockInStream(mCachedPositionedReadStream);
          mCachedPositionedReadStream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);
        }
        long offset = pos % mBlockSize;
        int bytesRead = mCachedPositionedReadStream.positionedRead(offset, b, off,
            (int) Math.min(mBlockSize - offset, len));
        Preconditions.checkState(bytesRead > 0, "No data is read before EOF");
        pos += bytesRead;
        off += bytesRead;
        len -= bytesRead;
        retry.reset();
        lastException = null;
        triggerAsyncCaching(mCachedPositionedReadStream);
      } catch (UnavailableException | DeadlineExceededException | ConnectException e) {
        lastException = e;
        if (mCachedPositionedReadStream != null) {
          handleRetryableException(mCachedPositionedReadStream, e);
          mCachedPositionedReadStream = null;
        }
      }
    }
    if (lastException != null) {
      throw lastException;
    }
    return lenCopy - len;
  }

  /* Seekable methods */
  @Override
  public long getPos() {
    return mPosition;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (mPosition == pos) {
      return;
    }
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLength,
        PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(), pos);

    if (mBlockInStream == null) { // no current stream open, advance position
      mPosition = pos;
      return;
    }

    long delta = pos - mPosition;

    if(mOptions.getOptions().isRequireTrans()) {
      mNewPosition = -1; // refresh file in stream
    }
    else {
      if (delta <= mBlockInStream.remaining() && delta >= -mBlockInStream.getPos()) { // within block
        mBlockInStream.seek(mBlockInStream.getPos() + delta);
      } else { // close the underlying stream as the new position is no longer in bounds
        closeBlockInStream(mBlockInStream);
      }
    }
    mPosition += delta;

    LOG.info("seek pos. mPos: " + mPosition + ". mNewPos: " + mNewPosition + ". fileToRead: " + mNewStatus.getPath());

  }

  /**
   * Initializes the underlying block stream if necessary. This method must be called before
   * reading from mBlockInStream.
   */
  private void updateStream() throws IOException {
    if (mBlockInStream != null && mBlockInStream.remaining() > 0) { // can still read from stream
      return;
    }

    if (mBlockInStream != null && mBlockInStream.remaining() == 0) { // current stream is done
      closeBlockInStream(mBlockInStream);
    }

    /* Create a new stream to read from mPosition. */
    // Calculate block id.

    if(mOptions.getOptions().isRequireTrans()) {
      updateNewBlockStream();
    }
    else{
      long blockId = mStatus.getBlockIds().get(Math.toIntExact(mPosition / mBlockSize));
      // Create stream
      mBlockInStream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);
      // Set the stream to the correct position.
      long offset = mPosition % mBlockSize;
      mBlockInStream.seek(offset);
    }
  }

  private void updateNewBlockStream() throws IOException {
    long blockId = mNewStatus.getBlockIds().get(Math.toIntExact(mNewPosition / mNewBlockSize));
    mBlockInStream = mBlockStore.getInStream(blockId, mNewOptions, mFailedWorkers);
    long offset = mNewPosition % mNewBlockSize;
    mBlockInStream.seek(offset);

    LOG.info("update block stream. mPos: {}. mNewPos: {}. blockSize: {}. blockId: {}. offset: {}. file: {}",
            mPosition, mNewPosition, mNewBlockSize, blockId, offset, mNewStatus.getPath());
  }

  private void closeBlockInStream(BlockInStream stream) throws IOException {
    if (stream != null) {
      BlockInStream.BlockInStreamSource blockSource = stream.getSource();
      stream.close();
      // TODO(calvin): we should be able to do a close check instead of using null
      if (stream == mBlockInStream) { // if stream is instance variable, set to null
        mBlockInStream = null;
      }
      if (blockSource == BlockInStream.BlockInStreamSource.LOCAL) {
        return;
      }
      triggerAsyncCaching(stream);
    }
  }

  // Send an async cache request to a worker based on read type and passive cache options.
  private void triggerAsyncCaching(BlockInStream stream) throws IOException {
    boolean cache = mOptions.getOptions().getReadType().isCache();
    boolean overReplicated = mStatus.getReplicationMax() > 0
        && mStatus.getFileBlockInfos().get((int) (getPos() / mBlockSize))
        .getBlockInfo().getLocations().size() >= mStatus.getReplicationMax();
    cache = cache && !overReplicated;
    boolean passiveCache = Configuration.getBoolean(PropertyKey.USER_FILE_PASSIVE_CACHE_ENABLED);
    long channelTimeout = Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
    // Get relevant information from the stream.
    WorkerNetAddress dataSource = stream.getAddress();
    long blockId = stream.getId();
    if (cache && (mLastBlockIdCached != blockId)) {
      WorkerNetAddress worker;
      if (passiveCache && mContext.hasLocalWorker()) { // send request to local worker
        worker = mContext.getLocalWorker();
      } else { // send request to data source
        worker = dataSource;
      }
      try {
        // Construct the async cache request
        long blockLength = mOptions.getBlockInfo(blockId).getLength();
        Protocol.AsyncCacheRequest request =
            Protocol.AsyncCacheRequest.newBuilder().setBlockId(blockId).setLength(blockLength)
                .setOpenUfsBlockOptions(mOptions.getOpenUfsBlockOptions(blockId))
                .setSourceHost(dataSource.getHost()).setSourcePort(dataSource.getDataPort())
                .build();
        Channel channel = mContext.acquireNettyChannel(worker);
        try {
          NettyRPCContext rpcContext =
              NettyRPCContext.defaults().setChannel(channel).setTimeout(channelTimeout);
          NettyRPC.fireAndForget(rpcContext, new ProtoMessage(request));
          mLastBlockIdCached = blockId;
        } finally {
          mContext.releaseNettyChannel(worker, channel);
        }
      } catch (Exception e) {
        LOG.warn("Failed to complete async cache request for block {} at worker {}: {}", blockId,
            worker, e.getMessage());
      }
    }
  }

  private void handleRetryableException(BlockInStream stream, IOException e) {
    WorkerNetAddress workerAddress = stream.getAddress();
    LOG.warn("Failed to read block {} from worker {}, will retry: {}",
        stream.getId(), workerAddress, e.getMessage());
    try {
      stream.close();
    } catch (Exception ex) {
      // Do not throw doing a best effort close
      LOG.warn("Failed to close input stream for block {}: {}", stream.getId(), ex.getMessage());
    }

    mFailedWorkers.put(workerAddress, System.currentTimeMillis());
  }
}
