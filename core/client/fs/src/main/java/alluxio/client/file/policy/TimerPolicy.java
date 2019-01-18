package alluxio.client.file.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.wire.WorkerNetAddress;
import com.google.common.collect.Lists;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;

/**
 * @author minchen
 *
 * A policy that decides which worker by a timer.
 * A sequence blocks will be assigned in the same worker.
 * Blocks whose time gap between arrivals exceeds the threshold will be assigned to different workers.
 */
@NotThreadSafe
public class TimerPolicy implements FileWriteLocationPolicy, BlockLocationPolicy {

    private List<BlockWorkerInfo> mWorkerInfoList;
    private int mIndex;

    // set threshold 3 seconds TODO
    private int threshold = 3000;
    private long previousTime;

    private boolean mInitialized = false;
//    /** This caches the {@link WorkerNetAddress} for the block IDs.*/
//    private final HashMap<Long, WorkerNetAddress> mBlockLocationCache = new HashMap<>();

    /**
     * Constructs a new {@link RoundRobinPolicy}.
     */
    public TimerPolicy() {
        this.previousTime = System.currentTimeMillis();
    }

    @Override
    public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList, long blockSizeBytes) {
        if (!mInitialized) {
            mWorkerInfoList = Lists.newArrayList(workerInfoList);
            mIndex = 0;
            mInitialized = true;
        }

        long currentTime = System.currentTimeMillis();
        if ((currentTime - previousTime) > threshold) {
            mIndex = (mIndex + 1) % mWorkerInfoList.size();
            previousTime = currentTime;
        }

        return mWorkerInfoList.get(mIndex).getNetAddress();
    }

    @Override
    public WorkerNetAddress getWorker(GetWorkerOptions options) {
        return getWorkerForNextBlock(options.getBlockWorkerInfos(), options.getBlockSize());
    }
}
