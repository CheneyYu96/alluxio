package alluxio.client.file.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.wire.WorkerNetAddress;
import com.google.common.collect.Lists;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * @author minchen
 *
 * A policy that decides which worker by a timer.
 * -A sequence blocks will be assigned in the same worker.
 * -Blocks whose time gap between arrivals exceeds the
 *  mThreshold will be assigned to different workers.
 */
@NotThreadSafe
public class TimerPolicy implements FileWriteLocationPolicy, BlockLocationPolicy {
    private List<BlockWorkerInfo> mWorkerInfoList;
    private int mIndex;

    private long mThreshold;
    private long mPreviousTime;

    private boolean mInitialized = false;

    /**
     * Constructs a new {@link RoundRobinPolicy}.
     */
    public TimerPolicy() {

        Path path = FileSystems.getDefault().getPath("/home/ec2-user/alluxio/conf/threshold");
        try {
            String threshold = Files.readAllLines(path).get(0);
            mThreshold = Long.parseLong(threshold);

            System.out.println(threshold);
        } catch (IOException e) {
            e.printStackTrace();
        }

//        BufferedReader Buff = new BufferedReader(new FileReader("/home/ec2-user/alluxio/conf/threshold"));
//        try {
//            String threshold = Buff.readLine();
//            mThreshold = Long.parseLong(threshold);
//
//            System.out.println(threshold);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList,
                                                  long blockSizeBytes) {
        if (!mInitialized) {
            mPreviousTime = System.currentTimeMillis();
            mWorkerInfoList = Lists.newArrayList(workerInfoList);
            mIndex = 0;
            mInitialized = true;
        }

        long currentTime = System.currentTimeMillis();
        if ((currentTime - mPreviousTime) > mThreshold) {
            mIndex = (mIndex + 1) % mWorkerInfoList.size();
            mPreviousTime = currentTime;
        }

        return mWorkerInfoList.get(mIndex).getNetAddress();
    }

    @Override
    public WorkerNetAddress getWorker(GetWorkerOptions options) {
        return getWorkerForNextBlock(options.getBlockWorkerInfos(), options.getBlockSize());
    }
}
