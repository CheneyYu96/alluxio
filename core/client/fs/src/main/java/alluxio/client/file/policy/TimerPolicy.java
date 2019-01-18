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
 *
 * A policy that decides which worker by custom index
 * - read index from conf/threshold
 *
 */
@NotThreadSafe
public class TimerPolicy implements FileWriteLocationPolicy, BlockLocationPolicy {
    private List<BlockWorkerInfo> mWorkerInfoList;
    private int mIndex;

    private boolean mInitialized = false;

    /**
     * Constructs a new {@link TimerPolicy}.
     */
    public TimerPolicy() {

        Path path = FileSystems.getDefault().getPath("/home/ec2-user/alluxio/conf/threshold");
        try {
            String index = Files.readAllLines(path).get(0);
            mIndex = Integer.parseInt(index);

            System.out.println(index);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList,
                                                  long blockSizeBytes) {
        if (!mInitialized) {
            mWorkerInfoList = Lists.newArrayList(workerInfoList);
            mInitialized = true;
        }

        if (mIndex >= mWorkerInfoList.size()) {
            mIndex = 0;
        }

        return mWorkerInfoList.get(mIndex).getNetAddress();
    }

    @Override
    public WorkerNetAddress getWorker(GetWorkerOptions options) {
        return getWorkerForNextBlock(options.getBlockWorkerInfos(), options.getBlockSize());
    }
}
