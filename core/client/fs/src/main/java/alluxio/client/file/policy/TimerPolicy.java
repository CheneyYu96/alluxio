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

    private boolean mInitialized = false;

    /**
     * Constructs a new {@link RoundRobinPolicy}.
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
