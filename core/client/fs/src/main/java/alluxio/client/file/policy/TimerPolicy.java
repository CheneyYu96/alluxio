package alluxio.client.file.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.wire.WorkerNetAddress;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 *
 * A policy that decides which worker by custom index
 * - read index from conf/threshold
 *
 */
@NotThreadSafe
public class TimerPolicy implements FileWriteLocationPolicy, BlockLocationPolicy {
    private String mWorkerName;

    /**
     * Constructs a new {@link TimerPolicy}.
     */
    public TimerPolicy() {

        Path path = FileSystems.getDefault().getPath("/home/ec2-user/alluxio/conf/threshold");
        try {
            mWorkerName = Files.readAllLines(path).get(0).trim();

            System.out.println(mWorkerName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList,
                                                  long blockSizeBytes) {
        for (BlockWorkerInfo info : workerInfoList) {
            if (info.getNetAddress().getHost().equals(mWorkerName)) {
                return info.getNetAddress();
            }
        }
        return null;
    }

    @Override
    public WorkerNetAddress getWorker(GetWorkerOptions options) {
        return getWorkerForNextBlock(options.getBlockWorkerInfos(), options.getBlockSize());
    }
}
