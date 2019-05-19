package fr.client;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.policy.SpecificHostPolicy;
import alluxio.exception.AlluxioException;
import alluxio.resource.CloseableResource;
import alluxio.util.CommonUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;
import fr.client.file.FRFileReader;
import fr.client.file.FRFileWriter;
import fr.client.utils.OffLenPair;
import fr.client.utils.ReplUnit;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 * The class is the endpoint to collect and replicate data segments, invoked by master
 */
public class FRClient {
    public static final String FR_DIR = "/fr_dir";
    private FileSystem mFileSystem;
    private FileSystemContext mContext;

    public FRClient() {
        mFileSystem = FileSystem.Factory.get();
        mContext = FileSystemContext.get();
    }

    public void deleteReplicas(List<AlluxioURI> replicaFilePath){
        try {
            for (AlluxioURI path : replicaFilePath){
                    mFileSystem.delete(path);
            }
        } catch (IOException | AlluxioException e) {
            e.printStackTrace();
        }
    }

    public List<AlluxioURI> copyFileOffset(AlluxioURI sourceFilePath, ReplUnit replUnit, List<WorkerNetAddress> availWorkers) {

        try {
            URIStatus orginStatus = mFileSystem.getStatus(sourceFilePath);

            // Assume part parquet file just occupy one block
            long blockId = orginStatus.getBlockIds().get(0);

            BlockInfo info;
            try (CloseableResource<BlockMasterClient> masterClientResource =
                         mContext.acquireBlockMasterClientResource()) {
                info = masterClientResource.get().getBlockInfo(blockId);
            }

            WorkerNetAddress blockLocation = info
                    .getLocations()
                    .get(0)
                    .getWorkerAddress();

            availWorkers.remove(blockLocation);
        } catch (IOException | AlluxioException e) {
            e.printStackTrace();
        }

        int replicaNum = Math.min(replUnit.getReplicas(), availWorkers.size());
        List<Integer> indexList = IntStream
                .range(0, availWorkers.size())
                .boxed()
                .collect(Collectors.toList());

        Collections.shuffle(indexList);

        List<String> hostNames = indexList
                .stream()
                .limit(replicaNum)
                .map(i -> availWorkers.get(i).getHost())
                .collect(Collectors.toList());

        return hostNames
                .stream()
                .map(name -> getOneReplica(
                        sourceFilePath,
                        replUnit.getOffLenPairs(),
                        CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE).setLocationPolicy(new SpecificHostPolicy(name)))
                )
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public List<AlluxioURI> copyFileOffset(AlluxioURI sourceFilePath, ReplUnit replUnit){
        return IntStream
                .range(0,replUnit.getReplicas())
                .mapToObj(i ->  getOneReplica(sourceFilePath, replUnit.getOffLenPairs()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private AlluxioURI getOneReplica(AlluxioURI sourceFilePath, List<OffLenPair> pairs){
        return getOneReplica(sourceFilePath, pairs, CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE));
    }

    private AlluxioURI getOneReplica(AlluxioURI sourceFilePath, List<OffLenPair> pairs, CreateFileOptions writeOptions) {
        String parentPath = sourceFilePath.getParent().getPath();

        String replicaParent = parentPath == null ? FR_DIR : FR_DIR + parentPath;
        String midName = pairs
                .stream()
                .map(o -> o.offset + ":" + o.length)
                .reduce("", (f, s) -> f + "-" + s);
        String replicaName = sourceFilePath.getName() + midName + "-" + CommonUtils.getCurrentMs();

        AlluxioURI destFilePath = new AlluxioURI(String.format("%s/%s", replicaParent, replicaName));

        FRFileReader reader = new FRFileReader(sourceFilePath, false);
        FRFileWriter writer = new FRFileWriter(destFilePath);
        writer.setWriteOption(writeOptions);

        try {
            int toRead = reader.readFile(pairs);
            writer.writeFile(reader.getBuf());
            return destFilePath;

        } catch (IOException | AlluxioException e) {
            e.printStackTrace();
            return null;
        }
    }
}
