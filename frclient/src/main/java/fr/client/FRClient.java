package fr.client;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.policy.SpecificHostPolicy;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;
import fr.client.file.FRFileReader;
import fr.client.file.FRFileWriter;
import fr.client.utils.FRUtils;
import fr.client.utils.OffLenPair;
import fr.client.utils.ReplUnit;

import java.io.FileWriter;
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
    public final String FR_DIR;
    private FileSystem mFileSystem;
    private FileSystemContext mContext;
    private WriteType mWriteTpye;
    private final String replicaLocsFile;

    public FRClient() {
        mFileSystem = FileSystem.Factory.get();
        mContext = FileSystemContext.get();
        FR_DIR = Configuration.get(PropertyKey.FR_REPL_DIR);

        boolean isThrottle = Configuration.getBoolean(PropertyKey.FR_REPL_THROTTHLE);
        mWriteTpye = isThrottle ? WriteType.CACHE_THROUGH : WriteType.MUST_CACHE;

        replicaLocsFile = System.getProperty("user.dir") + "/replica-locs.txt";
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

        WorkerNetAddress blockLocation = FRUtils.getFileLocation(sourceFilePath, mFileSystem, mContext);
        availWorkers.remove(blockLocation);

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
                        CreateFileOptions.defaults().setWriteType(mWriteTpye).setLocationPolicy(new SpecificHostPolicy(name)))
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
        return getOneReplica(sourceFilePath, pairs, CreateFileOptions.defaults().setWriteType(mWriteTpye));
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

            String pairStr = pairs
                    .stream()
                    .map(o -> o.offset + ":" + o.length)
                    .reduce("", (f, s) -> f + "," + s);

            WorkerNetAddress address = FRUtils.getFileLocation(destFilePath, mFileSystem, mContext);

            FileWriter fw = new FileWriter(replicaLocsFile, true);
            fw.write(destFilePath.getPath() + "," +
                    address.getHost() + "," +
                    sourceFilePath.getPath() + pairStr +
                    "\n");
            fw.close();

            return destFilePath;

        } catch (IOException | AlluxioException e) {
            e.printStackTrace();
            return null;
        }
    }
}
