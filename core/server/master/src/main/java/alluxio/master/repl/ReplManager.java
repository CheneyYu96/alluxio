package alluxio.master.repl;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.collections.Pair;
import alluxio.exception.status.UnavailableException;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.repl.meta.FileAccessInfo;
import alluxio.master.repl.meta.FileOffsetInfo;
import alluxio.master.repl.meta.FileRepInfo;
import alluxio.master.repl.policy.ReplPolicy;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import com.google.common.collect.ImmutableMap;
import fr.client.FRClient;
import fr.client.utils.MultiReplUnit;
import fr.client.utils.OffLenPair;
import fr.client.utils.ReplUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Control selective replication for raw data segments.
 *
 * 1. check stats info
 * 2. make replication decision based on policy
 * 3. send repl request to client
 * 4. update file/block metadata
 *
 */
public class ReplManager {
    private static final Logger LOG = LoggerFactory.getLogger(ReplManager.class);

    private FRClient frClient;
    private ReplPolicy replPolicy;
    private Map<AlluxioURI, FileAccessInfo> accessRecords;
    private Map<AlluxioURI, FileRepInfo> fileReplicas;
    private Map<AlluxioURI, AlluxioURI> replicaMap;
    private Map<AlluxioURI, FileOffsetInfo> offsetInfoMap;
    private int checkInterval; /* in seconds */

    private final String frDir;

    private boolean useParuqetInfo;
    private boolean repeatRepl;

    private boolean replGlobal;
    private boolean haveRepl;
    private boolean deleteOrigin;

    /* init empty access info and record ground truth*/
    private boolean useAccessInfo;
    private final String gtFilePath;

    public ReplManager() {
        frClient = new FRClient();
        accessRecords = new ConcurrentHashMap<>();
        fileReplicas = new ConcurrentHashMap<>();
        replicaMap = new ConcurrentHashMap<>();
        offsetInfoMap = new ConcurrentHashMap<>();

        replPolicy = CommonUtils.createNewClassInstance(Configuration.getClass(
                PropertyKey.FR_REPL_POLICY), new Class[] {}, new Object[] {});
        checkInterval = Configuration.getInt(PropertyKey.FR_REPL_INTERVAL);
        useParuqetInfo = Configuration.getBoolean(PropertyKey.FR_PARQUET_INFO);
        repeatRepl = Configuration.getBoolean(PropertyKey.FR_REPL_REPEAT);

        replGlobal = Configuration.getBoolean(PropertyKey.FR_REPL_GLOBAL);
        haveRepl = false;

        useAccessInfo = Configuration.getBoolean(PropertyKey.FR_REPL_BUDGET_ACCESS);
        deleteOrigin = Configuration.getBoolean(PropertyKey.FR_REPL_DEL_ORIGIN);

        frDir = Configuration.get(PropertyKey.FR_REPL_DIR);
        gtFilePath = "/home/ec2-user/alluxio/pattern-gt.txt";

        LOG.info("Create replication manager. check_interval : {}. policy : {}", checkInterval, replPolicy.getClass().getName());
    }

    public List<Pair<AlluxioURI, OffLenPair>> getReplicaInfo(AlluxioURI originFile){
        if (fileReplicas.containsKey(originFile)){
            return fileReplicas.get(originFile).getMappedReplicas();
        }
        else {
            return new ArrayList<>();
        }
    }

    public OffLenPair recordOffset(AlluxioURI requestFile, long offset, long length){
        LOG.debug("Receive offset check for file {}. offset {} length {}", requestFile.getPath(), offset, length);

        FileOffsetInfo offsetInfo = offsetInfoMap.get(requestFile);

        if(offsetInfo == null){
            return new OffLenPair(0, 0);
        }
        else {
            List<OffLenPair> pairs = offsetInfoMap.get(requestFile).getPairsByOffLen(offset, length);
            if(pairs.size() == 0){
                return new OffLenPair(0, 0);
            }
        }

        return new OffLenPair(1, 0);
    }

    public Map<AlluxioURI, OffLenPair> recordAccess(AlluxioURI requestFile, long offset, long length){


        OffLenPair pair = new OffLenPair(offset, length);

        // ignore replicas, usually comes from web UI
        if (requestFile.getPath().startsWith(frDir)){
            return ImmutableMap.of(requestFile, pair);
        }

        if (useParuqetInfo){
            FileOffsetInfo offsetInfo = offsetInfoMap.get(requestFile);
            if(offsetInfo == null){
                return ImmutableMap.of(requestFile, pair);
            }
            else {
                List<OffLenPair> pairs = offsetInfoMap.get(requestFile).getPairsByOffLen(offset, length);
                // TODO: when exist multiple pairs
                if(pairs.size() == 0){
                    return ImmutableMap.of(requestFile, pair);
                }
//                else if(pairs.size() == 1){
//                    pair = pairs.get(0);
//                }
            }
        }


        Map<AlluxioURI, OffLenPair> mappedOffsets = new ConcurrentHashMap<>();
        if (!deleteOrigin || !haveRepl){
            mappedOffsets.put(requestFile, pair);
        }

        FileRepInfo repInfo = fileReplicas.get(requestFile);
        if (repInfo != null){
            repInfo.getMappedPairs(pair).forEach(mappedOffsets::put);
        }

        // update access counts
        if (!useAccessInfo) {

            LOG.debug("Record access for file {}. offset {} length {}", requestFile.getPath(), pair.offset, pair.length);
            if (accessRecords.containsKey(requestFile)) {
                accessRecords.get(requestFile).incCount(pair);
            } else {
                accessRecords.put(requestFile, new FileAccessInfo(requestFile, pair));
            }
        }

        return mappedOffsets;
    }

    public void recordParInfo(AlluxioURI filePath, List<Long> offset, List<Long> length){
        if (!offsetInfoMap.containsKey(filePath)) {
            if (offset.size() == length.size()) {
                FileOffsetInfo fileOffsetInfo = new FileOffsetInfo(filePath,offset,length);
                LOG.info("Receive parquet info : {}", fileOffsetInfo.toString());

                offsetInfoMap.put(filePath, fileOffsetInfo);
            } else {
                LOG.error("File {}'s offset and length does not match", filePath);
            }

//            if(useAccessInfo){
                List<OffLenPair> allPairs = IntStream.range(0, offset.size())
                        .mapToObj(i -> new OffLenPair(offset.get(i), length.get(i)))
                        .collect(Collectors.toList());
                accessRecords.put(filePath, new FileAccessInfo(filePath, allPairs));
//            }
        }
    }

    public void checkStats(){
        try {
            // Thread.interrupted() clears the interrupt status. Do not call interrupt again to clear it.
            while (!Thread.interrupted()) {
                TimeUnit.SECONDS.sleep(checkInterval);
                LOG.info("Checking stats for replication");

                if (repeatRepl){
                    // TODO: delete all replicas
                }
                else{
                    if (haveRepl){
                        continue;
                    }
                    else {
                        haveRepl = true;
                    }

                    if(useAccessInfo){
                        readGTAccessPattern();
                    }

                    // allow bundling offsets from different tables
                    if(replGlobal){
                        List<MultiReplUnit> replUnits = replPolicy.calcMultiReplicas(new ArrayList<>(accessRecords.values()));

                        // TODO: treat as ReplUnit now. May allow more complicated ops.
//                        for (MultiReplUnit unit : replUnits){
//                            unit.toReplUnit().forEach((key, value) -> replicate(key, Collections.singletonList(value)));
//                        }

                        ExecutorService executorService = Executors.newCachedThreadPool();

                        List<Pair<AlluxioURI, ReplUnit>> singleReplUnits = replUnits
                                .stream()
                                .map(MultiReplUnit::toReplUnitPairs)
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList());

                        long startMs = CommonUtils.getCurrentMs();

                        for (Pair<AlluxioURI, ReplUnit> pair : singleReplUnits){
                            executorService.execute(() -> replicate(pair.getFirst(), Collections.singletonList(pair.getSecond())));
                        }

                        executorService.shutdown();
                        try {
                            executorService.awaitTermination(1, TimeUnit.HOURS);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        long endMs = CommonUtils.getCurrentMs();
                        LOG.info("Finish replication. elapsed: {}", endMs - startMs);

                    }
                    else {
                        accessRecords.forEach((filePath, accessInfo) -> {

                            List<ReplUnit> replUnits = replPolicy.calcReplicas(accessInfo);
                            replicate(filePath, replUnits);
                        });
                    }

//                    if(deleteOrigin){
//                        frClient.deleteReplicas(new ArrayList<>(accessRecords.keySet()));
//                    }

                }

            }
        } catch (InterruptedException e) {
            // Allow thread to exit.
        } catch (Exception e) {
            LOG.error("Uncaught exception in checking stats", e);
        }
    }

    private void readGTAccessPattern() throws IOException {
        FileInputStream is = new FileInputStream(gtFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] accessPattern = line.trim().split(",");
            String path = accessPattern[0];
            FileAccessInfo fileAccessInfo = accessRecords.get(new AlluxioURI(path));

            if (fileAccessInfo != null){
                List<Long> offsInPattern = Arrays.stream(accessPattern).skip(1).map(Long::parseLong).collect(Collectors.toList());
                fileAccessInfo.addPattern(offsInPattern);
            }
            else {
                LOG.warn("Find null file info when adding pattern. path: {}", path);
            }
        }
        is.close();
    }


    private void replicate(AlluxioURI filePath, List<ReplUnit> replUnits){
        if (replUnits != null && replUnits.size() > 0) {
//            LOG.info("Make replication decision for file : {} ", filePath.getPath());

            FileRepInfo repInfo = fileReplicas.getOrDefault(filePath, new FileRepInfo(filePath));

            replUnits.forEach(unit -> {
                LOG.info("File : {}. Replication : {}", filePath.getPath(), unit);

                if (unit.getReplicas() > 0) {

                    List<WorkerNetAddress> availWorkerAddress = null;

                    try {
                        List<WorkerInfo> allWorkers = BlockMasterFactory
                                .getBlockMaster()
                                .getWorkerInfoList();

                        availWorkerAddress = allWorkers
                                .stream()
                                .map(WorkerInfo::getAddress)
                                .collect(Collectors.toList());

                    } catch (UnavailableException e) {
                        e.printStackTrace();
                    }

                    List<AlluxioURI> replicas = availWorkerAddress != null ?
                            frClient.copyFileOffset(filePath, unit, availWorkerAddress) :
                            frClient.copyFileOffset(filePath, unit);

                    List<OffLenPair> originPairs = unit.getOffLenPairs();
                    long newOffset = 0;
                    List<OffLenPair> newPairs = new ArrayList<>();
                    for (OffLenPair originPair : originPairs) {
                        newPairs.add(new OffLenPair(newOffset, originPair.length));
                        newOffset += originPair.length;
                    }

                    replicas.forEach(r -> repInfo.addReplicas(r, originPairs, newPairs));
                    replicas.forEach(r -> replicaMap.put(r, filePath));
                }
            });
            fileReplicas.put(filePath, repInfo);
        }
    }
}
