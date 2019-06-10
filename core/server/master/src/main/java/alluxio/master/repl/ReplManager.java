package alluxio.master.repl;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    private String frDir;

    private boolean useParuqetInfo;
    private boolean repeatRepl;

    private boolean replGlobal;
    private boolean haveRepl;

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

        frDir = Configuration.get(PropertyKey.FR_REPL_DIR);

        LOG.info("Create replication manager. check_interval : {}. policy : {}", checkInterval, replPolicy.getClass().getName());
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

        LOG.debug("Receive request for file {}. offset {} length {}", requestFile.getPath(), offset, length);

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

        LOG.info("Record access for file {}. offset {} length {}", requestFile.getPath(), pair.offset, pair.length);

        Map<AlluxioURI, OffLenPair> mappedOffsets = new ConcurrentHashMap<>(ImmutableMap.of(requestFile, pair));

        if(accessRecords.containsKey(requestFile)){
            accessRecords.get(requestFile).incCount(pair);

            FileRepInfo repInfo = fileReplicas.get(requestFile);
            if (repInfo != null){
                repInfo.getMappedPairs(pair).forEach(mappedOffsets::put);
            }
        }
        else {
            accessRecords.put(requestFile, new FileAccessInfo(requestFile, pair));
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
        }
    }

    public void checkStats(){
        try {
            // Thread.interrupted() clears the interrupt status. Do not call interrupt again to clear it.
            while (!Thread.interrupted()) {
                TimeUnit.SECONDS.sleep(checkInterval);
                LOG.info("Checking stats for replication");

                // allow bundling offsets from different tables
                if(replGlobal){
                    if (repeatRepl){
                        // TODO: delete all replicas
                    }
                    else {
                        if (haveRepl){
                            continue;
                        }
                        else {
                            haveRepl = true;
                        }
                    }

                    List<MultiReplUnit> replUnits = replPolicy.calcMultiReplicas(new ArrayList<>(accessRecords.values()));

                    // TODO: treat as ReplUnit now. May allow more complicated ops.
                    for (MultiReplUnit unit : replUnits){
                        unit.toReplUnit().forEach((key, value) -> replicate(key, Collections.singletonList(value)));
                    }

                }
                else {
                    accessRecords.forEach((filePath, accessInfo) -> {

                        if (fileReplicas.containsKey(filePath)) {
                            if (repeatRepl) {
                                // TODO: delay deletion
                                // delete old replicas
                                FileRepInfo oldRepInfo = fileReplicas.remove(filePath);
                                frClient.deleteReplicas(oldRepInfo.getReplicasURI());
                                LOG.info("Delete replicas for file: {}.", filePath.getPath());
                            } else {
                                // just replica once
                                return;
                            }
                        }

                        List<ReplUnit> replUnits = replPolicy.calcReplicas(accessInfo);
                        replicate(filePath, replUnits);
                    });
                }

            }
        } catch (InterruptedException e) {
            // Allow thread to exit.
        } catch (Exception e) {
            LOG.error("Uncaught exception in checking stats", e);
        }
    }

    private void replicate(AlluxioURI filePath, List<ReplUnit> replUnits){
        if (replUnits != null && replUnits.size() > 0) {
//            LOG.info("Make replication decision for file : {} ", filePath.getPath());

            FileRepInfo repInfo = new FileRepInfo(filePath);

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
