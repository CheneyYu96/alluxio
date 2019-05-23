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
import fr.client.utils.OffLenPair;
import fr.client.utils.ReplUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

        frDir = Configuration.get(PropertyKey.FR_REPL_DIR);

        LOG.info("Create replication manager. check_interval : {}. policy : {}", checkInterval, replPolicy.getClass().getName());
    }

    public OffLenPair getReplicaOffsets(AlluxioURI originFile, AlluxioURI replica, OffLenPair originPair){
        // TODO: check
        return fileReplicas.get(originFile).getMappedReplicas(replica).get(originPair);
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
                OffLenPair pairForParquet = offsetInfoMap.get(requestFile).getPairByOffset(offset);
                if (pairForParquet == null) {
                    return ImmutableMap.of(requestFile, pair);
                } else {
                    pair = pairForParquet;
                }
            }
        }
        else {
            // skip small segments, i.e., less than 10 bytes, so as to ignore file meta data
            // TODO: small segment can also happen for some columns
            if (length < 10){
                return ImmutableMap.of(requestFile, pair);
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

    public void recordAccess(AlluxioURI filePath, List<Long> offset, List<Long> length){
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

                // TODO: allow bundling offsets from different tables
                accessRecords.forEach((filePath, accessInfo) -> {
                    List<ReplUnit> replUnits = replPolicy.calcReplicas(accessInfo);
                    if (replUnits != null && replUnits.size() > 0){
                        LOG.info("Make replication decision for file : {} ", filePath.getName());

                        // delete old replicas

                        if (fileReplicas.containsKey(filePath)) {
                            FileRepInfo oldRepInfo = fileReplicas.remove(filePath);
                            frClient.deleteReplicas(oldRepInfo.getReplicasURI());
                            LOG.info("Delete replicas for file: {}.", filePath.getPath());
                        }

                        FileRepInfo repInfo = new FileRepInfo(filePath);

                        replUnits.forEach(unit -> {
                            LOG.info("File : {}. Replication : {}", filePath.getName(), unit);

                            if(unit.getReplicas() > 0) {

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
                });

            }
        } catch (InterruptedException e) {
            // Allow thread to exit.
        } catch (Exception e) {
            LOG.error("Uncaught exception in checking stats", e);
        }
    }
}
