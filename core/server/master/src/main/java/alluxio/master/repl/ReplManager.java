package alluxio.master.repl;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.repl.policy.ReplPolicy;
import alluxio.util.CommonUtils;
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

/**
 * Control selective replication for raw data segments.
 *
 * 1. check stats info
 * 2. make replication decision based on policy
 * 3. send repl request to client/worker ?
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
    private int checkInterval; /* in seconds */

    public ReplManager() {
        frClient = new FRClient();
        accessRecords = new ConcurrentHashMap<>();
        fileReplicas = new ConcurrentHashMap<>();
        replicaMap = new ConcurrentHashMap<>();

        replPolicy = CommonUtils.createNewClassInstance(Configuration.getClass(
                PropertyKey.FR_REPL_POLICY), new Class[] {}, new Object[] {});
        checkInterval = Configuration.getInt(PropertyKey.FR_CHECK_INTERVSL);

        LOG.info("Create replication manager. check_interval : {}. policy : {}", checkInterval, replPolicy.getClass().getName());
    }

    public OffLenPair getReplicaOffsets(AlluxioURI originFile, AlluxioURI replica, OffLenPair originPair){
        // TODO: check
        return fileReplicas.get(originFile).getMappedReplicas(replica).get(originPair);
    }

    public void recordAccess(AlluxioURI requestFile, long offset, long length){
        if(accessRecords.containsKey(requestFile)){
            accessRecords.get(requestFile).incCount(new OffLenPair(offset, length));
        }
        else {
            if(replicaMap.containsKey(requestFile)){
                accessRecords.get(replicaMap.get(requestFile)).incCount(new OffLenPair(offset, length));
            }
            else {
                accessRecords.put(requestFile, new FileAccessInfo(requestFile, new OffLenPair(offset, length)));
            }
        }
    }

    public void checkStats(){
        try {
            // Thread.interrupted() clears the interrupt status. Do not call interrupt again to clear it.
            while (!Thread.interrupted()) {
                TimeUnit.SECONDS.sleep(checkInterval);
                LOG.info("Checking stats for replication");

                accessRecords.forEach((filePath, accessInfo) -> {
                    List<ReplUnit> replUnits = replPolicy.calcReplicas(accessInfo);
                    if (replUnits != null && replUnits.size() > 0){
                        LOG.info("Make replication decision for file : {} ", filePath.getName());

                        FileRepInfo repInfo = fileReplicas.get(filePath);

                        replUnits.forEach(unit -> {
                            List<AlluxioURI> replicas = frClient.copyFileOffset(filePath, unit);

                            List<OffLenPair> originPairs = unit.getOffLenPairs();
                            long newOffset = 0;
                            List<OffLenPair> newPairs = new ArrayList<>();
                            for (OffLenPair originPair : originPairs) {
                                newPairs.add(new OffLenPair(newOffset, originPair.length));
                                newOffset += originPair.length;
                            }

                            replicas.forEach(r -> repInfo.addReplicas(r, originPairs, newPairs));
                            replicas.forEach(r -> replicaMap.put(r, filePath));
                        });
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
