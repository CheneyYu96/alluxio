package alluxio.master.repl;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.repl.policy.ReplPolicy;
import alluxio.util.CommonUtils;
import fr.client.utils.ReplUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private ReplPolicy replPolicy;
    private Map<AlluxioURI, FileAccessInfo> accessRecords;
    private Map<AlluxioURI, FileRepInfo> fileReplicas;
    private int checkInterval; /* in seconds */

    public ReplManager() {
        accessRecords = new ConcurrentHashMap<>();
        fileReplicas = new ConcurrentHashMap<>();

        /* TODO: dynamic assign replication policy by property key */
        replPolicy = CommonUtils.createNewClassInstance(Configuration.getClass(
                PropertyKey.FR_REPL_POLICY), new Class[] {}, new Object[] {});

        checkInterval = Configuration.getInt(PropertyKey.FR_CHECK_INTERVSL);

        LOG.info("Create replication manager. check_interval : {}. policy : {}", checkInterval, replPolicy.getClass().getName());
    }

    private synchronized void updateReplicas(AlluxioURI originalFile){
        FileRepInfo repInfo = fileReplicas.get(originalFile);
    }

    public void checkStats(){
        try {
            // Thread.interrupted() clears the interrupt status. Do not call interrupt again to clear it.
            while (!Thread.interrupted()) {
                TimeUnit.SECONDS.sleep(checkInterval);
                LOG.info("Checking stats for replication");

                //TODO: sychronization
                accessRecords.forEach((filePath, accessInfo) -> {
                    List<ReplUnit> replUnits = replPolicy.calcReplicas(accessInfo);
                    if (replUnits != null && replUnits.size() > 0){
                        LOG.info("Make replication decision for file : {} ", filePath.getName());
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
