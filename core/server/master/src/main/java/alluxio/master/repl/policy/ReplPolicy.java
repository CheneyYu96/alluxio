package alluxio.master.repl.policy;

import alluxio.master.repl.FileAccessInfo;
import fr.client.utils.ReplUnit;

import java.util.List;

/**
 * interface for replication of raw data segments
 */
public interface ReplPolicy {

    /**
     * Calculate offsets and replicas given access info
     * @param fileAccessInfo
     * @return the offsets to replicate and the number of replicas
     */
    List<ReplUnit> calcReplicas(FileAccessInfo fileAccessInfo);
}
