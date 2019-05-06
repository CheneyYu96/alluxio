package alluxio.master.repl.policy;

import alluxio.master.repl.meta.FileAccessInfo;
import fr.client.utils.MultiReplUnit;
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

    /**
     * This method allows bundling offsets form different tables to replicate
     * @param fileAccessInfos
     * @return
     */
    List<MultiReplUnit> calcMultiReplicas(List<FileAccessInfo> fileAccessInfos);
}
