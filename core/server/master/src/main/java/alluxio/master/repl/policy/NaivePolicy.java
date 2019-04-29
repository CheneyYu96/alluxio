package alluxio.master.repl.policy;

import alluxio.master.repl.FileAccessInfo;
import fr.client.utils.ReplUnit;

import java.util.List;

/**
 * Test replication by a naive policy
 */
public class NaivePolicy implements ReplPolicy {
    @Override
    public List<ReplUnit> calcReplicas(FileAccessInfo fileAccessInfo) {
        return null;
    }
}
