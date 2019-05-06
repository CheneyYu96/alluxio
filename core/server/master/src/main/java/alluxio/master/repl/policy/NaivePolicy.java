package alluxio.master.repl.policy;

import alluxio.master.repl.meta.FileAccessInfo;
import fr.client.utils.MultiReplUnit;
import fr.client.utils.OffLenPair;
import fr.client.utils.ReplUnit;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test replication by a naive policy
 */
public class NaivePolicy implements ReplPolicy {
    @Override
    public List<ReplUnit> calcReplicas(FileAccessInfo fileAccessInfo) {
        List<OffLenPair> hotPairs = fileAccessInfo
                .getOffsetCount()
                .entrySet()
                .stream()
                .filter(e -> e.getValue() > 3)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        return Collections.singletonList(new ReplUnit(hotPairs, 1));
    }

    @Override
    public List<MultiReplUnit> calcMultiReplicas(List<FileAccessInfo> fileAccessInfos) {
        return null;
    }
}
