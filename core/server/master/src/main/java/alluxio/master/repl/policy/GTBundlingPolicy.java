package alluxio.master.repl.policy;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.collections.Pair;
import alluxio.master.repl.meta.FileAccessInfo;
import fr.client.utils.MultiReplUnit;
import fr.client.utils.OffLenPair;
import fr.client.utils.ReplUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public class GTBundlingPolicy implements ReplPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(GTBundlingPolicy.class);

    private double budget;

    public GTBundlingPolicy() {
        budget = Configuration.getDouble(PropertyKey.FR_REPL_BUDGET);
    }

    @Override
    public List<ReplUnit> calcReplicas(FileAccessInfo fileAccessInfo) {
        return null;
    }

    @Override
    public List<MultiReplUnit> calcMultiReplicas(List<FileAccessInfo> fileAccessInfos) {
        return ReplPolicyUtils.calcBundGlobReplicas(fileAccessInfos, this::calcPatternLoad, budget);
    }

    private List<Pair<Double, OffLenPair>> calcPatternLoad(long allSize, FileAccessInfo accessInfo){

        List<Pair<Double, OffLenPair>> resLoads = new ArrayList<>();

        List<Pair<Double, OffLenPair>> sortedLoads = ReplPolicyUtils.calcLoad(allSize, accessInfo.getOffsetCount());
        Map<Set<OffLenPair>, Long> patternCount = accessInfo.getPatternCount();

        Set<Set<OffLenPair>> patternsForOrigin = new HashSet<>();
        for (Pair<Double, OffLenPair> pair: sortedLoads){
            Set<Set<OffLenPair>> patterns = patternCount.keySet().stream()
                    .filter( pat -> pat.contains(pair.getSecond())).collect(Collectors.toSet());

            patternsForOrigin.addAll(patterns);

            double coldLoad = patternsForOrigin.stream().map( pat -> {
                long patSize = pat.stream().map( o -> o.length).reduce((long) 0, Long::sum);
                return patternCount.get(pat) * patSize * 1.0 / allSize;
            }).reduce(0.0, Double::sum);

            resLoads.add(new Pair<>(coldLoad, pair.getSecond()));
        }
        return resLoads;

    }


}
