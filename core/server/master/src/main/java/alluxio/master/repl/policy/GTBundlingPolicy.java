package alluxio.master.repl.policy;

import alluxio.AlluxioURI;
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

        long allSize = ReplPolicyUtils.calcAllSize(fileAccessInfos);

        Map<AlluxioURI, List<Pair<Double, Double>>> allLoadSize = fileAccessInfos
                .stream()
                .collect(Collectors.toMap(
                        FileAccessInfo::getFilePath,
                        info -> calcPatternLoad(allSize, info)
                            .stream()
                            .map(p -> new Pair<>(p.getFirst(), p.getSecond().length * 1.0 / allSize))
                            .collect(Collectors.toList())));

        double finalOptAlpha = ReplPolicyUtils.calcGlobalAlpha(allLoadSize, budget, this::calcReplCost);

        return fileAccessInfos
                .stream()
                .map(info -> {
                    List<Pair<Double, OffLenPair>> loads = calcPatternLoad(allSize, info);

                    int coldIndex = 0;

                    for(int i = 0; i < loads.size(); i++){
                        double coldLoad = 0;
                        if (coldLoad > 1 / finalOptAlpha){
                            coldIndex = i;
                            break;
                        }
                    }

                    List<OffLenPair> hotOffs = loads.stream().skip(coldIndex).map(Pair::getSecond).collect(Collectors.toList());
                    double hotL = loads.get(coldIndex).getFirst();

                    int replicas = (int) Math.ceil(finalOptAlpha * hotL);

                    LOG.info("File: {}. Bundle columns: {}. replicas: {}. offsets: {}",
                            info.getFilePath().getPath(),
                            hotOffs.size(),
                            replicas,
                            hotOffs);

                    return new MultiReplUnit(info.getFilePath(), hotOffs, replicas);
                })
                .collect(Collectors.toList());
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

    private double calcReplCost(double alpha, Map<AlluxioURI, List<Pair<Double, Double>>> allLoads){
        double cost = 0;
        for (List<Pair<Double, Double>> loadSize : allLoads.values()){
            int coldIndex = 0;

            for(int i = 0; i < loadSize.size(); i++){
                double coldLoad = loadSize.get(i).getFirst();
                if (coldLoad > 1 / alpha){
                    coldIndex = i;
                    break;
                }
            }

            double hotL = loadSize.get(coldIndex).getFirst();
            double hotS = loadSize.stream().skip(coldIndex).map(Pair::getSecond).reduce(0.0, Double::sum);

            cost = cost + Math.ceil(alpha * hotL) * hotS;
        }

        return cost;
    }
}
