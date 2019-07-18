package alluxio.master.repl.policy;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.master.repl.meta.FileAccessInfo;
import alluxio.util.CommonUtils;
import fr.client.utils.MultiReplUnit;
import fr.client.utils.OffLenPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class ReplPolicyUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ReplPolicyUtils.class);

    public static double calcGlobalAlpha(List<FileAccessInfo> fileAccessInfos, double budget, CostCalculator costCalculator) {

        long allSize = calcAllSize(fileAccessInfos);

        Map<AlluxioURI, List<Pair<Double, Double>>> allLoadSize = fileAccessInfos
                .stream()
                .collect(Collectors.toMap(
                        FileAccessInfo::getFilePath,
                        info -> calcLoad(allSize, info.getOffsetCount())
                                .stream()
                                .map(p -> new Pair<>(p.getFirst(), p.getSecond().length * 1.0 / allSize))
                                .collect(Collectors.toList())
                ));
        return calcGlobalAlpha(allLoadSize, budget, costCalculator);

    }

    public static double calcGlobalAlpha(Map<AlluxioURI, List<Pair<Double, Double>>> allLoadSize, double budget, CostCalculator costCalculator) {
        long startMs = CommonUtils.getCurrentMs();

        // ascending order
        List<Double> sortedLoads = allLoadSize
                .values()
                .stream()
                .flatMap(Collection::stream)
                .map(Pair::getFirst)
                .sorted()
                .collect(Collectors.toList());

        LOG.info("All loads: {}", sortedLoads);

        double lastAlpha = 0;
        double optAlpha = 0;

        double optCost = 0;
        // TODO binary search

//        for (int i = 0; i < sortedLoads.size(); i++){
//
//            double coldLoad = sortedLoads
//                    .stream()
//                    .limit(i + 1)
//                    .reduce(0.0, Double::sum);
//
//            lastAlpha = optAlpha;
//            optAlpha = 1 / coldLoad;
//
//            double cost = costCalculator.calcReplCost(optAlpha, allLoadSize);
//
//            LOG.info("Calculating alpha: {}; cost: {}", optAlpha, cost);
//
//            if (cost <= budget){
//                optCost = cost;
//                LOG.info("Suboptimal alpha: {}; cost: {}", optAlpha, optCost);
//                break;
//            }
//        }

        // Tuning alpha
        int attemp = 0;
//        double upperAlpha = lastAlpha;
//        double lowerAlpha = optAlpha;
        double upperAlpha = 1 / sortedLoads.stream().filter(l -> l > Double.MIN_VALUE).findFirst().get();
        double lowerAlpha = 1 / sortedLoads.stream().reduce(0.0, Double::sum);

        while( Math.abs(optCost - budget) / budget > 0.01){

            attemp += 1;

            optAlpha = (upperAlpha + lowerAlpha) / 2;
            optCost = costCalculator.calcReplCost(optAlpha, allLoadSize);

            if (optCost > budget){
                upperAlpha = optAlpha;
            }
            else {
                lowerAlpha = optAlpha;
            }

            if (attemp % 100 == 0){
                LOG.info("At attemps: {}. alpha: {}, cost; {}", attemp, optAlpha, optCost);
            }
            if (attemp > 3000){
                break;
            }

        }

        long endMs = CommonUtils.getCurrentMs();

        LOG.info("Attemps: {}. Optimal alpha: {}, cost; {}; elapsed: {}", attemp, optAlpha, optCost, endMs - startMs);

        return optAlpha;
    }


    public static List<Pair<Double, OffLenPair>> calcLoad(long allSize, Map<OffLenPair, Long> offsets){
        return offsets
                .entrySet()
                .stream()
                .map(e -> new Pair<>(e.getValue() * e.getKey().length * 1.0 /allSize, e.getKey()))
                .sorted((e1, e2) -> e1.getFirst() > e2.getFirst() ? 1 : -1)
                .collect(Collectors.toList());
    }

    public static long calcAllSize(List<FileAccessInfo> fileAccessInfos){
        return fileAccessInfos
                .stream()
                .map(o -> o.getOffsetCount().keySet())
                .flatMap(Collection::stream)
                .mapToLong( o -> o.length)
                .reduce(0, Long::sum);
    }

    public static double calcBundReplCost(double alpha, Map<AlluxioURI, List<Pair<Double, Double>>> allLoads){
        double cost = 0;
        for (List<Pair<Double, Double>> loadSize : allLoads.values()){
            int coldIndex = -1;

            for(int i = 0; i < loadSize.size(); i++){
                double coldLoad = loadSize.get(i).getFirst();
                if (coldLoad > 1 / alpha){
                    coldIndex = Math.max(i - 1, 0);
                    break;
                }
            }

            if (coldIndex >= 0) {
                double allL = loadSize.get(loadSize.size() - 1).getFirst();
                double hotL = allL - loadSize.get(coldIndex).getFirst();
                double hotS = loadSize.stream().skip(coldIndex).map(Pair::getSecond).reduce(0.0, Double::sum);
                cost = cost + (int) Math.ceil(alpha * hotL) * hotS;
            }
        }

        return cost;
    }

    public static List<MultiReplUnit> calcBundGlobReplicas(List<FileAccessInfo> fileAccessInfos, PatternCalculator patternCalculator, double budget){
        long allSize = calcAllSize(fileAccessInfos);

        Map<AlluxioURI, List<Pair<Double, Double>>> allLoadSize = fileAccessInfos
                .stream()
                .collect(Collectors.toMap(
                        FileAccessInfo::getFilePath,
                        info -> patternCalculator.calcPatternLoad(allSize, info)
                                .stream()
                                .map(p -> new Pair<>(p.getFirst(), p.getSecond().length * 1.0 / allSize))
                                .collect(Collectors.toList())));

        double finalOptAlpha = calcGlobalAlpha(allLoadSize, budget, ReplPolicyUtils::calcBundReplCost);

        return fileAccessInfos
                .stream()
                .map(info -> {
                    List<Pair<Double, OffLenPair>> loads = patternCalculator.calcPatternLoad(allSize, info);

                    int coldIndex = -1;

                    for(int i = 0; i < loads.size(); i++){
                        double coldLoad = loads.get(i).getFirst();
                        if (coldLoad > 1 / finalOptAlpha){
                            coldIndex = i - 1;
                            break;
                        }
                    }

                    double hotL = 0;
                    List<OffLenPair> hotOffs = new ArrayList<>();

                    if (coldIndex >= 0) {
                        double allL = loads.get(loads.size() - 1).getFirst();
                        hotL = allL - loads.get(coldIndex).getFirst();
                        hotOffs = loads.stream().skip(coldIndex).map(Pair::getSecond).collect(Collectors.toList());
                    }

                    int replicas = (int) Math.ceil(finalOptAlpha * hotL);

                    String loadStr = loads.stream().map(p -> p.getSecond().offset + "," + p.getSecond().length + "," + p.getFirst() + "|").reduce("", String::concat);

                    LOG.info("Log all loads. load: {}. path: {}", loadStr, info.getFilePath().getPath());

                    LOG.info("File: {}. all columns: {}. cold index: {}. bundle columns: {}. replicas: {}.",
                            info.getFilePath().getPath(),
                            loads.size(),
                            coldIndex,
                            hotOffs.size(),
                            replicas);

                    return new MultiReplUnit(info.getFilePath(), hotOffs, replicas);
                })
                .collect(Collectors.toList());
    }

    interface CostCalculator{
        double calcReplCost(double alpha, Map<AlluxioURI, List<Pair<Double, Double>>> allLoads);
    }

    interface PatternCalculator{
        List<Pair<Double, OffLenPair>> calcPatternLoad(long allSize, FileAccessInfo accessInfo);
    }

}
