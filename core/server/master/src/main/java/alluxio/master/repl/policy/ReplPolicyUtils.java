package alluxio.master.repl.policy;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.master.repl.meta.FileAccessInfo;
import fr.client.utils.OffLenPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        // ascending order
        List<Double> sortedLoads = allLoadSize
                .values()
                .stream()
                .flatMap(Collection::stream)
                .map(Pair::getFirst)
                .sorted((e1, e2) -> e1 > e2 ? 1 : -1)
                .collect(Collectors.toList());

        LOG.info("All loads: {}", sortedLoads);

        double lastAlpha = 0;
        double optAlpha = 0;

        double optCost = 0;
        // TODO binary search
        for (int i = 0; i < sortedLoads.size(); i++){

            double coldLoad = sortedLoads
                    .stream()
                    .limit(i + 1)
                    .reduce(0.0, Double::sum);

            lastAlpha = optAlpha;
            optAlpha = 1 / coldLoad;

            double cost = costCalculator.calcReplCost(optAlpha, allLoadSize);

            LOG.info("Calculating alpha: {}; cost: {}", optAlpha, cost);

            if (cost <= budget){
                optCost = cost;
                LOG.info("Suboptimal alpha: {}; cost: {}", optAlpha, optCost);
                break;
            }
        }

        // Tuning alpha
        int attemp = 0;
        double upperAlpha = lastAlpha;
        double lowerAlpha = optAlpha;

        while( Math.abs(optCost - budget) / budget > 0.05){

            attemp += 1;

            optAlpha = (upperAlpha + lowerAlpha) / 2;
            optCost = costCalculator.calcReplCost(optAlpha, allLoadSize);

            if (optCost > budget){
                upperAlpha = optAlpha;
            }
            else {
                lowerAlpha = optAlpha;
            }

            if (attemp > 1000){
                break;
            }
        }

        LOG.info("Attemps: {}. Optimal alpha: {}, cost; {}", attemp, optAlpha, optCost);

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

    interface CostCalculator{
        double calcReplCost(double alpha, Map<AlluxioURI, List<Pair<Double, Double>>> allLoads);
    }
}
