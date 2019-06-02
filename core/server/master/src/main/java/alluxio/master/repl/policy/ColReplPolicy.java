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
public class ColReplPolicy implements ReplPolicy{
    private static final Logger LOG = LoggerFactory.getLogger(ReplPolicy.class);

    private double budget;

    public ColReplPolicy() {
        budget = Configuration.getDouble(PropertyKey.FR_REPL_BUDGET);
    }

    @Override
    public List<ReplUnit> calcReplicas(FileAccessInfo fileAccessInfo) {
        return null;
    }

    @Override
    public List<MultiReplUnit> calcMultiReplicas(List<FileAccessInfo> fileAccessInfos) {
        long allSize = fileAccessInfos
                .stream()
                .map(o -> o.getOffsetCount().keySet())
                .flatMap(Collection::stream)
                .mapToLong( o -> o.length)
                .reduce(0, Long::sum);

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

        double optAlpha = 0;
        // TODO binary search
        for (int i = 0; i < sortedLoads.size(); i++){

            double coldLoad = sortedLoads
                    .stream()
                    .limit(i + 1)
                    .reduce(0.0, Double::sum);

            double cost = calcReplCost(1 / coldLoad, allLoadSize);
            optAlpha = 1 / coldLoad;

            LOG.info("Test alpha: {}; cost: {}", optAlpha, cost);

            if (cost <= budget){
                LOG.info("Optimal alpha: {}; cost: {}", optAlpha, cost);
                break;
            }
        }


        double finalOptAlpha = optAlpha;

        return fileAccessInfos
                .stream()
                .map(info -> {
                    List<Pair<Double, OffLenPair>> loads = calcLoad(allSize, info.getOffsetCount());
                    List<MultiReplUnit> replUnits = new ArrayList<>();

                    boolean isCold = true;
                    double coldLoad = 0;

                    for (Pair<Double, OffLenPair> p : loads){
                        double l = p.getFirst();

                        if (coldLoad + l > 1 / finalOptAlpha){
                            isCold = false;
                        }
                        else {
                            coldLoad = coldLoad + l;
                        }

                        if(!isCold){
                            replUnits.add(new MultiReplUnit(
                                    info.getFilePath(),
                                    Collections.singletonList(p.getSecond()),
                                    (int) Math.ceil(finalOptAlpha * l)));
                        }
                    }

                    return replUnits;
                })
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private List<Pair<Double, OffLenPair>> calcLoad(long allSize, Map<OffLenPair, Long> offsets){
        return offsets
                .entrySet()
                .stream()
                .map(e -> new Pair<>(e.getValue() * e.getKey().length * 1.0 /allSize, e.getKey()))
                .sorted((e1, e2) -> e1.getFirst() > e2.getFirst() ? 1 : -1)
                .collect(Collectors.toList());
    }

    private double calcReplCost(double alpha, Map<AlluxioURI, List<Pair<Double, Double>>> allLoads){
        double cost = 0;
        for (List<Pair<Double, Double>> loadSize : allLoads.values()){

            boolean isCold = true;
            double coldLoad = 0;

            for (Pair<Double, Double> p : loadSize){

                double l = p.getFirst();
                double s = p.getSecond();

                if (coldLoad + l > 1 / alpha){
                    isCold = false;
                }
                else {
                    coldLoad = coldLoad + l;
                }

                if(!isCold){
                    cost = cost + Math.ceil(alpha * l) * s;
                }

            }
        }

        return cost;
    }
}
