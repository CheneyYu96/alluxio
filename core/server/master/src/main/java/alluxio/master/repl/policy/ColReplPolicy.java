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
    private static final Logger LOG = LoggerFactory.getLogger(ColReplPolicy.class);

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
        long allSize = ReplPolicyUtils.calcAllSize(fileAccessInfos);

        double finalOptAlpha = ReplPolicyUtils.calcGlobalAlpha(fileAccessInfos, budget, this::calcReplCost);

        return fileAccessInfos
                .stream()
                .map(info -> {
                    List<Pair<Double, OffLenPair>> loads = ReplPolicyUtils.calcLoad(allSize, info.getOffsetCount());
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
                            int replicas = (int) Math.ceil(finalOptAlpha * l);
                            replUnits.add(new MultiReplUnit(
                                    info.getFilePath(),
                                    Collections.singletonList(p.getSecond()),
                                    replicas));

                            LOG.info("File: {}. replicas: {}. off: {}",
                                    info.getFilePath().getPath(),
                                    replicas,
                                    p.getSecond());
                        }
                    }

                    return replUnits;
                })
                .flatMap(Collection::stream)
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
