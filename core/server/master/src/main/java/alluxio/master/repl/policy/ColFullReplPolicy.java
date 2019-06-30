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
public class ColFullReplPolicy implements ReplPolicy{
    private static final Logger LOG = LoggerFactory.getLogger(ColFullReplPolicy.class);

    private double budget;

    public ColFullReplPolicy() {
        budget = Configuration.getDouble(PropertyKey.FR_REPL_BUDGET) + 1; // delete origin
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

                    for (Pair<Double, OffLenPair> p : loads){
                        double l = p.getFirst();

                        int replicas = Math.max(1, (int) Math.ceil(finalOptAlpha * l));
                        replUnits.add(new MultiReplUnit(
                                info.getFilePath(),
                                Collections.singletonList(p.getSecond()),
                                replicas));

                        LOG.info("File: {}. replicas: {}. off: {}",
                                info.getFilePath().getPath(),
                                replicas,
                                p.getSecond());
                    }

                    return replUnits;
                })
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private double calcReplCost(double alpha, Map<AlluxioURI, List<Pair<Double, Double>>> allLoads){
        double cost = 0;
        for (List<Pair<Double, Double>> loadSize : allLoads.values()){

            for (Pair<Double, Double> p : loadSize){

                double l = p.getFirst();
                double s = p.getSecond();

                int replica = Math.max(1, (int) Math.ceil(alpha * l));
                cost = cost + replica * s;

            }
        }

        return cost;
    }
}
