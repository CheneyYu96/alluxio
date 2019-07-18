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
public class TableReplPolicy implements ReplPolicy{
    private static final Logger LOG = LoggerFactory.getLogger(TableReplPolicy.class);

    private double budget;

    public TableReplPolicy() {
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

                    double allLoad = loads.stream().map(Pair::getFirst).reduce(0.0, Double::sum);
                    int replica = (int) Math.ceil(finalOptAlpha * allLoad) - 1;
                    List<OffLenPair> allCols = loads.stream().map(Pair::getSecond).collect(Collectors.toList());

                    LOG.info("File: {}. replicas: {}.", info.getFilePath().getPath(), replica);

                    return new MultiReplUnit(info.getFilePath(), allCols, replica);
                })
                .collect(Collectors.toList());
    }


    private double calcReplCost(double alpha, Map<AlluxioURI, List<Pair<Double, Double>>> allLoads){
        double cost = 0;
        for (List<Pair<Double, Double>> loadSize : allLoads.values()){
            double allLoad = loadSize.stream().map(Pair::getFirst).reduce(0.0, Double::sum);
            double allSize = loadSize.stream().map(Pair::getSecond).reduce(0.0, Double::sum);

            int replica = (int) Math.ceil(alpha * allLoad) - 1;
            cost = cost + replica * allSize;
        }

        return cost;
    }
}
