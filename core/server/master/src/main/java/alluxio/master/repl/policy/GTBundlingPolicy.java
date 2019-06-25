package alluxio.master.repl.policy;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.collections.Pair;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.repl.meta.FileAccessInfo;
import fr.client.utils.MultiReplUnit;
import fr.client.utils.OffLenPair;
import fr.client.utils.ReplUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class GTBundlingPolicy implements ReplPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(GTBundlingPolicy.class);

    private int workNum;

    private double budget;

    public GTBundlingPolicy() {
        budget = Configuration.getDouble(PropertyKey.FR_REPL_BUDGET);
    }

    private void updateWorkNum(){
        workNum = BlockMasterFactory.getBlockMaster().getWorkerCount();
    }

    @Override
    public List<ReplUnit> calcReplicas(FileAccessInfo fileAccessInfo) {
        return null;
    }

    @Override
    public List<MultiReplUnit> calcMultiReplicas(List<FileAccessInfo> fileAccessInfos) {
        updateWorkNum();

        long allSize = ReplPolicyUtils.calcAllSize(fileAccessInfos);

        double finalOptAlpha = ReplPolicyUtils.calcGlobalAlpha(fileAccessInfos, budget, this::calcReplCost);

        return fileAccessInfos
                .stream()
                .map(info -> {
                    List<Pair<Double, OffLenPair>> loads = ReplPolicyUtils.calcLoad(allSize, info.getOffsetCount());

                    int coldIndex = 0;
                    double coldLoad = 0;

                    for(int i = 0; i < loads.size(); i++){
                        double l = loads.get(i).getFirst();
                        if (coldLoad + l > 1 / finalOptAlpha){
                            coldIndex = i;
                            break;
                        }
                        else {
                            coldLoad = coldLoad + l;
                        }
                    }


                    List<OffLenPair> hotOffs = loads.stream().skip(coldIndex).map(Pair::getSecond).collect(Collectors.toList());
                    double hotL = loads.stream().skip(coldIndex).map(Pair::getFirst).reduce(0.0, Double::sum);

                    int replicas = (int) Math.ceil(finalOptAlpha * hotL);

                    LOG.info("File: {}. Bundle columns: {}. replicas: {}. offsets: {}",
                            info.getFilePath().getPath(),
                            loads.size() - coldIndex,
                            replicas,
                            hotOffs);

                    return new MultiReplUnit(info.getFilePath(), hotOffs, replicas);
                })
                .collect(Collectors.toList());
    }

    private double calcReplCost(double alpha, Map<AlluxioURI, List<Pair<Double, Double>>> allLoads){
        double cost = 0;
        for (List<Pair<Double, Double>> loadSize : allLoads.values()){
            int coldIndex = 0;
            double coldLoad = 0;

            for(int i = 0; i < loadSize.size(); i++){
                double l = loadSize.get(i).getFirst();
                if (coldLoad + l > 1 / alpha){
                    coldIndex = i;
                    break;
                }
                else {
                    coldLoad = coldLoad + l;
                }
            }

            double hotL = loadSize.stream().skip(coldIndex).map(Pair::getFirst).reduce(0.0, Double::sum);
            double hotS = loadSize.stream().skip(coldIndex).map(Pair::getSecond).reduce(0.0, Double::sum);

            cost = cost + Math.ceil(alpha * hotL) * hotS;
        }

        return cost;
    }
}
