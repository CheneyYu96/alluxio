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

import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public class BundleHottestKPolicy implements ReplPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(BundleHottestKPolicy.class);

    private double weight;
    private int workNum;

    private double budget;

    public BundleHottestKPolicy() {
        weight = Configuration.getDouble(PropertyKey.FR_REPL_WEIGHT);
        budget = Configuration.getDouble(PropertyKey.FR_REPL_BUDGET);
    }

    private void updateWorkNum(){
        workNum = BlockMasterFactory.getBlockMaster().getWorkerCount();
    }

    private double calcObjective(double hotLoad, double coldLoad, double hotSize, int r){
        double hotSqu = hotLoad * hotLoad;
        double coldSqu = coldLoad * coldLoad;

        return (hotSqu / r + coldSqu) / workNum - (hotSqu + coldSqu) / (workNum * workNum) + weight * r * hotSize;
    }

    @Override
    public List<ReplUnit> calcReplicas(FileAccessInfo fileAccessInfo) {
        updateWorkNum();

        List<Pair<OffLenPair, Long>> allPops = fileAccessInfo
                .getOffsetCount()
                .entrySet()
                .stream()
                .map( o -> new Pair<>(o.getKey(), o.getValue()))
                .collect(Collectors.toList());

        long totalSize = allPops
                .stream()
                .mapToLong(o -> o.getFirst().length)
                .reduce((long) 0, Long::sum);

        List<Pair<OffLenPair, Double>> sortedLoads = allPops
                .stream()
                .map( o -> new Pair<>(o.getFirst(), o.getFirst().length * o.getSecond() * 1.0 / totalSize))
                .sorted((e1, e2) -> e1.getSecond() < e2.getSecond()?1:-1)
                .collect(Collectors.toList());

        LOG.info("file: {}. sorted loads: {}", fileAccessInfo.getFilePath().getPath(), sortedLoads);


        List<Pair<OffLenPair, Long>> sortedPops = sortedLoads
                .stream()
                .map( o -> new Pair<>(o.getFirst(), fileAccessInfo.getOffsetCount().get(o.getFirst())))
                .collect(Collectors.toList());

        LOG.info("query_num: {}. sorted pops: {}", fileAccessInfo.getQueryNum(), sortedPops);

        List<Double> sortedSizes = sortedPops
                .stream()
                .map( o -> o.getFirst().length * 1.0 / totalSize)
                .collect(Collectors.toList());

        double allLoad = sortedLoads.stream().mapToDouble(Pair::getSecond).reduce(0.0, Double::sum);

        int bestK = -1;
        int bestR = -1;
        // initial obj as no replicas
//        double bestObj = calcObjective(0, allLoad, 0, 0);
        double bestObj = Double.POSITIVE_INFINITY;

        for(int k = 0; k < sortedPops.size(); k++){

            double hotLoad = sortedLoads
                    .stream()
                    .limit(k + 1)
                    .mapToDouble(Pair::getSecond)
                    .reduce( 0.0, Double::sum);

            double regret = sortedPops
                    .stream()
                    .skip(k + 1)
                    .mapToDouble( o -> 1 - o.getSecond() * 1.0 / fileAccessInfo.getQueryNum())
                    .reduce(1.0, (d1, d2) -> d1 * d2);

            hotLoad = hotLoad * regret;

            double coldLoad = allLoad - hotLoad;

            double hotSize = sortedSizes
                    .stream()
                    .limit(k + 1)
                    .reduce(0.0, Double::sum);

            double doubleR = Math.sqrt(hotLoad * hotLoad / (workNum * weight * hotSize));

//            System.out.println("k=" + k + "; Lh=" + hotLoad + "; workNum=" + workNum + "; Sh=" + hotSize + "; r=" + doubleR);

            if (doubleR > workNum){
                double localObj = calcObjective(hotLoad, coldLoad, hotSize, workNum);
                if (localObj < bestObj){
                    bestObj = localObj;
                    bestK = k;
                    bestR = workNum;
                }
            }
            else {
                int floorR = (int) Math.floor(doubleR);
                int ceilR = (int) Math.ceil(doubleR);

                double floorObj = calcObjective(hotLoad, coldLoad, hotSize, floorR);
                double ceilObj = calcObjective(hotLoad, coldLoad, hotSize, ceilR);

                double localObj = Math.min(floorObj, ceilObj);
                if (localObj < bestObj){
                    bestObj = localObj;
                    bestK = k;
                    bestR = floorObj < ceilObj ? floorR : ceilR;
                }
            }

        }

        LOG.info("Opt K = {}; Opt R = {}; Opt Obj = {}", bestK, bestR, bestObj);

        List<OffLenPair> replPairs = sortedPops
                .stream()
                .limit(bestK + 1)
                .map(Pair::getFirst)
                .collect(Collectors.toList());

        return Collections.singletonList(new ReplUnit(replPairs, bestR));
    }

    @Override
    public List<MultiReplUnit> calcMultiReplicas(List<FileAccessInfo> fileAccessInfos) {
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
