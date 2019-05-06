package alluxio.master.repl.policy;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.collections.Pair;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.repl.meta.FileAccessInfo;
import fr.client.utils.MultiReplUnit;
import fr.client.utils.OffLenPair;
import fr.client.utils.ReplUnit;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class BundleHottestKPolicy implements ReplPolicy {
    private double weight;
    private int workNum;

    public BundleHottestKPolicy() {
        weight = Configuration.getDouble(PropertyKey.FR_REPL_WEIGHT);
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

        List<Pair<OffLenPair, Long>> sortedPops = fileAccessInfo
                .getOffsetCount()
                .entrySet()
                .stream()
                .sorted((e1, e2) -> e1.getValue() < e2.getValue()?1:-1)
                .map( o -> new Pair<>(o.getKey(), o.getValue()))
                .collect(Collectors.toList());

        System.out.println(sortedPops);

        long totalSize = sortedPops
                .stream()
                .mapToLong(o -> o.getFirst().length)
                .reduce((long) 0, Long::sum);

        List<Double> sortedSizes = sortedPops
                .stream()
                .map( o -> o.getSecond() * 1.0 / totalSize)
                .collect(Collectors.toList());


        List<Double> sortedLoads = sortedPops
                .stream()
                .mapToDouble( o -> o.getFirst().length * o.getSecond() * 1.0 / totalSize)
                .boxed()
                .collect(Collectors.toList());

        int bestK = -1;
        int bestR = -1;
        double bestObj = Double.POSITIVE_INFINITY;

        for(int k = 0; k < sortedPops.size(); k++){
            double hotLoad = sortedLoads
                    .stream()
                    .limit(k + 1)
                    .reduce( 0.0, Double::sum);


            double regret = sortedPops
                    .stream()
                    .skip(k + 1)
                    .mapToDouble( o -> 1 - o.getSecond() * 1.0 / fileAccessInfo.getQueryNum())
                    .reduce(1.0, (d1, d2) -> d1 * d2);

            hotLoad *= regret;

            double coldLoad = sortedLoads.stream().reduce(0.0, Double::sum) - hotLoad;

            double hotSize = sortedSizes
                    .stream()
                    .limit(k + 1)
                    .reduce(0.0, Double::sum);

            double doubleR = Math.sqrt(hotLoad * hotLoad / (workNum * weight * hotSize));

            System.out.println("k=" + k + "; Lh=" + hotLoad + "; workNum=" + workNum + "; Sh=" + hotSize + "; r=" + doubleR);

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

        System.out.println("Opt K = " + bestK + "; Opt R = " + bestR + "; Opt Obj = " + bestObj);

        List<OffLenPair> replPairs = sortedPops
                .stream()
                .limit(bestK + 1)
                .map(Pair::getFirst)
                .collect(Collectors.toList());

        return Collections.singletonList(new ReplUnit(replPairs, bestR));
    }

    @Override
    public List<MultiReplUnit> calcMultiReplicas(List<FileAccessInfo> fileAccessInfos) {
        return null;
    }
}
