package fr.client.utils;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * replication unit that contains multiple files.
 *
 * containing list of offsets and the number of replicas
 */
public class MultiReplUnit {
    private Map<AlluxioURI, List<OffLenPair>> offLenPairsWithFile;
    private int replicas;

    public MultiReplUnit(Map<AlluxioURI, List<OffLenPair>> offLenPairsWithFile, int replicas) {
        this.offLenPairsWithFile = offLenPairsWithFile;
        this.replicas = replicas;
    }

    public MultiReplUnit(AlluxioURI file, List<OffLenPair> offsets, int replicas) {
        this(ImmutableMap.of(file, offsets), replicas);
    }

    public Map<AlluxioURI, List<OffLenPair>> getOffLenPairsWithFile() {
        return offLenPairsWithFile;
    }

    public int getReplicas() {
        return replicas;
    }

    public Map<AlluxioURI, ReplUnit> toReplUnit(){
        return offLenPairsWithFile
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new ReplUnit(e.getValue(), replicas)));
    }

    public List<Pair<AlluxioURI, ReplUnit>> toReplUnitPairs(){
        return offLenPairsWithFile
                .entrySet()
                .stream()
                .map( o -> new Pair<>(o.getKey(), new ReplUnit(o.getValue(), replicas)))
                .collect(Collectors.toList());
    }

}
