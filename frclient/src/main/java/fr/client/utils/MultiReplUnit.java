package fr.client.utils;

import alluxio.AlluxioURI;

import java.util.List;
import java.util.Map;

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

    public Map<AlluxioURI, List<OffLenPair>> getOffLenPairsWithFile() {
        return offLenPairsWithFile;
    }

    public int getReplicas() {
        return replicas;
    }
}
