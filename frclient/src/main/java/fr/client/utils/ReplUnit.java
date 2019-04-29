package fr.client.utils;

import java.util.List;

/**
 * replication unit.
 *
 * containing list of offsets and the number of replicas
 */
public class ReplUnit {
    private List<OffLenPair> offLenPairs;
    private int replicas;

    public ReplUnit(List<OffLenPair> offLenPairs, int replicas) {
        this.offLenPairs = offLenPairs;
        this.replicas = replicas;
    }

    public List<OffLenPair> getOffLenPairs() {
        return offLenPairs;
    }

    public int getReplicas() {
        return replicas;
    }
}
