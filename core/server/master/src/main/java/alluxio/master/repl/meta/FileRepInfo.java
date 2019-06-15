package alluxio.master.repl.meta;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import fr.client.utils.OffLenPair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Maintain file replicas
 */
public class FileRepInfo {
    private AlluxioURI originalFilePath;

    private Map<AlluxioURI, Map<OffLenPair, OffLenPair>> mappedReplicas; /* key: origin offset; value: mapped offsets */
    private Map<OffLenPair, Map<AlluxioURI, OffLenPair>> mappedPairs; /* key: replicas; value: mapped offsets */

    public FileRepInfo(AlluxioURI originalFilePath) {
        this.originalFilePath = originalFilePath;
        this.mappedReplicas = new ConcurrentHashMap<>();
        this.mappedPairs = new ConcurrentHashMap<>();
    }

    public List<AlluxioURI> getReplicasURI(){
        return new ArrayList<>(mappedReplicas.keySet());
    }

    public List<Pair<AlluxioURI, OffLenPair>> getMappedReplicas(){
        return mappedReplicas
                .entrySet()
                .stream()
                .map( e -> {
                    List<Pair<AlluxioURI, OffLenPair>> ret = new ArrayList<>();

                    AlluxioURI replica = e.getKey();
                    return e.getValue().keySet().stream().map( o -> new Pair<>(replica, o)).collect(Collectors.toList());
                })
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public Map<AlluxioURI, OffLenPair> getMappedPairs(OffLenPair pair){
        return mappedPairs.getOrDefault(pair, new ConcurrentHashMap<>());
    }

    public void addReplicas(AlluxioURI replica, List<OffLenPair> originPairs, List<OffLenPair> newPairs){
        // update mapped replicas
        Map<OffLenPair, OffLenPair> mappedPair = mappedReplicas.getOrDefault(replica, new ConcurrentHashMap<>());
        IntStream.range(0, originPairs.size()).forEach(i -> mappedPair.put(originPairs.get(i), newPairs.get(i)));
        mappedReplicas.put(replica, mappedPair);

        // update mapped pairs
        IntStream.range(0, originPairs.size()).forEach(i -> {
            Map<AlluxioURI, OffLenPair> offsetMap = mappedPairs.getOrDefault(originPairs.get(i), new ConcurrentHashMap<>());
            offsetMap.put(replica, newPairs.get(i));
            mappedPairs.put(originPairs.get(i), offsetMap);
        });
    }

}
