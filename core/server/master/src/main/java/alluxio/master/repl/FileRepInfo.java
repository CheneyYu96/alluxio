package alluxio.master.repl;

import alluxio.AlluxioURI;
import fr.client.utils.OffLenPair;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

/**
 * Maintain file replicas
 */
public class FileRepInfo {
    private AlluxioURI originalFilePath;

    private Map<AlluxioURI, Map<OffLenPair, OffLenPair>> mappedReplicas;

    public FileRepInfo(AlluxioURI originalFilePath) {
        this.originalFilePath = originalFilePath;
        this.mappedReplicas = new ConcurrentHashMap<>();
    }

    public Map<OffLenPair, OffLenPair> getMappedReplicas(AlluxioURI replica){
        return mappedReplicas.getOrDefault(replica, new ConcurrentHashMap<>());
    }

    public void addReplicas(AlluxioURI replica, List<OffLenPair> originPairs, List<OffLenPair> newPairs){
        Map<OffLenPair, OffLenPair> mappedPair = mappedReplicas.getOrDefault(replica, new ConcurrentHashMap<>());
        IntStream.range(0, originPairs.size()).forEach(i -> mappedPair.put(originPairs.get(i), newPairs.get(i)));
        mappedReplicas.put(replica, mappedPair);
    }

//    private Map<OffLenPair, Map<AlluxioURI, OffLenPair>> mappedReplicas;
//
//    public Map<AlluxioURI, OffLenPair> getMappedReplicas(OffLenPair offLenPair){
//        return mappedReplicas.getOrDefault(offLenPair, new ConcurrentHashMap<>());
//    }
//
//    public void addReplicas(AlluxioURI replica, List<OffLenPair> originPairs, List<OffLenPair> newPairs){
//        IntStream.range(0, originPairs.size()).forEach(i -> {
//            Map<AlluxioURI, OffLenPair> offsetMap = mappedReplicas.getOrDefault(originPairs.get(i), new ConcurrentHashMap<>());
//            offsetMap.put(replica, newPairs.get(i));
//            mappedReplicas.put(originPairs.get(i), offsetMap);
//        });
//    }

}
