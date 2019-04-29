package alluxio.master.repl;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import fr.client.utils.OffLenPair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Maintain file replicas
 */
public class FileRepInfo {
    private AlluxioURI originalFilePath;

    private Map<OffLenPair, List<Pair<AlluxioURI, OffLenPair>>> mappedReplicas;

    public FileRepInfo(AlluxioURI originalFilePath) {
        this.originalFilePath = originalFilePath;
    }

    public List<Pair<AlluxioURI, OffLenPair>> getMappedReplicas(OffLenPair offLenPair){
        return mappedReplicas.getOrDefault(offLenPair, new ArrayList<>());
    }

}
