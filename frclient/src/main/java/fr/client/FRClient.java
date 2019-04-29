package fr.client;

import alluxio.AlluxioURI;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import fr.client.file.FRFileReader;
import fr.client.file.FRFileWriter;
import fr.client.utils.OffLenPair;
import fr.client.utils.ReplUnit;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 * The class is the endpoint to collect and replicate data segments, invoked by master
 */
public class FRClient {
    public static final String FR_DIR = "/fr_dir";

    public FRClient() {
    }

    public List<AlluxioURI> copyFileOffset(AlluxioURI sourceFilePath, ReplUnit replUnit){
        return IntStream
                .range(0,replUnit.getReplicas())
                .mapToObj(i ->  getOneReplica(sourceFilePath, replUnit.getOffLenPairs()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private AlluxioURI getOneReplica(AlluxioURI sourceFilePath, List<OffLenPair> pairs){
        String parentPath = sourceFilePath.getParent().getPath();

        String replicaParent = parentPath == null ? FR_DIR : FR_DIR + parentPath;
        String midName = pairs
                .stream()
                .map( o -> o.offset + ":" + o.length)
                .reduce("", (f, s) -> f + "-" + s);
        String replicaName = sourceFilePath.getName() + midName + "-" + CommonUtils.getCurrentMs();

        AlluxioURI destFilePath = new AlluxioURI(String.format("%s/%s", replicaParent, replicaName));

        FRFileReader reader = new FRFileReader(sourceFilePath);
        FRFileWriter writer = new FRFileWriter(destFilePath);

        try {
            int toRead = reader.readFile(pairs);
            writer.writeFile(reader.getBuf());
            return destFilePath;

        } catch (IOException | AlluxioException e) {
            e.printStackTrace();
            return null;
        }
    }
}
