package fr.client;

import alluxio.AlluxioURI;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import fr.client.file.FRFileReader;
import fr.client.file.FRFileWriter;

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

    public List<AlluxioURI> copyFileOffset(AlluxioURI sourceFilePath, long offset, long length, int replicas){
        return IntStream
                .range(0,replicas)
                .mapToObj(i ->  getOneReplica(sourceFilePath, offset, length))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private AlluxioURI getOneReplica(AlluxioURI sourceFilePath, long offset, long length){
        String parentPath = sourceFilePath.getParent().getPath();

        String replicaParent = parentPath == null ? FR_DIR : FR_DIR + parentPath;
        String replicaName = sourceFilePath.getName() + "-" + offset + "-" + length + "-" + CommonUtils.getCurrentMs();

        AlluxioURI destFilePath = new AlluxioURI(String.format("%s/%s", replicaParent, replicaName));

        FRFileReader reader = new FRFileReader(sourceFilePath);
        FRFileWriter writer = new FRFileWriter(destFilePath);

        try {
            int toRead = reader.readFile(offset, length);
            writer.writeFile(reader.getBuf());
            return destFilePath;

        } catch (IOException | AlluxioException e) {
            e.printStackTrace();
            return null;
        }
    }
}
