package fr.client.utils;

import alluxio.AlluxioURI;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.resource.CloseableResource;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;

/**
 *
 */
public class FRUtils {

    public static WorkerNetAddress getFileLocation(AlluxioURI path, FileSystem fileSystem, FileSystemContext context){
        try {
            URIStatus orginStatus = fileSystem.getStatus(path);
            // Assume part parquet file just occupy one block
            long blockId = orginStatus.getBlockIds().get(0);

            BlockInfo info;
            try (CloseableResource<BlockMasterClient> masterClientResource =
                         context.acquireBlockMasterClientResource()) {
                info = masterClientResource.get().getBlockInfo(blockId);
            }

            return info
                    .getLocations()
                    .get(0)
                    .getWorkerAddress();

        } catch (IOException | AlluxioException e) {
            e.printStackTrace();
            return null;
        }

    }
}
