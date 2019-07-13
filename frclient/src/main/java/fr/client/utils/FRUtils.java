package fr.client.utils;

import alluxio.AlluxioURI;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.resource.CloseableResource;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.List;

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

            List<BlockLocation> blockLocationList =  info.getLocations();

            if(blockLocationList.size() > 0){
                return blockLocationList.get(0).getWorkerAddress();
            }
            else {
                return null;
            }

        } catch (IOException | AlluxioException e) {
            e.printStackTrace();
            return null;
        }

    }
}
