package fr.client;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import fr.client.file.FRFileReader;
import fr.client.file.FRFileWriter;
import fr.client.utils.OffLenPair;
import fr.client.utils.ReplUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;


/**
 *
 */
public class FRClientTest {
    private static final String TEST_PATH = "/test";
    private static final Logger LOG = LoggerFactory.getLogger(FRClientTest.class);

    public FRClientTest() {
    }

    public AlluxioURI writeFileTest(String localFile) throws Exception{
        // Test the effect of IO parallelism in a cluster, by distributing the file to different number of workers.

        FileInputStream is = new FileInputStream(localFile);
        File file = new File(localFile);
        FileSystem fs = FileSystem.Factory.get();

        int slash = localFile.lastIndexOf("/");
        String name = localFile.substring(slash + 1);
        AlluxioURI filePath = new AlluxioURI(String.format("%s/%s", TEST_PATH, name));

        FRFileWriter writer = new FRFileWriter(filePath);

        int len = (int) file.length();
        byte[] tBuf = new byte[len];
        int tBytesRead = is.read(tBuf);
        writer.writeFile(tBuf);

        is.close();
        return filePath;
    }

    public void readOffTest(AlluxioURI path, List<OffLenPair> pairs) throws Exception{
        FRFileReader reader = new FRFileReader(path);
        reader.readFile(pairs);
        System.out.println(new String(reader.getBuf()));
    }

    public void replicateTest(AlluxioURI path, List<OffLenPair> pairs, int replicas) throws Exception{
        FRClient client = new FRClient();
        ReplUnit replUnit = new ReplUnit(pairs, replicas);
        List<AlluxioURI> pathes = client.copyFileOffset(path, replUnit);
        pathes.forEach( i -> System.out.println(i.getPath()));
    }

    public static void main(String[] args) throws Exception {
        FRClientTest frClientTest = new FRClientTest();

        String testFile= System.getProperty("user.dir") + "/conf/alluxio-site.properties.template";
        List<OffLenPair> pairs = new ArrayList<>();
        pairs.add(new OffLenPair(4, 11));
        pairs.add(new OffLenPair(15, 20));

        AlluxioURI path = new AlluxioURI("/test/alluxio-site.properties.template");

//        LOG.info("--------------Test: write from local "+ testFile + " to Alluxio------------------");
//        AlluxioURI path = frClientTest.writeFileTest(testFile);

        LOG.info("--------------Test: read from Alluxio--------------------------");
        for (int i = 0; i < 10; i++) {
            frClientTest.readOffTest(path, pairs);
        }


//        LOG.info("--------------Test: Replicate data segment in Alluxio--------------");
//        frClientTest.replicateTest(path, pairs, 2);

    }

}
