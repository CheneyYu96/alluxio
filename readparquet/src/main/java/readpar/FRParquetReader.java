package readpar;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.AlluxioException;
import fr.client.file.FRFileReader;
import fr.client.utils.OffLenPair;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Properties;

/**
 *
 */
public class FRParquetReader {

    private static final Logger LOG = LoggerFactory.getLogger(FRParquetReader.class);


    public FRParquetReader() throws IOException {
        // configure log
        Properties props = new Properties();
        String absPath = System.getProperty("user.dir");
        String logFile = absPath + "/log4j.properties";
        props.load(new FileInputStream(logFile));
        PropertyConfigurator.configure(props);

        // configure alluxio master
        String masterAddr = null;

        FileInputStream is = new FileInputStream("/home/ec2-user/alluxio/conf/alluxio-site.properties");
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line;
        while ((line = reader.readLine()) != null) {
            if(line.startsWith("alluxio.master.hostname")){
                masterAddr = line.trim().split("=")[1];
            }
        }
        is.close();

        Configuration.set(PropertyKey.MASTER_HOSTNAME, masterAddr);
    }

    public void read(String filePath, List<OffLenPair> columnsToRead){
//        long startTimeMs = CommonUtils.getCurrentMs();
        LOG.info("Read file: {}; off: {}", filePath, columnsToRead);
        FRFileReader reader = new FRFileReader(new AlluxioURI(filePath), true);
        try {
            reader.readFile(columnsToRead);
        } catch (IOException | AlluxioException e) {
            e.printStackTrace();
        }

    }
}