package readpar;

import alluxio.AlluxioURI;
import alluxio.exception.AlluxioException;
import fr.client.file.FRFileReader;
import fr.client.utils.OffLenPair;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 *
 */
public class FRParquetReader {

    private static final Logger LOG = LoggerFactory.getLogger(FRParquetReader.class);


    public FRParquetReader() {
        Properties props = new Properties();
        String absPath = System.getProperty("user.dir");
        String logFile = absPath + "/log4j.properties";
        try {
            props.load(new FileInputStream(logFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
        PropertyConfigurator.configure(props);
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
