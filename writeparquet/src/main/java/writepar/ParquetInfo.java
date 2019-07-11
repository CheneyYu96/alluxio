package writepar;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.policy.SpecificHostPolicy;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;
import fr.client.file.FRFileWriter;
import fr.client.utils.FRUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 */
public class ParquetInfo {
    private FileSystem mFileSystem;

    private final String locationsFile;

    public ParquetInfo() {
        mFileSystem = FileSystem.Factory.get();
        locationsFile = System.getProperty("user.dir") + "/origin-locs.txt";
    }

    public void alphaTest(int fileNum, int offNum) throws IOException{
        List<Long> lengthes = IntStream.range(0, offNum).mapToLong( i -> 100).boxed().collect(Collectors.toList());
        List<Long> offsets = IntStream.range(0, offNum).mapToLong( i -> i * 100).boxed().collect(Collectors.toList());

        String tag = "<alpha>" + fileNum;
        mFileSystem.sendParquetInfo(new AlluxioURI(tag), offsets, lengthes);
    }

    public void sendInfo(String filePath, String infoPath, int isRecord) throws IOException{
        long startTimeMs = CommonUtils.getCurrentMs();

        List<Long> offset = new ArrayList<>();
        List<Long> length = new ArrayList<>();
        FileInputStream is = new FileInputStream(infoPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String str;
        String [] temp;
        while (true) {
            str = reader.readLine();
            if(str!=null){
                temp = str.split(",");
                offset.add(Long.parseLong(temp[0]));
                length.add(Long.parseLong(temp[1]));
            } else{
                break;
            }
        }
        is.close();

        mFileSystem.sendParquetInfo(new AlluxioURI(filePath), offset, length);

        System.out.println("Send parquet file info. " + filePath + "; elapsed:" + (CommonUtils.getCurrentMs() - startTimeMs));

        if (isRecord == 1) {
            recordLoc(filePath);
        }

    }

    private void recordLoc(String filePath) throws IOException {
        WorkerNetAddress address = FRUtils.getFileLocation(new AlluxioURI(filePath), mFileSystem, FileSystemContext.get());
        if (address == null){
            System.err.println("File block address is null");
            return;
        }
        FileWriter fw = new FileWriter(locationsFile, true);
        fw.write(filePath + "," + address.getHost() + "\n");
        fw.close();
        System.out.println("Write loc for file " + filePath);
    }

    public void writeParquet(String locationFile) throws IOException {

        FileInputStream is = new FileInputStream(locationFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] splitLine = line.split(",");
            String path = splitLine[0];
            String address = splitLine[1];

            FRFileWriter writer = new FRFileWriter(new AlluxioURI(path));
            writer.setWriteOption(CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE).setLocationPolicy(new SpecificHostPolicy(address)));

            FileInputStream localFileStream = new FileInputStream(path);
            File file = new File(path);

            int len = (int) file.length();
            byte[] tBuf = new byte[len];
            int tBytesRead = localFileStream.read(tBuf);
            try {
                writer.writeFile(tBuf);
            } catch (AlluxioException e) {
                e.printStackTrace();
            }

            localFileStream.close();

        }
        is.close();

    }
}
