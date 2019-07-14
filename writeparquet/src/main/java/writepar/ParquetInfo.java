package writepar;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.policy.SpecificHostPolicy;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerNetAddress;
import fr.client.file.FRFileWriter;
import fr.client.utils.FRUtils;
import fr.client.utils.OffLenPair;

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

    public void test(String filePath){
        try {
            URIStatus orginStatus = mFileSystem.getStatus(new AlluxioURI(filePath));

            List<FileBlockInfo> fileBlockInfos = orginStatus.getFileBlockInfos();

            System.out.println("file block info sizes: " + fileBlockInfos.size());

            System.out.println(fileBlockInfos);

        } catch (IOException | AlluxioException e) {
            e.printStackTrace();
        }

    }

    public void writeParquet(int isThrt, int isRepl, String locationFile) throws IOException {

        WriteType writeTpye = isThrt==1 ? WriteType.CACHE_THROUGH : WriteType.MUST_CACHE;

        System.out.println("Throttle: " + isThrt + ". Write Type: " + writeTpye);

        FileInputStream is = new FileInputStream(locationFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line;
        int idx = 0;
        while ((line = reader.readLine()) != null) {
            String[] splitLine = line.split(",");
            String path = splitLine[0];
            String address = splitLine[1];

            path = "/test/" + idx++;

            FRFileWriter writer = new FRFileWriter(new AlluxioURI(path));
            writer.setWriteOption(CreateFileOptions.defaults().setWriteType(writeTpye).setLocationPolicy(new SpecificHostPolicy(address)));

            FileInputStream localFileStream;
            byte[] tBuf;

            if (isRepl == 1){
                System.out.println("Write replica: " + path + " on address: " + address);

                String sourceFile = splitLine[2];
                List<OffLenPair> pairs = new ArrayList<>();
                for (int i = 3; i < splitLine.length; i++){
                    String[] pairSpl = splitLine[i].split(":");
                    pairs.add(new OffLenPair(Long.parseLong(pairSpl[0]), Long.parseLong(pairSpl[1])));
                }

                localFileStream = new FileInputStream(sourceFile);

                long length = pairs
                        .stream()
                        .mapToLong( o -> o.length)
                        .sum();

                System.out.println("Source: " + sourceFile + ". len: " + length);

                tBuf = new byte[(int) length];
                int tBytesRead = localFileStream.read(tBuf);

            }
            else {
                System.out.println("Write source file: " + path + " on address: " + address);

                localFileStream = new FileInputStream(path);
                File file = new File(path);

                int len = (int) file.length();
                tBuf = new byte[len];
                int tBytesRead = localFileStream.read(tBuf);

            }

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
