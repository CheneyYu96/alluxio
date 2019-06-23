package readpar;

import fr.client.utils.OffLenPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class Main {

    public static void main(String[] args) throws IOException {
        if (args.length > 1 && args.length % 2 == 1){
            long startTime = System.currentTimeMillis();
            System.out.println("Main-Received read cmd.");

            String fileName = args[0];

            List<OffLenPair> columnsToRead = new ArrayList<>();
            for (int i = 1; i < args.length; i=i+2){
                columnsToRead.add(
                        new OffLenPair(Long.parseLong(args[i]), Long.parseLong(args[i + 1])));
            }

            FRParquetReader parquetInfo = new FRParquetReader();
            parquetInfo.read(fileName, columnsToRead);

            System.out.println("Main-execution time: " + (System.currentTimeMillis() - startTime));
        }
        else {
            System.out.println("Require (file path, List<OffLenPair>)");
        }
    }
}
