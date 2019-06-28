package readpar;

import java.io.IOException;

/**
 *
 */
public class Main {

    public static void main(String[] args) throws IOException {
        if (args.length > 1 && args.length % 2 == 1){
            long startTime = System.currentTimeMillis();
            System.out.println("Main-Received read cmd.");

            String fileName = args[0];

            long[] segs = new long[args.length - 1];
            for (int i = 1; i < args.length; i++){
                segs[i - 1] = Long.parseLong(args[i]);
            }

            FRParquetReader parquetInfo = new FRParquetReader();
            parquetInfo.read(fileName, segs);

            System.out.println("Main-execution time: " + (System.currentTimeMillis() - startTime));
        }
        else {
            System.out.println("Require (file path, List<OffLenPair>)");
        }
    }
}
