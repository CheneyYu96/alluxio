package writepar;

import java.io.IOException;

/**
 *
 */
public class Main {

    public static void main(String[] args) throws IOException {
        if (args.length == 3){
            int isRecord = Integer.parseInt(args[0]);
            ParquetInfo parquetInfo = new ParquetInfo();
            parquetInfo.sendInfo(args[1], args[2], isRecord);
        }
        else if (args.length == 2){
            ParquetInfo parquetInfo = new ParquetInfo();
            parquetInfo.sendInfo(args[0], args[1]);
        }
        else if (args.length == 1){
            ParquetInfo parquetInfo = new ParquetInfo();
            parquetInfo.writeParquet(args[0]);
        }
        else {
            System.out.println("Require <file path, info path> or <location path>");
        }
    }
}
