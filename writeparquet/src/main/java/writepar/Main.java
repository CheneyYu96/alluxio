package writepar;

import java.io.IOException;

/**
 *
 */
public class Main {

    public static void main(String[] args) throws IOException {
        ParquetInfo parquetInfo = new ParquetInfo();
        if (args.length == 3){
            int isRecord = Integer.parseInt(args[0]);
            parquetInfo.sendInfo(args[1], args[2], isRecord);
        }
        else if (args.length == 2){
            if (args[0].equals("test")){
                parquetInfo.test(args[1]);
            }
            else {
                int fileNum = Integer.parseInt(args[0]);
                int offNum = Integer.parseInt(args[1]);
                parquetInfo.alphaTest(fileNum, offNum);
            }
        }
        else if (args.length == 1){
            parquetInfo.writeParquet(args[0]);
        }
        else {
            System.out.println("Require <file path, info path> or <location path>");
        }
    }
}
