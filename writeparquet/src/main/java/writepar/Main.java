package writepar;

import java.io.IOException;

/**
 *
 */
public class Main {

    public static void main(String[] args) throws IOException {
        ParquetInfo parquetInfo = new ParquetInfo();
        if (args.length > 0){
            switch (args[0]){
                case "test":
                    parquetInfo.test(args[1]);
                    break;

                case "alpha":
                    int fileNum = Integer.parseInt(args[1]);
                    int offNum = Integer.parseInt(args[2]);
                    parquetInfo.alphaTest(fileNum, offNum);
                    break;

                case "info":
                    int isRecord = Integer.parseInt(args[1]);
                    parquetInfo.sendInfo(args[2], args[3], isRecord);
                    break;

                case "write":
                    int isThrt = Integer.parseInt(args[1]);
                    int isRepl = Integer.parseInt(args[2]);

                    parquetInfo.writeParquet(isThrt, isRepl, args[3]);
            }
        }

        else {
            System.out.println("Require <file path, info path> or <location path>");
        }
    }
}
