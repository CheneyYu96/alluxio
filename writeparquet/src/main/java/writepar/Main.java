package writepar;

import java.io.IOException;

/**
 *
 */
public class Main {

    public static void main(String[] args) throws IOException {
        if (args.length > 1 && args.length % 2 == 0){
            ParquetInfo parquetInfo = new ParquetInfo();
            for (int i = 0; i < args.length; i=i+2){
                parquetInfo.sendInfo(args[i], args[i+1]);
            }
        }
        else {
            System.out.println("Require <file path, info path>");
        }
    }
}
