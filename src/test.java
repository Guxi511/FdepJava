import com.csvreader.CsvReader;

import java.io.IOException;

public class test {

    public static void main(String[] args) throws IOException {


        String readerCsvFilePath = "C:\\Users\\guyun\\Desktop\\FdepJava\\data\\abalone.csv";
        CsvReader csvReaderUtil = new CsvReader(readerCsvFilePath);
        csvReaderUtil.readHeaders(); // 跳过表头   如果需要表头的话，不要写这句。
        String[] head = csvReaderUtil.getHeaders(); //获取表头
        while (csvReaderUtil.readRecord())
        {
            for (int i = 0; i < head.length; i++)
            {
                System.out.println(head[i] + ":" + csvReaderUtil.get(head[i]));
            }

        }
        csvReaderUtil.close();

    }
}
