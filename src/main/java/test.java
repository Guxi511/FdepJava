import com.csvreader.CsvReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class test {

    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();


        String readerCsvFilePath = "C:\\Users\\guyun\\Desktop\\FdepJava\\data\\iris.csv";
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
        long endTime = System.currentTimeMillis();
        long usedTime = (endTime - startTime);
        System.out.println(usedTime);

        long t1 = System.currentTimeMillis();
        CSVUtil csvtest = new CSVUtil();
        List<String> records = new ArrayList<>();
        records = csvtest.readCSV(readerCsvFilePath);
        for(int i = 0;i<records.size();i++){
            System.out.println(records.get(i));
        }
        long t2 = System.currentTimeMillis();
        System.out.println(t2-t1);
    }
}
