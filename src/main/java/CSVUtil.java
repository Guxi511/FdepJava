import com.csvreader.CsvReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.FileNotFoundException;


public class CSVUtil {
    static String readPath = "C:\\Users\\guyun\\Desktop\\FdepJava\\data\\iris.csv";

    /**
     *读取每行数据
     * @param readPath
     * @return
     *
     */

    public static List<String> readCSV(String readPath) throws FileNotFoundException {
        String filePath = readPath;
        List<String> listData = new ArrayList<>();
        try{
            filePath = readPath;
            CsvReader csvReader = new CsvReader(filePath);
            //boolean re = csvReader.readHeaders(); 略过表头
            while(csvReader.readRecord()){
                String rawRecord = csvReader.getRawRecord();
                listData.add(rawRecord);
            }
            return listData;
        }catch (FileNotFoundException e){
            throw new RuntimeException("文件未找到");
        }catch (IOException e){
            throw new RuntimeException(e.getMessage());
        }

    }
}
