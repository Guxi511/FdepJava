package tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.csvreader.CsvReader;

public class CSVUtil {
    /**
     *读取每行数据
     * @param readPath
     * @return
     *
     */

    public static List<List<String>> readCSV(String readPath) throws FileNotFoundException {
        String filePath = readPath;
        List<List<String>> listData = new ArrayList<List<String>>();
        try{
            filePath = readPath;
            CsvReader csvReader = new CsvReader(filePath);
            //boolean re = csvReader.readHeaders(); 略过表头
            while(csvReader.readRecord()){
                String rawRecord = csvReader.getRawRecord(); //一整行记录包括逗号 以String格式返回
                String[] splited = rawRecord.split(",");
                List<String> list = new ArrayList<String>();
                int len = splited.length;

                for (int i =0; i < len; i++){
                    list.add(splited[i]);
                }
                /*
                StringTokenizer t = new StringTokenizer(rawRecord,",");
                List<String> list = new ArrayList<String>();
                while(t.hasMoreTokens()){
                    list.add(t.nextToken());
                }
                 */
                listData.add(list);
            }
            return listData;
        }catch (FileNotFoundException e){
            throw new RuntimeException("文件未找到");
        }catch (IOException e){
            throw new RuntimeException(e.getMessage());
        }

    }
}
