package HyFD.structures;

import com.csvreader.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RelationalInput {

       public List<String> columnNames;
       public int numberOfColumns;
       private CsvReader csvReader;

    public RelationalInput(String path) throws IOException {
        this.csvReader = new CsvReader(path);
        csvReader.readRecord();
        String[] s = csvReader.getRawRecord().split(",");
        columnNames = Arrays.asList(s);
        numberOfColumns = columnNames.size();
    }
    public boolean hasNext() throws IOException {
        return this.csvReader.readRecord();
    }

    public List<String> next(){
        String[] splited = csvReader.getRawRecord().split(",");
        return Arrays.asList(splited);
    }
}
