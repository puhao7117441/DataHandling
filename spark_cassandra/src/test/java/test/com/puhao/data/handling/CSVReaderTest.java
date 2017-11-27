package test.com.puhao.data.handling;

import com.puhao.data.handling.CSVReader;
import com.puhao.data.handling.Main;
import com.puhao.data.handling.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Becarefule, PowerMockRunner is conflict with SparkSession
 * Created by pu on 2017/11/25.
 */
public class CSVReaderTest {
    public static final String LOCAL_FILE_PREFIX = "file:///";

    @BeforeClass
    public static void before(){
        /**
         * Ensure in windows platform the test case still able to execute
         */
        Utils.PATH_SEPARATOR = File.separator;
    }

    @AfterClass
    public static void clean(){
        /**
         * When run all test case together, all test case will reuse same JVM
         * if don't restore this static field, it will impact later test case
         */
        Utils.PATH_SEPARATOR = "/";
    }

    @Test
    public void test_empty_file() throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, URISyntaxException, IOException {

        String testFileName = "1999-12-22.csv"; // this file have one invalid row

        System.out.println("Path separator: " + Utils.PATH_SEPARATOR);


        Field pFilePath = Main.class.getDeclaredField("PROPERTIES_FILE_PATH");
        pFilePath.setAccessible(true);
        pFilePath.set(null, "test1.application.properties");

        Method initialPropertiesM = Main.class.getDeclaredMethod("initializeProperties");
        initialPropertiesM.setAccessible(true);
        initialPropertiesM.invoke(null);



        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("test_csv")//.config("spark.cassandra.connection.host","10.140.0.183")
                .getOrCreate();


        CSVReader reader = new CSVReader(session);

        Path testFilePath = Paths.get(CSVReaderTest.class.getClassLoader().getResource(testFileName).toURI());
        System.out.println("Test file: " + testFilePath);
        Dataset<Row> allData = reader.readCSVandConvertToTableStructure(LOCAL_FILE_PREFIX + testFilePath.toString());



        Row[] rows = (Row[]) allData.collect();
        assertEquals(0, rows.length);
    }

    @Test
    public void test_negative_invalid_line_in_csv() throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, URISyntaxException, IOException {

        String testFileName = "1999-12-11.csv"; // this file have one invalid row


        System.out.println("Path separator: " + Utils.PATH_SEPARATOR);


        Field pFilePath = Main.class.getDeclaredField("PROPERTIES_FILE_PATH");
        pFilePath.setAccessible(true);
        pFilePath.set(null, "test1.application.properties");

        Method initialPropertiesM = Main.class.getDeclaredMethod("initializeProperties");
        initialPropertiesM.setAccessible(true);
        initialPropertiesM.invoke(null);



        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("test_csv")//.config("spark.cassandra.connection.host","10.140.0.183")
                .getOrCreate();


        CSVReader reader = new CSVReader(session);

        Path testFilePath = Paths.get(CSVReaderTest.class.getClassLoader().getResource(testFileName).toURI());
        System.out.println("Test file: " + testFilePath);
        Dataset<Row> allData = reader.readCSVandConvertToTableStructure(LOCAL_FILE_PREFIX + testFilePath.toString());


        Row[] rows = (Row[]) allData.collect();


        compareRowWithOriginalFile(rows, testFilePath);



    }

    @Test
    public void test_normal() throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, URISyntaxException, IOException {

        String testFileName = "1999-12-01.csv";


        System.out.println("Path separator: " + Utils.PATH_SEPARATOR);


        Field pFilePath = Main.class.getDeclaredField("PROPERTIES_FILE_PATH");
        pFilePath.setAccessible(true);
        pFilePath.set(null, "test1.application.properties");

        Method initialPropertiesM = Main.class.getDeclaredMethod("initializeProperties");
        initialPropertiesM.setAccessible(true);
        initialPropertiesM.invoke(null);



        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("test_csv")//.config("spark.cassandra.connection.host","10.140.0.183")
                .getOrCreate();


        CSVReader reader = new CSVReader(session);

        Path testFilePath = Paths.get(CSVReaderTest.class.getClassLoader().getResource(testFileName).toURI());
        System.out.println("Test file: " + testFilePath);
        Dataset<Row> allData = reader.readCSVandConvertToTableStructure(LOCAL_FILE_PREFIX + testFilePath.toString());


        Row[] rows = (Row[]) allData.collect();


        compareRowWithOriginalFile(rows, testFilePath);
    }

    private void compareRowWithOriginalFile(Row[] rows, Path testFilePath) throws IOException {
        String expectedDate = Utils.getFileNameWithoutSuffix(testFilePath.toString());

        Map<String, List<String>> originalData = readCSVFile(testFilePath.toFile());

        int expectedRowCount = getTotalValueCount(originalData);

        assertEquals(expectedRowCount, rows.length);



        String stockId = null;
        String date = null;
        String value = null;
        List<String> origialValues = null;
        for(Row row : rows){
            stockId = row.getString(1);
            date = Main.fileNameDateFormatter.print(row.getDate(2).getTime());
            value = "" + row.getDouble(3);

            origialValues = originalData.get(stockId);

            assertNotNull(origialValues);
            assertEquals(expectedDate, date);
            assertEquals(true, origialValues.contains(value)); // Actually I'm consering here that double accuracy may make here fail
            /**
             * remove the matched value, if the data in rows are same as in orignal data
             * after all row be checked, the original data will be totally removed
             */
            origialValues.remove(value);
        }

        /**
         * All match data should be removed, so no data exist in the map
         */
        assertEquals(0, getTotalValueCount(originalData));
    }

    private int getTotalValueCount(Map<String, List<String>> originalData) {
        int sumHolder[] = new int[]{0};
        originalData.values().forEach(list -> sumHolder[0] += list.size());
        return sumHolder[0];
    }

    private Map<String,List<String>> readCSVFile(File file) throws IOException {
        Map<String,List<String>> map = new HashMap<>();
        try(BufferedReader reader = new BufferedReader(new FileReader(file))) {


            reader.readLine(); // ignore the header line

            String line = null;
            String data[] = null;
            List<String> itemValueList  = null;
            while (null != (line = reader.readLine())) {
                data = line.split(",");

                if(!data[0].matches("\\d+")){
                    //ignore invalid line
                    continue;
                }

                try {
                    /**
                     * Just verify if data is valid double
                     */
                    Double.parseDouble(data[1]);
                    Double.parseDouble(data[2]);
                    Double.parseDouble(data[3]);
                }catch (NumberFormatException e){
                    // if any data is invalid, ignore that line
                    continue;
                }



                itemValueList = map.get(data[0]);
                if(itemValueList == null){
                    itemValueList = new LinkedList<>();
                    map.put(data[0], itemValueList);
                }

                itemValueList.add(data[1]);
                itemValueList.add(data[2]);
                itemValueList.add(data[3]);
            }
        }
        return map;
    }


}
