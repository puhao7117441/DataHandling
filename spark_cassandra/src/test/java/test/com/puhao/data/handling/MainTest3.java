package test.com.puhao.data.handling;

import com.puhao.data.handling.CSVReader;
import com.puhao.data.handling.Main;
import com.puhao.data.handling.SparkConf;
import com.puhao.data.handling.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

/**
 * When use PowerMock to mock static class, the mocked fiedl will cross all test cases.
 * To ensure each test case work independently, we need clear up or reset all static mocks,
 * this is annoying, so here use multiple test class (MainTest1,2,3...) to separate each test case.
 *
 * Created by pu on 2017/11/25.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CSVReader.class, SparkConf.class, Utils.class, Main.class, LogManager.class})
@PowerMockIgnore("javax.management.*") // to avoid warning message, this is not used
public class MainTest3 {

    @Test
    public void testMain_errorLog() throws Exception {
        CSVReader reader = Mockito.mock(CSVReader.class);
        PowerMockito.whenNew(CSVReader.class).withAnyArguments().thenReturn(reader);

        PowerMockito.mockStatic(SparkConf.class);
        Mockito.when(SparkConf.getSparkSession(any())).thenReturn(null);


        PowerMockito.mockStatic(Utils.class);
        String[] args = new String[]{"a", "b"};
        List<String> validPath = new ArrayList<>(1);
        validPath.add("a");
        Mockito.when(Utils.getAllMatchStringFromArray(any(), any())).thenReturn(validPath);

        Logger logger = Mockito.mock(Logger.class);
        PowerMockito.mockStatic(LogManager.class);
        Mockito.when(LogManager.getLogger(any(Class.class))).thenReturn(logger);

        ArgumentCaptor<Object> errorPathCapther = ArgumentCaptor.forClass(Object.class);
        Mockito.doNothing().when(logger).error(any(String.class), errorPathCapther.capture());


        ArgumentCaptor<String> pathCapter = ArgumentCaptor.forClass(String.class);
        Mockito.doNothing().when(reader).readAndSaveToDB(pathCapter.capture());


        PowerMockito.mockStatic(Main.class);
        PowerMockito.doCallRealMethod().when(Main.class, "main", any(String[].class));

        Main.PROPERTIES = new Properties();
        Main.PROPERTIES.setProperty(SparkConf.PROPERTY_KEY_FILE_NAME_PATTERN, ".+");

        Main.main(args);

        Mockito.verify(reader, Mockito.times(1)).readAndSaveToDB(any());
        assertEquals(1, pathCapter.getAllValues().size());
        assertEquals(validPath.get(0), pathCapter.getValue());

        assertEquals(1, errorPathCapther.getAllValues().size());
        assertEquals(args[1], errorPathCapther.getValue());
        Mockito.verify(logger, Mockito.times(1)).error(any(String.class), any(Object.class));


    }
}
