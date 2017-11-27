package test.com.puhao.data.handling;

import com.puhao.data.handling.CSVReader;
import com.puhao.data.handling.Main;
import com.puhao.data.handling.SparkConf;
import com.puhao.data.handling.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
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
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CSVReader.class, SparkConf.class, Utils.class, Main.class, LogManager.class})
@PowerMockIgnore("javax.management.*") // to avoid warning message, this is not used
public class MainTest1 {


    @Test
    public void testMain_withInput() throws Exception {
        CSVReader reader = Mockito.mock(CSVReader.class);
        PowerMockito.whenNew(CSVReader.class).withAnyArguments().thenReturn(reader);

        PowerMockito.mockStatic(SparkConf.class);
        Mockito.when(SparkConf.getSparkSession(any())).thenReturn(null);


        PowerMockito.mockStatic(Utils.class);
        String[] args = new String[]{"a", "b", "c"};
        List<String> validPath = Arrays.asList(args);
        Mockito.when(Utils.getAllMatchStringFromArray(any(), any())).thenReturn(validPath);


        ArgumentCaptor<String> pathCapter = ArgumentCaptor.forClass(String.class);
        Mockito.doNothing().when(reader).readAndSaveToDB(pathCapter.capture());


        PowerMockito.mockStatic(Main.class);
        PowerMockito.doCallRealMethod().when(Main.class, "main", any(String[].class));

        Main.PROPERTIES = new Properties();
        Main.PROPERTIES.setProperty(SparkConf.PROPERTY_KEY_FILE_NAME_PATTERN, ".+");

        Main.main(args);

        assertEquals(3, pathCapter.getAllValues().size());
        for(int i = 0 ; i < args.length; i++) {
            assertEquals(args[i], pathCapter.getAllValues().get(i));
        }

    }

}
