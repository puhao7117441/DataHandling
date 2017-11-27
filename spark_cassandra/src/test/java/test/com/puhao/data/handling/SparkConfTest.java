package test.com.puhao.data.handling;

import com.puhao.data.handling.CSVReader;
import com.puhao.data.handling.Main;
import com.puhao.data.handling.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

/**
 * Created by pu on 2017/11/25.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({SparkSession.class})
@PowerMockIgnore("javax.management.*") // to avoid warning message, this is not used
public class SparkConfTest {

    @Test
    public void testSparkBuilder_anony(){
        PowerMockito.mockStatic(SparkSession.class);
        SparkSession.Builder builder = Mockito.mock(SparkSession.Builder.class);

        PowerMockito.when(SparkSession.builder()).thenReturn(builder);

        ArgumentCaptor<String> appNameCapter = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> masterURLCapter = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> configKeyCapter = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> configValueCapter = ArgumentCaptor.forClass(String.class);

        Mockito.when(builder.appName(appNameCapter.capture())).thenReturn(builder);
        Mockito.when(builder.master(masterURLCapter.capture())).thenReturn(builder);
        Mockito.when(builder.config(configKeyCapter.capture(), configValueCapter.capture())).thenReturn(builder);


        Properties p = new Properties();
        p.setProperty(SparkConf.PROPERTY_KEY_SPARK_MASTER_URL, "masterURL");
        p.setProperty(SparkConf.PROPERTY_KEY_SPARK_APP_NAME, "appName");
        p.setProperty(SparkConf.PROPERTY_KEY_CASSANDRAN_CONNECTION_HOST, "cassandraHost");
        p.setProperty(SparkConf.PROPERTY_KEY_CASSANDRAN_ANONYMOUS_LOGIN, "true");


        SparkConf.getSparkSession(p);


        Mockito.verify(builder, Mockito.times(1)).appName(any());
        Mockito.verify(builder, Mockito.times(1)).master(any());
        Mockito.verify(builder, Mockito.times(1)).config(any(),any());

        assertEquals(p.getProperty(SparkConf.PROPERTY_KEY_SPARK_APP_NAME), appNameCapter.getValue());
        assertEquals(1, appNameCapter.getAllValues().size());

        assertEquals(p.getProperty(SparkConf.PROPERTY_KEY_SPARK_MASTER_URL), masterURLCapter.getValue());
        assertEquals(1, masterURLCapter.getAllValues().size());

        assertEquals(SparkConf.PROPERTY_KEY_CASSANDRAN_CONNECTION_HOST, configKeyCapter.getValue());
        assertEquals(1, configKeyCapter.getAllValues().size());

        assertEquals(p.getProperty(SparkConf.PROPERTY_KEY_CASSANDRAN_CONNECTION_HOST), configValueCapter.getValue());
        assertEquals(1, configValueCapter.getAllValues().size());
    }

    @Test
    public void testSparkBuilder_login(){
        PowerMockito.mockStatic(SparkSession.class);
        SparkSession.Builder builder = Mockito.mock(SparkSession.Builder.class);

        PowerMockito.when(SparkSession.builder()).thenReturn(builder);

        ArgumentCaptor<String> appNameCapter = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> masterURLCapter = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> configKeyCapter = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> configValueCapter = ArgumentCaptor.forClass(String.class);

        Mockito.when(builder.appName(appNameCapter.capture())).thenReturn(builder);
        Mockito.when(builder.master(masterURLCapter.capture())).thenReturn(builder);
        Mockito.when(builder.config(configKeyCapter.capture(), configValueCapter.capture())).thenReturn(builder);


        Properties p = new Properties();
        p.setProperty(SparkConf.PROPERTY_KEY_SPARK_MASTER_URL, "masterURL");
        p.setProperty(SparkConf.PROPERTY_KEY_SPARK_APP_NAME, "appName");
        p.setProperty(SparkConf.PROPERTY_KEY_CASSANDRAN_CONNECTION_HOST, "cassandraHost");
        p.setProperty(SparkConf.PROPERTY_KEY_CASSANDRAN_AUTH_USERNAME, "puhao");
        p.setProperty(SparkConf.PROPERTY_KEY_CASSANDRAN_AUTH_PASSWORD, "myPass");



        SparkConf.getSparkSession(p);


        Mockito.verify(builder, Mockito.times(1)).appName(any());
        Mockito.verify(builder, Mockito.times(1)).master(any());
        Mockito.verify(builder, Mockito.times(3)).config(any(),any());

        assertEquals(p.getProperty(SparkConf.PROPERTY_KEY_SPARK_APP_NAME), appNameCapter.getValue());
        assertEquals(1, appNameCapter.getAllValues().size());

        assertEquals(p.getProperty(SparkConf.PROPERTY_KEY_SPARK_MASTER_URL), masterURLCapter.getValue());
        assertEquals(1, masterURLCapter.getAllValues().size());


        /**
         * For config, the sequence is not improtant, so only check all those value exist and count is correct
         */
        assertEquals(true, configKeyCapter.getAllValues().contains(SparkConf.PROPERTY_KEY_CASSANDRAN_AUTH_USERNAME));
        assertEquals(true, configKeyCapter.getAllValues().contains(SparkConf.PROPERTY_KEY_CASSANDRAN_AUTH_PASSWORD));
        assertEquals(true, configKeyCapter.getAllValues().contains(SparkConf.PROPERTY_KEY_CASSANDRAN_CONNECTION_HOST));

        assertEquals(true, configValueCapter.getAllValues().contains(p.getProperty(SparkConf.PROPERTY_KEY_CASSANDRAN_AUTH_USERNAME)));
        assertEquals(true, configValueCapter.getAllValues().contains(p.getProperty(SparkConf.PROPERTY_KEY_CASSANDRAN_AUTH_PASSWORD)));
        assertEquals(true, configValueCapter.getAllValues().contains(p.getProperty(SparkConf.PROPERTY_KEY_CASSANDRAN_CONNECTION_HOST)));


        assertEquals(3, configKeyCapter.getAllValues().size());
        assertEquals(3, configValueCapter.getAllValues().size());
    }

}
