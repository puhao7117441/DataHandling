package test.com.puhao.data.handling;

import com.puhao.data.handling.Main;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * When use PowerMock to mock static class, the mocked fiedl will cross all test cases.
 * To ensure each test case work independently, we need clear up or reset all static mocks,
 * this is annoying, so here use multiple test class (MainTest1,2,3...) to separate each test case.
 *
 * Created by pu on 2017/11/26.
 */
public class MainTest4 {
    @Test
    public void test_1() throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        Field pFilePath = Main.class.getDeclaredField("PROPERTIES_FILE_PATH");
        pFilePath.setAccessible(true);


        Method initialPropertiesM = Main.class.getDeclaredMethod("initializeProperties");
        initialPropertiesM.setAccessible(true);


        Throwable expectE = null;
        for(int i = 0; i < 10; i++) {

            pFilePath.set(null, "negative/n"+i+".application.properties");
            System.out.println("negative/n"+i+".application.properties");
            expectE = null;
            try {
                initialPropertiesM.invoke(null);
            }catch (InvocationTargetException e){
                expectE = e.getCause();
            }


            assertNotNull(expectE);
            assertEquals(IllegalArgumentException.class, expectE.getClass());
        }
    }
}
