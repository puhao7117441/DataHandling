package test.com.puhao.data.handling.common;

import com.puhao.data.handling.common.Utils;
import org.junit.Test;

import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by pu on 2017/11/25.
 */
public class UtilsTest {
//    @BeforeClass
//    public void before(){
//        /**
//         * in some other class
//         */
////        Utils.PATH_SEPARATOR = "/";
//    }

    @Test
    public void test_getFileName(){

        System.out.println(Utils.PATH_SEPARATOR);
        assertEquals("bac.xy", Utils.getFileName("/user/data/bac.xy"));
        assertEquals("xya-zys.csv", Utils.getFileName("/user/data/xya-zys.csv"));
        assertEquals("1970-11-10.csv", Utils.getFileName("/user/data/1970-11-10.csv"));
        assertEquals("x", Utils.getFileName("/user/data/x"));
        assertEquals("m", Utils.getFileName("m"));
        assertEquals("x11", Utils.getFileName("/user/data//x11"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_getFileName_nagtive_1(){
        Utils.getFileName("    ");
    }
    @Test(expected = IllegalArgumentException.class)
    public void test_getFileName_nagtive_2(){
        Utils.getFileName("");
    }
    @Test(expected = NullPointerException.class)
    public void test_getFileName_nagtive_3(){
        Utils.getFileName(null);
    }
    @Test(expected = IllegalArgumentException.class)
    public void test_getFileName_nagitive_4(){
        Utils.getFileNameWithoutSuffix("aba/");
    }


    @Test
    public void test_getFileNameWithoutSuffix(){
        assertEquals("1986-01-01", Utils.getFileNameWithoutSuffix("/usr/bin/1986-01-01.csv"));
        assertEquals("a.b", Utils.getFileNameWithoutSuffix("/usr/bin/a.b.c"));
        assertEquals("a.b", Utils.getFileNameWithoutSuffix("/usr/bin/a.b."));
        assertEquals("..", Utils.getFileNameWithoutSuffix("/usr/bin/..."));
        assertEquals("b..", Utils.getFileNameWithoutSuffix("b...c"));
        assertEquals("bcd d", Utils.getFileNameWithoutSuffix("bcd d.doc"));
        assertEquals("xyz_", Utils.getFileNameWithoutSuffix("xyz_.txt"));
        assertEquals("xyz_txt", Utils.getFileNameWithoutSuffix("xyz_txt"));
    }
    @Test(expected = NullPointerException.class)
    public void test_getFileNameWithoutSuffix_nagitive_1(){
        Utils.getFileNameWithoutSuffix(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_getFileNameWithoutSuffix_nagitive_2(){
        Utils.getFileNameWithoutSuffix("");
    }



    @Test
    public void test_getAllMatchStringFromArray_oneInvalid(){
        Pattern pattern  = Pattern.compile("(.*/)*\\d{4}-\\d{2}-\\d{2}\\.csv");
        String[] testStringArr = new String[]{"/usr/bin/1111-11-11.csv", "2222-22-22.csv", "/3333-33-33.csv"
                , "333-33-33.csv", "/axy3333-33-33.csv"};

        List<String> matchStringList = Utils.getAllMatchStringFromArray(testStringArr, pattern);


        assertEquals(true, matchStringList.contains(testStringArr[0]));
        assertEquals(true, matchStringList.contains(testStringArr[1]));
        assertEquals(true, matchStringList.contains(testStringArr[2]));
        assertEquals(false, matchStringList.contains(testStringArr[3]));
        assertEquals(false, matchStringList.contains(testStringArr[4]));
        assertEquals(testStringArr.length - 2, matchStringList.size());
    }


    @Test
    public void test_getAllMatchStringFromArray_anotherRegex(){
        Pattern pattern  = Pattern.compile("(.*/)*\\d{1,2}\\.txt");
        String[] testStringArr = new String[]{"/usr/bin/5.txt", "54.txt", "ab.txt", "a54.txt","/54.txt", "./54.csv","./99.txt"};

        List<String> matchStringList = Utils.getAllMatchStringFromArray(testStringArr, pattern);

        assertEquals(true, matchStringList.contains(testStringArr[0]));
        assertEquals(true, matchStringList.contains(testStringArr[1]));
        assertEquals(false, matchStringList.contains(testStringArr[2]));
        assertEquals(false, matchStringList.contains(testStringArr[3]));
        assertEquals(true, matchStringList.contains(testStringArr[4]));
        assertEquals(false, matchStringList.contains(testStringArr[5]));
        assertEquals(true, matchStringList.contains(testStringArr[6]));
        assertEquals(testStringArr.length - 3, matchStringList.size());
    }

    @Test
    public void test_getAllMatchStringFromArray_nothing(){
        Pattern pattern  = Pattern.compile("(.*/)*\\d{1,2}\\.txt");
        String[] testStringArr = new String[]{"/usr/bin/5x.txt", "54.txdt", "ab.txt", "a54.txt","/5d4.txt", "./54.csv","./929.txt"};

        List<String> matchStringList = Utils.getAllMatchStringFromArray(testStringArr, pattern);
        assertNotNull(matchStringList);
        assertEquals(false, matchStringList.contains(testStringArr[0]));
        assertEquals(false, matchStringList.contains(testStringArr[1]));
        assertEquals(false, matchStringList.contains(testStringArr[2]));
        assertEquals(false, matchStringList.contains(testStringArr[3]));
        assertEquals(false, matchStringList.contains(testStringArr[4]));
        assertEquals(false, matchStringList.contains(testStringArr[5]));
        assertEquals(false, matchStringList.contains(testStringArr[6]));
        assertEquals(0, matchStringList.size());
    }

    @Test
    public void test_getAllMatchStringFromArray_zeroArray(){
        Pattern pattern  = Pattern.compile("(.*/)*\\d{1,2}\\.txt");
        String[] testStringArr = new String[0];

        List<String> matchStringList = Utils.getAllMatchStringFromArray(testStringArr, pattern);

        assertEquals(0, matchStringList.size());
    }

    @Test(expected = NullPointerException.class)
    public void test_getAllMatchStringFromArray_nagetive(){
        Pattern pattern  = Pattern.compile("(.*/)*\\d{1,2}\\.txt");

        List<String> matchStringList = Utils.getAllMatchStringFromArray(null, pattern);

    }

    @Test
    public void test_getAllMatchStringFromArray_null_pattern_but_no_string(){
        List<String> matchStringList = Utils.getAllMatchStringFromArray(new String[0], null);

    }

    @Test(expected = NullPointerException.class)
    public void test_getAllMatchStringFromArray_negative_2(){
        List<String> matchStringList = Utils.getAllMatchStringFromArray(new String[]{"a", "b"}, null);

    }
}
