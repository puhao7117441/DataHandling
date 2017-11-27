package com.puhao.data.handling;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by pu on 2017/11/25.
 */
public class Utils {
    public static String PATH_SEPARATOR = "/";


    public static List<String> getAllMatchStringFromArray(String[] stringArrays, Pattern pattern) {
        List<String> matchStringList = new ArrayList<>(stringArrays.length);
        for(String string : stringArrays){
            if(pattern.matcher(string).matches()){
                matchStringList.add(string);
            }
        }
        return matchStringList;
    }



    public static String getFileName(String filePath){
        if(filePath == null ){
            throw new NullPointerException();
        }

        if(StringUtils.isBlank(filePath)){
            throw new IllegalArgumentException("Invalid empty file path");
        }

        String fileName = null;
        int lastSlashIndex = filePath.lastIndexOf(PATH_SEPARATOR);
        if(lastSlashIndex == filePath.length() - 1){
            throw new IllegalArgumentException("Invalid file path: " + filePath + " no file name given. Is it a folder?");
        }
        if(lastSlashIndex < 0){
            fileName = filePath;
        }else{
            fileName = filePath.substring(lastSlashIndex + 1);
        }
        return fileName;
    }

    public static String getFileNameWithoutSuffix(String filePath) {

        String fileName = getFileName(filePath);

        int lastDotIndex = fileName.lastIndexOf('.');
        if(lastDotIndex < 0){
            return fileName;
        }else{
            return fileName.substring(0, lastDotIndex);
        }
    }


}
