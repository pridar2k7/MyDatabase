package com.database.assignment;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class MyDatabase {
    public Map<Integer, List<Long>> idIndex;
    public Map<String, List<Long>> companyIndex;
    public Map<String, List<Long>> drug_idIndex;
    public Map<String, List<Long>> trialsIndex;
    public Map<String, List<Long>> patientsIndex;
    public Map<String, List<Long>> dosage_mgIndex;
    public Map<String, List<Long>> readingIndex;
    public Map<String, List<Long>> double_blindIndex;
    public Map<String, List<Long>> controlled_studyIndex;
    public Map<String, List<Long>> govt_fundedIndex;
    public Map<String, List<Long>> fda_approvedIndex;
    public List<Integer> integerList;
    public List<Integer> stringList;

    public MyDatabase() throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        this.idIndex = new TreeMap<Integer, List<Long>>();
        this.companyIndex = new TreeMap<String, List<Long>>();
        this.drug_idIndex = new TreeMap<String, List<Long>>();
        this.trialsIndex = new TreeMap<String, List<Long>>();
        this.patientsIndex = new TreeMap<String, List<Long>>();
        this.dosage_mgIndex = new TreeMap<String, List<Long>>();
        this.readingIndex = new TreeMap<String, List<Long>>();
        this.double_blindIndex = new TreeMap<String, List<Long>>();
        this.controlled_studyIndex = new TreeMap<String, List<Long>>();
        this.govt_fundedIndex = new TreeMap<String, List<Long>>();
        this.fda_approvedIndex = new TreeMap<String, List<Long>>();
//        integerList = Arrays.asList(0,3,4,5);
        stringList = Arrays.asList(0, 1,2, 3, 4, 5,6,7,8,9,10);
    }

    public static void main(String[] args) throws IOException, NoSuchFieldException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        MyDatabase myDatabase = new MyDatabase();
        RandomAccessFile file = new RandomAccessFile("resources/data.txt", "rw");


        List<String> records = Files.readAllLines(Paths.get("resources/PHARMA_TRIALS_1000B.csv"), StandardCharsets.UTF_8);
        String fieldNames = records.get(0);
        String[] fieldNameArray = fieldNames.split(",");
        records.remove(0);
        for (String serverDetail : records) {
            long recordStartLocation = file.getFilePointer();
            String[] splitData = serverDetail.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            myDatabase.createBinaryFile(file, splitData);
            for (int count= 0; count < fieldNameArray.length; count++ ) {
                if(NumberUtils.isDigits(splitData[count])){
                    myDatabase.createStringIndex(fieldNameArray[count], Integer.parseInt(splitData[count]), recordStartLocation);
                } else{
                    myDatabase.createStringIndex(fieldNameArray[count], splitData[count], recordStartLocation);
                }
            }
        }

        System.out.println("File write completed!");
        System.out.println(myDatabase.idIndex.toString());
        System.out.println(myDatabase.companyIndex.toString());
        System.out.println(myDatabase.drug_idIndex.toString());
        System.out.println(myDatabase.trialsIndex.toString());
        System.out.println(myDatabase.patientsIndex.toString());
        System.out.println(myDatabase.dosage_mgIndex.toString());
        System.out.println(myDatabase.readingIndex.toString());
        System.out.println(myDatabase.double_blindIndex.toString());
        System.out.println(myDatabase.controlled_studyIndex.toString());
        System.out.println(myDatabase.govt_fundedIndex.toString());
        System.out.println(myDatabase.fda_approvedIndex.toString());
        file.close();


    }

    private void createIntegerIndex(String fieldName, Integer key, long recordStartLocation) throws NoSuchFieldException, IllegalAccessException {

        Map<Integer, List<Long>> sampleMap1 = (TreeMap<Integer, List<Long>>) MyDatabase.class.getField(fieldName + "Index").get(this);
        if(sampleMap1.containsKey(key)){
            List<Long> startValues = (ArrayList)sampleMap1.get(key);
            startValues.add(recordStartLocation);
            sampleMap1.put(key, startValues);
        } else {
            ArrayList<Long> startValues = new ArrayList<Long>();
            startValues.add(recordStartLocation);
            sampleMap1.put(key, startValues);
        }
    }

    private <T> void createStringIndex(String fieldName, T key, long recordStartLocation) throws NoSuchFieldException, IllegalAccessException {
//        Object keyValue;

        Map<T, List<Long>> sampleMap1 = (TreeMap<T, List<Long>>) MyDatabase.class.getField(fieldName + "Index").get(this);
        if(sampleMap1.containsKey(key)){
            List<Long> startValues = (ArrayList)sampleMap1.get(key);
            startValues.add(recordStartLocation);
            sampleMap1.put(key, startValues);
        } else {
            ArrayList<Long> startValues = new ArrayList<Long>();
            startValues.add(recordStartLocation);
            sampleMap1.put(key, startValues);
        }
    }

    private void createBinaryFile(RandomAccessFile file, String[] splitData) throws IOException {
        splitData[1] = splitData[1].replaceAll("\"", "");
        file.writeInt(Integer.parseInt(splitData[0]));
        file.writeByte(splitData[1].length());
        file.writeBytes(splitData[1]);
        file.writeBytes(splitData[2]);
        file.writeShort(Integer.parseInt(splitData[3]));
        file.writeShort(Integer.parseInt(splitData[4]));
        file.writeShort(Integer.parseInt(splitData[5]));
        file.writeFloat(Float.parseFloat(splitData[6]));
        int value = 0;
        value = Boolean.parseBoolean(splitData[7]) ? (value | 8) : (value);
        value = Boolean.parseBoolean(splitData[8]) ? (value | 4) : (value);
        value = Boolean.parseBoolean(splitData[9]) ? (value | 2) : (value);
        value = Boolean.parseBoolean(splitData[10]) ? (value | 1) : (value);
        file.writeByte(value);
    }
}
