package com.database.assignment;

import com.sun.xml.internal.ws.util.StringUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class MyDatabase {

    public static void main(String[] args) throws IOException {
//        String str = "129018";
//        String str2 = String.format("%10s", str).replace(' ', '0');
//        System.out.println(str2);
//
        MyDatabase myDatabase = new MyDatabase();
//        String arg = "GlaxoSmithKline LLC";
//        String sample = String.format("%04x", arg.length());
//        System.out.println(sample);
//        System.out.println(myDatabase.hexToBin(sample));
//        System.out.println(String.format("%04x", 16));
//        String hexlength = myDatabase.convert(arg.length());
//        System.out.println(hexlength);
//        System.out.println(myDatabase.hexToBin(hexlength));
//        String concat = arg;
//        System.out.println(concat);
//        String output = myDatabase.toHex(concat);
//        System.out.println(output);
//        String s = myDatabase.hexToBin(output);
//        System.out.println(s);
        RandomAccessFile file = new RandomAccessFile("resources/data.txt", "rw");


        List<String> records = Files.readAllLines(Paths.get("resources/PHARMA_TRIALS_1000B.csv"), StandardCharsets.UTF_8);
        String fieldNames = records.get(0);
        String[] fieldNameArray = fieldNames.split(",");
        records.remove(0);
        for (String serverDetail : records) {
            String[] splitData = serverDetail.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
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
        System.out.println("File write completed!");
        file.close();


    }

    public String hexToBin(String s) {
//        return StringUtils.leftPad(Integer.toBinaryString(1), 16, '0');
    return String.format("%8s", new BigInteger(s, 16).toString(2)).replace(' ', '0');

    }

    public String toHex(String arg) {
        return String.format("%02x", new BigInteger(1, arg.getBytes(/*YOUR_CHARSET?*/)));
    }

    public String convert(int n) {
        return Integer.toHexString(n);
    }
}
