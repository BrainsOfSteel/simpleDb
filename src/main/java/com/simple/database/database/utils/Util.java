package com.simple.database.database.utils;

import java.util.concurrent.ConcurrentHashMap;

public class Util {
    public final static String KEY_VALUE_DELIMITER = "\0";
    public final static String ENTRY_DELIMITER = "\n";
    public final static String ADD_OPERATION = "ADD";
    public final static String DEL_OPERATION = "DEL";
    public final static char CHECKSUM_CHARACTER = '\u0001';
    public final static String UNIT_TEST = "unit_test";
    public final static String BLOCK_START =  "**START BLOCK**" + CHECKSUM_CHARACTER;
    public final static String BLOCK_END = "**END BLOCK**" + CHECKSUM_CHARACTER;

    public static void loadStateFromLogLine(String logLine, ConcurrentHashMap<String, String> keyValuePair){
        String[] line = logLine.split(KEY_VALUE_DELIMITER);
        if(line[line.length-1].charAt(0) == CHECKSUM_CHARACTER ){
            if(line[1].equals(ADD_OPERATION)){
                String key = line[2];
                String value = line[3];
                keyValuePair.put(key, value);
            }
            else if(line[1].equals(DEL_OPERATION)){
                String key = line[2];
                if(!keyValuePair.containsKey(key)){
                    System.out.println("Key not found in DB. why ? "+ logLine +" key = "+key);
                }
                else{
                    keyValuePair.remove(key);
                }
            }
            else{
                System.out.println("Unknown operation in line =" + logLine);
            }
        }
        else{
            System.out.println("invalid checksum in line....skipping line = " + logLine);
        }
    }

    public static boolean isValid(String line) {
        if(line.charAt(line.length()-1) == Util.CHECKSUM_CHARACTER){
            return true;
        }
        else{
            return false;
        }
    }
}
