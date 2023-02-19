package com.simple.database.database.replica.engine;

import com.simple.database.database.replica.LogAwareWriteAheadReader;
import com.simple.database.database.utils.Util;
import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class DatabaseEngineTest {

    private static String walReplicaFileName = "src/test/walReplica.txt";

    @Test
    void walLogsFromMasterSkipMessages() throws Exception{
        String walReplicaFileName = "src/test/walReplica.txt";
        LogAwareWriteAheadReader logAwareWriteAheadReader = null;
        DatabaseEngine databaseEngine = null;
        try {
            logAwareWriteAheadReader = LogAwareWriteAheadReader.getTestInstance(walReplicaFileName);
            databaseEngine = DatabaseEngine.getTestInstance(logAwareWriteAheadReader);
            databaseEngine.walLogsFromMaster(2, getWalLines(), 1, 10);
            assert(databaseEngine.getBufferedMessages().size() == 1);
        }catch (Exception e){
            throw e;
        }
        finally {
            logAwareWriteAheadReader.closeFiles();
            Files.deleteIfExists(Path.of(walReplicaFileName));
        }
    }

    @Test
    void walLogsFromMasterParseOutOfBufferedMessages() throws Exception{
        String walReplicaFileName = "src/test/walReplica.txt";
        LogAwareWriteAheadReader logAwareWriteAheadReader = null;
        DatabaseEngine databaseEngine = null;
        try{
            logAwareWriteAheadReader = LogAwareWriteAheadReader.getTestInstance(walReplicaFileName);
            databaseEngine = DatabaseEngine.getTestInstance(logAwareWriteAheadReader);
            List<String> firstBatch = getGeneratedWalLines("key1", "value1");
            List<String> secondBatch = getGeneratedWalLines("key2", "value2");
            List<String> thirdBatch = getGeneratedWalLines("key3", "value3");

            databaseEngine.walLogsFromMaster(0, firstBatch, 2, 200);
            databaseEngine.walLogsFromMaster(0, secondBatch, 1, 200);
            databaseEngine.walLogsFromMaster(0, thirdBatch, 0, 200);

            assert(databaseEngine.getBufferedMessages().size() == 0);
            assert(databaseEngine.getKeySetSize() == (firstBatch.size() + secondBatch.size() + thirdBatch.size()));
        }catch (Exception e){
            throw e;
        }
        finally{
            logAwareWriteAheadReader.closeFiles();
            Files.deleteIfExists(Path.of(walReplicaFileName));
        }
    }

    @Test
    void rejectPreviousBufferedMethodsWithVersionUpgrade() throws Exception{
        LogAwareWriteAheadReader logAwareWriteAheadReader = null;
        try{
            List<String> firstBatch = getGeneratedWalLines("key1", "value1");
            List<String> secondBatch = getGeneratedWalLines("key2", "value2");
            logAwareWriteAheadReader = LogAwareWriteAheadReader.getTestInstance(walReplicaFileName);
            DatabaseEngine databaseEngine = DatabaseEngine.getTestInstance(logAwareWriteAheadReader);
            databaseEngine.walLogsFromMaster(0, firstBatch, 1, 200);
            databaseEngine.walLogsFromMaster(100, secondBatch, 0, 200);
            assert(databaseEngine.getBufferedMessages().size() == 0);
            assert(databaseEngine.getKeySetSize() == secondBatch.size());
        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }
        finally {
            logAwareWriteAheadReader.closeFiles();
            Files.deleteIfExists(Path.of(walReplicaFileName));
        }
    }

    private List<String> getGeneratedWalLines(String key, String value){
        int versionNumber = 0;
        List<String> walLines = new ArrayList<>();
        for(int i = 0;i<10;i++){
            String walLine = "" + versionNumber + "" + Util.KEY_VALUE_DELIMITER +"ADD" +Util.KEY_VALUE_DELIMITER + key +i +
                    Util.KEY_VALUE_DELIMITER+value+i+Util.KEY_VALUE_DELIMITER+""+Util.CHECKSUM_CHARACTER+ "" +Util.ENTRY_DELIMITER;
            walLines.add(walLine);
        }
        return walLines;
    }

    private List<String> getWalLines(){
        return Arrays.asList("3\u0000ADD\u00002CJO3TD019CZ85ECT\u0000P0MFODNBACOGDS9NS\u0000\u0001",
                "3\u0000ADD\u0000M1U0BBC18AORHLLXZ\u0000DST8DI8TEGK1E6I3U\u0000\u0001",
                "3\u0000ADD\u0000MAYFNUS43T2WKTP25\u0000CREJ8MBZIQUFAW3AB\u0000\u0001",
                "3\u0000ADD\u0000UI1HZD7WLKJ9CKS0F\u0000JERMXBGKDFJLIX85Z\u0000\u0001",
                "3\u0000ADD\u0000178IGHLES0JYBC94P\u0000QFNBXRAL1IFFQH3T9\u0000\u0001",
                "3\u0000ADD\u0000GYD5NI0RMEPNWZWWN\u0000DN57YSBFNBSAE6CP3\u0000\u0001");
    }
}