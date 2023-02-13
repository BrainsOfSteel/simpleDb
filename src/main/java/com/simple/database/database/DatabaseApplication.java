package com.simple.database.database;

import com.simple.database.database.replica.LogAwareWriteAheadReader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.simple.database.database.master.ReplicaAwareWriteAheadLog;
import com.simple.database.database.master.engine.DatabaseEngine;
import com.simple.database.database.mode.DatabaseMode;
import com.simple.database.database.mode.ModeEnum;

@SpringBootApplication
public class DatabaseApplication {
	public static void main(String[] args) {

		String fileName;
		if(args[0].equals(ModeEnum.MASTER.name())){
			DatabaseMode.getInstance(ModeEnum.MASTER);
			fileName = "walMaster.log";
			ReplicaAwareWriteAheadLog replicaAwareWriteAheadLog = ReplicaAwareWriteAheadLog.getInstance(fileName);
			DatabaseEngine.getInstance(replicaAwareWriteAheadLog);
		}
		else{
			DatabaseMode.getInstance(ModeEnum.REPLICA);
			fileName = "walReplica.log";
			LogAwareWriteAheadReader logAwareWriteAheadReader = LogAwareWriteAheadReader.getInstance(fileName);
			com.simple.database.database.replica.engine.DatabaseEngine.getInstance(logAwareWriteAheadReader);
		}

		SpringApplication.run(DatabaseApplication.class, args);
	}
}
