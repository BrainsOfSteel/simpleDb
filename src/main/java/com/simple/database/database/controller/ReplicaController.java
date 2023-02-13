package com.simple.database.database.controller;

import com.simple.database.database.mode.DatabaseMode;
import com.simple.database.database.mode.ModeEnum;
import com.simple.database.database.replica.engine.DatabaseEngine;
import com.simple.database.database.service.ReplicaDatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController("/replica")
public class ReplicaController {

    @Autowired
    private ReplicaDatabaseService databaseService;

    private DatabaseMode databaseMode  = DatabaseMode.getInstance();


    @GetMapping("/getKeyValue/{key}")
    public ResponseEntity<?> getKeyValue(@PathVariable("key") String key){
        if(databaseMode.getModeEnum() == ModeEnum.REPLICA){
            return new ResponseEntity<>(HttpStatus.FORBIDDEN);
        }
        String value = databaseService.getValue(key);
        return new ResponseEntity<>("value = "+value, HttpStatus.OK);
    }
}
