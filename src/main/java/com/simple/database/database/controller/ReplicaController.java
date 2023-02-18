package com.simple.database.database.controller;

import com.simple.database.database.mode.DatabaseMode;
import com.simple.database.database.mode.ModeEnum;
import com.simple.database.database.replica.engine.DatabaseEngine;
import com.simple.database.database.replica.request.ReplicaRequest;
import com.simple.database.database.request.AddRequest;
import com.simple.database.database.service.ReplicaDatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class ReplicaController {

    @Autowired
    private ReplicaDatabaseService databaseService;

    private DatabaseMode databaseMode  = DatabaseMode.getInstance();


    @GetMapping("/replica/getKeyValue/{key}")
    public ResponseEntity<?> getKeyValue(@PathVariable("key") String key){
        if(databaseMode.getModeEnum() != ModeEnum.REPLICA){
            return new ResponseEntity<>(HttpStatus.FORBIDDEN);
        }
        String value = databaseService.getValue(key);
        return new ResponseEntity<>("value = "+value, HttpStatus.OK);
    }

    @PostMapping(value = "/replica/addKey", consumes = "application/json")
    public ResponseEntity<?> addKey(@RequestBody ReplicaRequest request){
        if(databaseMode.getModeEnum() != ModeEnum.REPLICA){
            return new ResponseEntity<>(HttpStatus.FORBIDDEN);
        }
        try{
            System.out.println("request received = "+request);
        }catch(Exception e){
            e.printStackTrace();
            return new ResponseEntity<>(e.getCause(), HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>("Added successfully", HttpStatus.CREATED);
    }
}
