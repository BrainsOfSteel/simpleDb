package com.simple.database.database.controller;

import com.simple.database.database.request.AddReplicaRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.simple.database.database.mode.DatabaseMode;
import com.simple.database.database.mode.ModeEnum;
import com.simple.database.database.request.AddRequest;
import com.simple.database.database.service.DatabaseService;

@RestController
public class MainController {
    
    @Autowired
    private DatabaseService databaseService;

    private DatabaseMode databaseMode = DatabaseMode.getInstance();

    @PostMapping(value = "/addKey", consumes = "application/json")
    public ResponseEntity<?> addKey(@RequestBody AddRequest request){
        if(databaseMode.getModeEnum() == ModeEnum.REPLICA){
            return new ResponseEntity<>(HttpStatus.FORBIDDEN);
        }
        try{
            databaseService.addKey(request.getKey(), request.getValue());
        }catch(Exception e){
            e.printStackTrace();
            return new ResponseEntity<>(e.getCause(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        
        return new ResponseEntity<>("Added successfully", HttpStatus.CREATED);
    }

    @PostMapping(value = "/addReplica", consumes = "application/json")
    public ResponseEntity<?> addReplica(@RequestBody AddReplicaRequest request){
        if(databaseMode.getModeEnum() != ModeEnum.MASTER){
            return new ResponseEntity<>(HttpStatus.FORBIDDEN);
        }
        try{
            databaseService.addReplica(request.getRestEndPoint(), request.getFileName());
        }catch(Exception e){
            e.printStackTrace();
            return new ResponseEntity<>(e.getCause(), HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>("Added successfully", HttpStatus.CREATED);
    }

    @GetMapping("/getKeyValue/{key}")
    public ResponseEntity<?> getKeyValue(@PathVariable("key") String key){
        if(databaseMode.getModeEnum() == ModeEnum.REPLICA){
            return new ResponseEntity<>(HttpStatus.FORBIDDEN);
        }
        String value = databaseService.getValue(key);
        return new ResponseEntity<>("value = "+value, HttpStatus.OK);
    }

    @DeleteMapping("/delKey/{key}")
    public ResponseEntity<?> delKey(@PathVariable("key") String key){
        if(databaseMode.getModeEnum() == ModeEnum.REPLICA){
            return new ResponseEntity<>(HttpStatus.FORBIDDEN);
        }

        try{
            databaseService.delKey(key);
        }catch(Exception e){
            e.printStackTrace();
            return new ResponseEntity<>(e.getCause(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        
        return new ResponseEntity<>(HttpStatus.OK);
    }
}
