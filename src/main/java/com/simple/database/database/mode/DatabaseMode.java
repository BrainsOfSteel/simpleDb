package com.simple.database.database.mode;

public class DatabaseMode {
    private static DatabaseMode databaseMode;  
    private ModeEnum modeEnum;
           
    public ModeEnum getModeEnum() {
        return modeEnum;
    }

    private DatabaseMode(ModeEnum modeEnum) {
        this.modeEnum = modeEnum;
    }  
       
    public static DatabaseMode getInstance(ModeEnum modeEnum) {      
        if(databaseMode == null){
            databaseMode  = new DatabaseMode(modeEnum);
        }

        return databaseMode;
    }

    public static DatabaseMode getInstance() {
        return databaseMode;
    } 
}
