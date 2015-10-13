/**
 * Copyright (C) 2015 Werner Lamprecht
 *
 * This file is part of JEVisExample.
 *
 * JEVisExample is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation in version 3.
 *
 * JEVisExample is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * JEVisExample. If not, see <http://www.gnu.org/licenses/>.
 *
 * JEVisExample is part of the OpenJEVis project, further project information
 * are published at <http://www.OpenJEVis.org/>.
 *
 * @author Werner Lamprecht <werner.lamprecht@ymail.com>
 * 
 */




package org.jevis.structurecreator;

import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jevis.api.JEVisAttribute;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisDataSource;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisSample;
import org.jevis.api.sql.JEVisDataSourceSQL;
import org.jevis.commons.unit.JEVisUnitImp;
import org.joda.time.DateTime;


public class WiotechStructureCreator {

    
    
    protected static Connection _con;
    private static String _host = "10.8.4.123";
    private static Integer _port = 3306;
    private static String _schema = "db_lm_cbv2";
    private static String _dbUser = "jevis";
    private static String _dbPW = "jevistest";
    private static final Long BUILDING_ID = 3959l;
    
    static List<Sensor> _result = new ArrayList<>();
    
    
    /**
     * The JEVisDataSource is the central class handling the connection to the
     * JEVis Server
     */
    private static JEVisDataSource jevis;
    
    
    
    /**
     * Main class to start the examples.
     *
     *
     * @param args
     * @throws JEVisException
     */
    public static void main(String[] args) throws JEVisException {
       String url = null;
        try {
            url = loadJDBC(_host, _port, _schema, _dbUser, _dbPW);
            
        } catch (ClassNotFoundException | SQLException ex) {
            java.util.logging.Logger.getLogger(WiotechStructureCreator.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            return;
        }
        
        
        
        String sql_query = "select macs.MACAddr, macs.NwkAddr, tabs.TABLE_NAME " +
                            "from db_lm_cbv2._cbv2_macnwkaddr as macs " +
                            "LEFT JOIN information_schema.tables AS tabs " +
                            "ON tabs.TABLE_NAME like CONCAT(CONCAT('sensor\\_', macs.MACAddr), '\\_%')" +
                            ";";
        
        try {
            PreparedStatement ps = _con.prepareStatement(sql_query);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                if(rs.getString(3)!=null){
                    String[] sensorDetails = rs.getString(3).split("_");
                    boolean add = _result.add(new Sensor(sensorDetails[1], sensorDetails[2], rs.getString(3)));
                }
            }
        } catch (SQLException ex) {
                Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.SEVERE, null, ex);
        }

        
         
            
            
            //create an new Example, this example has hardcodet connection settings.
            //Change these settings to your server configuration
            basicExamples("localhost", "3306", "jevis", "jevis", "jevistest", "Wiotech", "wiotech");
            
            //Example which prints all Objects which are from the JEVisClass
            //given by its name (in this case "Data")
            //example.printObjects("Data");
            
            //Example which prints some information about the given JEVisClass
            //(in this case "Email Plugin")
            //example.printClass("Email Plugin");
            
            //Example of writing new values to the JEVis system. The parameters are
            //the unique id of the object and its JEVisAttribute name where the data
            //should be stored
            //example.writeToJEVis(1588l, "Value");
            
            //Example of creating an new JEVisObject in the JEVis System.
            // - 1587 is the parent Object,
            // - "Data" is the JEVisClass of the new Object,
            // - "My new Data Object" is the name
            JEVisObject dsd =createObject(BUILDING_ID, "Data Source Directory", "Data Source Directory");
            
            
            JEVisObject mysqlServer = createObject(dsd.getID(), "MySQL Server", "MySQL Server");
            writeToJEVis(mysqlServer.getID(), "Schema", "db_lm_cbv2");
            writeToJEVis(mysqlServer.getID(), "User", "jevis");
            writeToJEVis(mysqlServer.getID(), "Port", "3306");
            writeToJEVis(mysqlServer.getID(), "Host", "10.8.4.123");
            writeToJEVis(mysqlServer.getID(), "Password", "jevistest");
            
            JEVisObject dataDirectory = createObject(BUILDING_ID, "Data Directory", "Data Directory");
            
            for(Sensor sensor : _result){
                JEVisObject sqlChannelDir = createObject(mysqlServer.getID(),"SQL Channel Directory", sensor.getName());
                JEVisObject channel = createObject(sqlChannelDir.getID(),"SQL Channel", "SQL Channel");
                writeToJEVis(channel.getID(), "Column Timestamp", "time");
                writeToJEVis(channel.getID(), "Column Value", "value");
                writeToJEVis(channel.getID(), "Table",sensor.getTable());
                writeToJEVis(channel.getID(), "Timestamp Format", "yyyy-MM-dd HH:mm:ss.s");
                
                
                JEVisObject sqlDPD = createObject(channel.getID(), "SQL Data Point Directory", "DPD");
                JEVisObject sqlDP = createObject(sqlDPD.getID(), "SQL Data Point", "DP");
                JEVisObject data = createObject(dataDirectory.getID(), "Data", sensor.getName());
                writeToJEVis(sqlDP.getID(), "Target", data.getID().toString());
                

            }        
                 
            
    }
    
    
    public static String loadJDBC(String host, int port, String schema, String dbUser, String dbPW) throws ClassNotFoundException, SQLException {
        
        String url = "jdbc:mysql://" + host + ":" + port + "/" + schema + "?";
        Class.forName("com.mysql.jdbc.Driver");
        _con = DriverManager.getConnection(url, dbUser, dbPW);
        
        return url;
    }
    
     /**
     * Create an new JEVisObject on the JEVis Server.
     *
     * @param parentObjectID unique ID of the parent object where the new object
     * will be created under
     * @param newObjectClass The JEVisClass of the new JEVisObject.#
     * @param newObjectName The name of the new JEVisObject
     */
    public static JEVisObject createObject(long parentObjectID, String newObjectClass, String newObjectName) {
        JEVisObject newObject = null;
        try {
            //Check if the connection is still alive. An JEVisException will be
            //thrown if you use one of the functions and the connection is lost
            if (jevis.isConnectionAlive()) {

                //Get the ParentObject from the JEVis system
                if (jevis.getObject(parentObjectID) != null) {

                    JEVisObject parentObject = jevis.getObject(parentObjectID);
                    JEVisClass parentClass = parentObject.getJEVisClass();

                    //Get the JEVisClass we want our new JEVisObject to have
                    if (jevis.getJEVisClass(newObjectClass) != null) {
                        JEVisClass newClass = jevis.getJEVisClass(newObjectClass);

                        //Check if the JEVisObject with this class is allowed under a parent of the other Class
                        //it will also check if the JEVisClass is unique and if another object of the Class exist.
                        if (newClass.isAllowedUnder(parentClass)) {
                            newObject = parentObject.buildObject(newObjectName, newClass);
                            newObject.commit();
                            Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "New ID: " + newObject.getID());
                        } else {
                            Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, "Cannot create Object because the parent JEVisClass does not allow the child");
                        }
                    }

                } else {
                    Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, "Cannot create Object because the parent is not accessible");
                }

            } else {
                Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, "Connection to the JEVisServer is not alive");
            }

        } catch (JEVisException ex) {
            Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, null, ex);
        }
        return newObject;
    }
    
    /**
     * Create an new SystemReader an connect to the JEVis Server.
     *
     * TODO: implement an neutral JEVisDataSource constructor which can work with
     * JEAPI-SQl and JEAPI-WS and other implementations. This will be part of the
     * JECommons library.
     *
     * @param sqlServer Address of the MySQL Server
     * @param port Port of the MySQL Server, Default is 3306
     * @param sqlSchema Database schema of the JEVis database
     * @param sqlUser MySQl user for the connection
     * @param sqlPW MySQL password for the connection
     * @param jevisUser Username of the JEVis user
     * @param jevisPW Password of the JEVis user
     */
    public static void basicExamples(String sqlServer, String port, String sqlSchema, String sqlUser, String sqlPW, String jevisUser, String jevisPW) {

        try {
            //Create an new JEVisDataSource from the MySQL implementation 
            //JEAPI-SQl. This connection needs an vaild user on the MySQl Server.
            //Later it will also be possible to use the JEAPI-WS and by this 
            //using the JEVis webservice (REST) as an endpoint which is much
            //saver than using a public SQL-port.
            jevis = new JEVisDataSourceSQL(sqlServer, port, sqlSchema, sqlUser, sqlPW);

            //authentificate the JEVis user.
            if (jevis.connect(jevisUser, jevisPW)) {
                Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "Connection was successful");
            } else {
                Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "Connection was not successful, exiting app");
                System.exit(1);
            }

        } catch (JEVisException ex) {
            Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, "There was an error while connecting to the JEVis Server");
            Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }

    }
    
     /**
     * Write the current used disk-space into the JEVis System.
     *
     * TODO: Add an example for the exception handling of a lost connection
     *
     * TODO: Check the expected type(Integer,Double,String...) of the value of an
     * attribute. The API will throw a warning if the types do not match.
     *
     * TODO: make an example with a JEVisSample with unit.
     *
     * @param objectID unique ID of the JEVisObject on the Server.
     * @param attributeName unique name of the Attribute under this Object
     */
    public static void writeToJEVis(long objectID, String attributeName, Object value ) {
        try {
            //Check if the connection is still alive. An JEVisException will be
            //thrown if you use one of the functions and the connection is lost
            if (jevis.isConnectionAlive()) {

                //Get the JEVisObject with the given ID. You can get the uniqe
                //ID with the help of JEConfig.
                if (jevis.getObject(objectID) != null) {
                    JEVisObject myObject = jevis.getObject(objectID);
                    Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "JEVisObject: " + myObject);

                    //Get the JEVisAttribute by its unique identifier.
                    if (myObject.getAttribute(attributeName) != null) {
                        JEVisAttribute attribute = myObject.getAttribute(attributeName);
                        Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "JEVisAttribute: " + attribute);

                        
                        DateTime timestamp = DateTime.now();

                        //Now we let the Attribute creates an JEVisSample,an JEVisSample allways need an Timestamp and an value.
                        JEVisSample newSample = attribute.buildSample(timestamp, value, "This is an note, imported via SysReader");
                        //Until now we created the sample only localy and we have to commit it to the JEVis Server.
                        newSample.commit();

                        //TODO: we need an example for attribute.addSamples(listOfSamples); function. This function allows to commit a bunch of sample at once
                    } else {
                        Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, "Could not found the Attribute with the name:" + attributeName);
                    }
                } else {
                    Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, "Could not found the Object with the id:" + objectID);
                }
            } else {
                Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, "Connection to the JEVisServer is not alive");
                //TODO: the programm could now retry to connect,
                //We dont have to do the isConnectionAlive() but use the JEVisException to handle this problem.
            }
        } catch (JEVisException ex) {
            Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

/**
     * Read all free disk space of the locale machine. This function is only a
     * simple source of data and has not much to do with the JEVis himself.
     *
     * @return total used space
     * @throws IOException
     */
    private static long getUsedSpace() {
        NumberFormat nf = NumberFormat.getNumberInstance();
        long total = 0;
        for (Path root : FileSystems.getDefault().getRootDirectories()) {
            System.out.print(root + ": ");

            try {
                FileStore store = Files.getFileStore(root);
                System.out.println("available=" + nf.format(store.getUsableSpace()) + ", total=" + nf.format(store.getTotalSpace()));
                total += store.getTotalSpace();
            } catch (Exception ex) {
                Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, "There was an error while reading the free space");
                Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return total;
    }

}
