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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
import org.joda.time.DateTime;


public class WiotechStructureCreator {

    private static List<Sensor> _result = new ArrayList<>();
    protected static Connection _con;
    private static String _host;
    private static Integer _port;
    private static String _schema;
    private static String _dbUser;
    private static String _dbPW;
    
    
    /**
     * The JEVisDataSource is the central class handling the connection to the
     * JEVis Server
     */
    private static JEVisDataSource jevis;
    
    /**
     * 
     * Reads the sensor details from the mysql db
     * 
     * @param host wiotech local host ip 
     * @param port wiotech db port
     * @param schema db schema
     * @param dbUser user
     * @param dbPW password
     * 
     *
     *   public static void main(String[] args){
     *       WiotechStructureCreator wsc = new WiotechStructureCreator("10.8.4.123", 3306, "db_lm_cbv2", "jevis", "jevistest");
     *       wsc.connectToJEVis("localhost", "3306", "jevis", "jevis", "jevistest", "Wiotech", "wiotech");
     *       wsc.createStructure(3959l);
     *   }
     * 
     */
    public WiotechStructureCreator(String host, Integer port, String schema, String dbUser, String dbPW) {
        
        this._host = host;
        this._port = port;
        this._schema = schema;
        this._dbUser = dbUser;
        this._dbPW = dbPW;
        try {
            String url = loadJDBC(_host, _port, _schema, _dbUser, _dbPW);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
        getSensorDetails();
    }
    
     /**
     * Example how to use WiotechStructureCreator
     *
     * @param args not used
     */
    public static void main(String[] args){
        WiotechStructureCreator wsc = new WiotechStructureCreator("10.8.4.123", 3306, "db_lm_cbv2", "jevis", "jevistest");
        wsc.connectToJEVis("localhost", "3306", "jevis", "jevis", "jevistest", "Wiotech", "wiotech");
        wsc.createStructure(3959l);
    }
    
    /**
     * 
     * Creates the needed JEVis structure
     * 
     * @param buildingId building node id, where the structure has to be created
     * 
     * TODO Enable mysql server
     * TODO Add units
     */
    public void createStructure(long buildingId){
        
        JEVisObject dsd =createObject(buildingId, "Data Source Directory", "Data Source Directory");
             
        JEVisObject mysqlServer = createObject(dsd.getID(), "MySQL Server", "MySQL Server");
        writeToJEVis(mysqlServer.getID(), "Schema", _schema);
        writeToJEVis(mysqlServer.getID(), "User", _dbUser);
        writeToJEVis(mysqlServer.getID(), "Port", _port);
        writeToJEVis(mysqlServer.getID(), "Host", _host);
        writeToJEVis(mysqlServer.getID(), "Password", _dbPW);
            
        JEVisObject dataDirectory = createObject(buildingId, "Data Directory", "Data Directory");
            
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
    
    /**
     * 
     * Reads the sensor details from the wiotech local manager db and stores it in a List of sensor types
     * 
     */
    private void getSensorDetails(){
        try {
            
           
            String sql_query = "select macs.MACAddr, macs.NwkAddr, tabs.TABLE_NAME " +
                            "from db_lm_cbv2._cbv2_macnwkaddr as macs " +
                            "LEFT JOIN information_schema.tables AS tabs " +
                            "ON tabs.TABLE_NAME like CONCAT(CONCAT('sensor\\_', macs.MACAddr), '\\_%')" +
                            ";";
            
            PreparedStatement ps = _con.prepareStatement(sql_query);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                if(rs.getString(3)!=null){
                    String[] sensorDetails = rs.getString(3).split("_");
                    boolean add = _result.add(new Sensor(sensorDetails[1], sensorDetails[2], rs.getString(3)));
                }
            }
            
        } catch (SQLException ex) {
            java.util.logging.Logger.getLogger(WiotechStructureCreator.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            return;
        }
        
    }
    
    /**
     * Load appropriate jdbc driver and set protected SQL-connection _con
     * @param host Hostname or IP of the SQL-database to connect to
     * @param port the used TCP-port
     * @param schema Database/Schema name
     * @param dbUser User used to connect to the SQL-database
     * @param dbPW Password of the user
     * @return URL used to connect to the database, for debugging
     * @throws ClassNotFoundException
     * @throws SQLException 
     */
    private static String loadJDBC(String host, int port, String schema, String dbUser, String dbPW) throws ClassNotFoundException, SQLException {
        
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
     * @param newObjectClass The JEVisClass of the new JEVisObject
     * @param newObjectName The name of the new JEVisObject
     */
    private static JEVisObject createObject(long parentObjectID, String newObjectClass, String newObjectName) {
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
                            Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.INFO, "New ID: " + newObject.getID());
                        } else {
                            Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.SEVERE, "Cannot create Object because the parent JEVisClass does not allow the child");
                        }
                    }

                } else {
                    Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.SEVERE, "Cannot create Object because the parent is not accessible");
                }

            } else {
                Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.SEVERE, "Connection to the JEVisServer is not alive");
            }

        } catch (JEVisException ex) {
            Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
        return newObject;
    }
    
    /**
     * 
     * Connect to JEVis
     *
     * @param sqlServer Address of the MySQL Server
     * @param port Port of the MySQL Server, Default is 3306
     * @param sqlSchema Database schema of the JEVis database
     * @param sqlUser MySQl user for the connection
     * @param sqlPW MySQL password for the connection
     * @param jevisUser Username of the JEVis user
     * @param jevisPW Password of the JEVis user
     */
    public void connectToJEVis(String sqlServer, String port, String sqlSchema, String sqlUser, String sqlPW, String jevisUser, String jevisPW) {

        try {
            //Create an new JEVisDataSource from the MySQL implementation 
            //JEAPI-SQl. This connection needs an vaild user on the MySQl Server.
            //Later it will also be possible to use the JEAPI-WS and by this 
            //using the JEVis webservice (REST) as an endpoint which is much
            //saver than using a public SQL-port.
            jevis = new JEVisDataSourceSQL(sqlServer, port, sqlSchema, sqlUser, sqlPW);

            //authentificate the JEVis user.
            if (jevis.connect(jevisUser, jevisPW)) {
                Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.INFO, "Connection was successful");
            } else {
                Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.INFO, "Connection was not successful, exiting app");
                System.exit(1);
            }

        } catch (JEVisException ex) {
            Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.SEVERE, "There was an error while connecting to the JEVis Server");
            Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }

    }
    
     /**
     *
     * set a node attribute 
     *
     * @param objectID unique ID of the JEVisObject on the Server.
     * @param attributeName unique name of the Attribute under this Object
     * @param value and its value
     *
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
                    Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.INFO, "JEVisObject: " + myObject);

                    //Get the JEVisAttribute by its unique identifier.
                    if (myObject.getAttribute(attributeName) != null) {
                        JEVisAttribute attribute = myObject.getAttribute(attributeName);
                        Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.INFO, "JEVisAttribute: " + attribute);

                        DateTime timestamp = DateTime.now();

                        //Now we let the Attribute creates an JEVisSample,an JEVisSample allways need an Timestamp and an value.
                        JEVisSample newSample = attribute.buildSample(timestamp, value, "This is an note, imported via SysReader");
                        //Until now we created the sample only localy and we have to commit it to the JEVis Server.
                        newSample.commit();

                        //TODO: we need an example for attribute.addSamples(listOfSamples); function. This function allows to commit a bunch of sample at once
                    } else {
                        Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.SEVERE, "Could not found the Attribute with the name:" + attributeName);
                    }
                } else {
                    Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.SEVERE, "Could not found the Object with the id:" + objectID);
                }
            } else {
                Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.SEVERE, "Connection to the JEVisServer is not alive");
                //TODO: the programm could now retry to connect,
                //We dont have to do the isConnectionAlive() but use the JEVisException to handle this problem.
            }
        } catch (JEVisException ex) {
            Logger.getLogger(WiotechStructureCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
