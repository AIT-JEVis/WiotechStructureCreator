/**
 * Copyright (C) 2015 Envidatec GmbH <info@envidatec.com>
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
 */
package org.jevis.structurecreator;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jevis.api.JEVisAttribute;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisConstants;
import org.jevis.api.JEVisDataSource;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisSample;
import org.jevis.api.JEVisType;
import org.jevis.api.sql.JEVisDataSourceSQL;
import org.joda.time.DateTime;

/**
 * Basic example for an system information reader working with the JEVis system.
 *
 * @author Florian Simon<florian.simon@envidatec.com>
 */
public class BasicExamples {

    /**
     * The JEVisDataSource is the central class handling the connection to the
     * JEVis Server
     */
    private JEVisDataSource jevis;

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
    public BasicExamples(String sqlServer, String port, String sqlSchema, String sqlUser, String sqlPW, String jevisUser, String jevisPW) {

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
    public void writeToJEVis(long objectID, String attributeName) {
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

                        //This is our sample (measured data)
                        long totalSpaceUsed = getUsedSpace();
                        DateTime timestamp = DateTime.now();

                        //Now we let the Attribute creates an JEVisSample,an JEVisSample allways need an Timestamp and an value.
                        JEVisSample newSample = attribute.buildSample(timestamp, totalSpaceUsed, "This is an note, imported via SysReader");
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
     * Print all (for the user) accessible JEVisObjects and their JEVisAttributes
     * which are from the given JEVisClass or an heir.
     *
     * @param jevisClass
     */
    public void printObjects(String jevisClass) {
        try {
            //Check if the connection is still alive. An JEVisException will be
            //thrown if you use one of the functions and the connection is lost
            if (jevis.isConnectionAlive()) {

                //Get the JEVisClass from the server
                if (jevis.getJEVisClass(jevisClass) != null) {
                    JEVisClass myClass = jevis.getJEVisClass(jevisClass);

                    //Get all JEVisObjects from the JEVisClass and all the inherit this class if the user has the userrights for them
                    List<JEVisObject> allObjects = jevis.getObjects(myClass, true);

                    for (JEVisObject myObject : allObjects) {
                        //Print the name and id of the object
                        Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "JEVisObject: [" + myObject.getID() + "] " + myObject.getName());

                        //Print for all attributes this object has the lastest value an timestamp
                        for (JEVisAttribute myAttribute : myObject.getAttributes()) {
                            Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO,
                                    "    JEVisAttribute: " + myAttribute.getName()
                                    + " [" + myAttribute.getLatestSample().getTimestamp() + "]"
                                    + " - " + myAttribute.getLatestSample().getValue());
                        }
                    }
                } else {
                    Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, "Requested JEVisClass dies not exist: " + jevisClass);
                }
            } else {
                Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, "Connection to the JEVisServer is not alive");
            }

        } catch (JEVisException ex) {
            Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     * Print informations about an JEVisClass
     *
     * @param className
     */
    public void printClass(String className) {
        try {
            //Check if the connection is still alive. An JEVisException will be thrown if you use one of the functions and the connection is lost
            if (jevis.isConnectionAlive()) {

                //Get the JEVisClass from the server
                if (jevis.getJEVisClass(className) != null) {
                    JEVisClass myClass = jevis.getJEVisClass(className);

                    //Print the unique name of this JEVisClass
                    Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "Name: " + myClass.getName());
                    //Print the JEVisClass unique status, if an class is unique 
                    //it is forbidden to create more than one object with this
                    //class under the same parent.
                    Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "isUnique: " + myClass.isUnique());

                    //Print all valid parent classes. This will be used to check
                    //if a JEVisObject can be created under another JEVisObject.
                    //This class can only be created under object from the following classes
                    Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "Valid Parents:");
                    for (JEVisClass allowedParents : myClass.getValidParents()) {
//                        printClass(allowedParents.getName());
                        Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "Name: " + allowedParents.getName());
                    }

                    //Print all JEVisTypes this JEVisClass has. A type is the
                    //rule which attributes a JEVisObject from this class has.
                    Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "Types:");
                    for (JEVisType type : myClass.getTypes()) {
                        Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "  Name: " + type.getName());
                        //The PrimitiveType tells the JEVis System what kind of
                        //value is expected (Int, Double,String etc..) SEE  JEVisConstants.PrimitiveType.*
                        Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "  Type: " + type.getPrimitiveType());
                        Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "  Discription: " + type.getDescription());
                        //If the Unit is not null the System expects a value
                        //in this unit, the sample will try to convert the input
                        //unit into this unit
                        Logger.getLogger(BasicExamples.class.getName()).log(Level.INFO, "  Unit: " + type.getUnit());
                    }

                } else {
                    Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, "Requested JEVisClass dies not exist: " + className);
                }
            } else {
                Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, "Connection to the JEVisServer is not alive");
            }

        } catch (JEVisException ex) {
            Logger.getLogger(BasicExamples.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     * Create an new JEVisObject on the JEVis Server.
     *
     * @param parentObjectID unique ID of the parent object where the new object
     * will be created under
     * @param newObjectClass The JEVisClass of the new JEVisObject.#
     * @param newObjectName The name of the new JEVisObject
     */
    public void createObject(long parentObjectID, String newObjectClass, String newObjectName) {
        try {
            //Check if the connection is still alive. An JEVisException will be
            //thrown if you use one of the functions and the connection is lost
            if (jevis.isConnectionAlive()) {

                //Get the ParentObject from the JEVis system
                if (jevis.getObject(parentObjectID) != null) {

                    JEVisObject parentObject = jevis.getObject(parentObjectID);
                    JEVisClass parentClass = parentObject.getJEVisClass();

                    //Get the JEVisClass we want our new JEVisObject to have
                    if (jevis.getJEVisClass(newObjectName) != null) {
                        JEVisClass newClass = jevis.getJEVisClass(newObjectName);

                        //Check if the JEVisObject with this class is allowed under a parent of the other Class
                        //it will also check if the JEVisClass is unique and if another object of the Class exist.
                        if (newClass.isAllowedUnder(parentClass)) {
                            JEVisObject newObject = parentObject.buildObject(newObjectName, newClass);
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
    }

    /**
     * Read all free disk space of the locale machine. This function is only a
     * simple source of data and has not much to do with the JEVis himself.
     *
     * @return total used space
     * @throws IOException
     */
    private long getUsedSpace() {
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
