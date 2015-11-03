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
 */
package org.jevis.structurecreator;

/**
 *
 * @author Werner Lamprecht <werner.lamprecht@ymail.com>
 */
public class Sensor {

    private String name;
    //private String prefix;
    private String symbol;
    private String unit;
    private String table;
    private String mac;

    
    public Sensor(String wiotechSensorableName) {
        
        String[] sensorDetails = wiotechSensorableName.split("_");
        this.unit = convertAndSetUnit(sensorDetails[2]);
        this.mac = sensorDetails[1];
        this.table = wiotechSensorableName;
        this.name = mac;
        
    }
   
    public String getTable(){
        return table;
    }
    


    public String getName() {
        return name;
    }

    private void setName(String name) {

        this.name = name;
    }

    /*public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }*/

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String unitSymbol) {
        this.symbol = symbol;
    }

    public String getUnit() {
        return unit;
    }

    private String convertAndSetUnit(String wiotechUnitNumber){
        switch(wiotechUnitNumber){
            case "1": //°C
                this.symbol="Temp";
                this.unit = "\u00b0C";
                return this.getUnit();
            case "6"://%
                this.symbol="CO2";
                this.unit = "\u0025";
                return  this.getUnit();
            case "9":// ‰
                this.symbol="rH";
                this.unit = "\u0025";
                return this.getUnit();    
                
            default:
                return null;
        }
        
    }

    /**
     * @return the mac
     */
    public String getMac() {
        return mac;
    }
}
