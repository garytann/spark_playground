package org.example;
import java.io.Serializable;


public class LocationRankingModel implements Serializable {
    private String geographical_Location;
    private String itemName;
    private int rank;

    public String getGeographicalLocation() {
        return geographical_Location;
    }

    public void setGeographicalLocation(String geographical_Location) {
        this.geographical_Location = geographical_Location;
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public int getRank() {
        return rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }
}
