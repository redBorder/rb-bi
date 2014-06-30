package net.redborder.state.gridgain;

/**
 * Created by andresgomez on 30/06/14.
 */
public class GridGainOptions {

    public GridGainOptions(){

    }


    public GridGainOptions(String cacheName, Integer cacheMode, Integer backups){
        this.backups=backups;
        this.cacheMode=cacheMode;
        this.cacheName=cacheName;
    }

    /*
        Cache mode:

          0 - LOCAL
          1 - REPLICATED
          2 - PARTITIONED
     */
    Integer cacheMode = 2;

    /*
        Cache name.
     */
    String cacheName = "default";

    /*
        Numbers of backups to can restore cached.
     */
    Integer backups = 0;
}
