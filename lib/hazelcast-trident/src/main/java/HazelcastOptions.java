import java.util.List;

/**
 * Created by andresgomez on 28/06/14.
 */
public class HazelcastOptions {

    List<String> addressHazelcastNodes;

    String groupName = "storm-name";
    String groupPassword = "storm-pass";
    String mapName = "storm-map";



    boolean existsHazelcastCluster = true;

    /*
           If you need a cluster you can configure this too.
           More info on hazelcast.org
     */

    int backupCount=0;
    int AsynbackupCount=1;
    String memoryFormat = "BINARY"; // BINARY or OBJECT
}
