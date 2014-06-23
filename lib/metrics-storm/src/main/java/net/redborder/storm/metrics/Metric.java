package net.redborder.storm.metrics;

/**
 * Created by andresgomez on 23/06/14.
 */
public class Metric {
    String name;
    String worker;
    String component;
    Integer port;
    Integer taskId;
    Object value;

    public Metric(String name, String worker, Integer port, String component, Integer taskId, Object value) {
        this.name = name;
        this.value = value;
        this.worker = worker;
        this.port = port;
        this.component = component;
        this.taskId = taskId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Metric other = (Metric) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (value != other.value)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Metric [name=" + name + ", value=" + value + "]";
    }
}
