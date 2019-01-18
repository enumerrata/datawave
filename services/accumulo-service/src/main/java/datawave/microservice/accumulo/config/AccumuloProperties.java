package datawave.microservice.accumulo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "accumulo")
public class AccumuloProperties {
    
    private String zookeepers;
    private String instanceName;
    private String username;
    private String password;
    
    public String getZookeepers() {
        return zookeepers;
    }
    
    public void setZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
    }
    
    public String getInstanceName() {
        return instanceName;
    }
    
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }
    
    public String getUsername() {
        return username;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
}
