package datawave.microservice.accumulo.config;

import com.google.common.collect.ImmutableMap;
import datawave.accumulo.util.security.UserAuthFunctions;
import datawave.marking.MarkingFunctions;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
@EnableConfigurationProperties(AccumuloProperties.class)
public class AccumuloConfiguration {
    
    private Logger log = LoggerFactory.getLogger(this.getClass());
    
    @Bean
    @ConditionalOnMissingBean
    public Connector connector(AccumuloProperties accumuloProperties) {
        
        log.debug("Creating a new accumulo connector for {}", accumuloProperties.getInstanceName());
        
        // TODO: Perhaps enable connection pooling and limit total Accumulo connections?
        //@formatter:off
        Map<String,String> conf = ImmutableMap.of(
            "instance.name", accumuloProperties.getInstanceName(),
            "instance.zookeeper.host", accumuloProperties.getZookeepers());
        //@formatter:on
        
        final Instance instance = new ZooKeeperInstance(ClientConfiguration.fromMap(conf));
        Connector connector = null;
        try {
            connector = instance.getConnector(accumuloProperties.getUsername(), new PasswordToken(accumuloProperties.getPassword()));
        } catch (Throwable t) {
            log.error("Unable to establish connection to Accumulo", t);
        }
        return connector;
    }
    
    @Bean
    @ConditionalOnMissingBean
    public MarkingFunctions markingFunctions() {
        return new MarkingFunctions.NoOp();
    }
    
    @Bean
    @ConditionalOnMissingBean
    public UserAuthFunctions userAuthFunctions() {
        return UserAuthFunctions.getInstance();
    }
}
