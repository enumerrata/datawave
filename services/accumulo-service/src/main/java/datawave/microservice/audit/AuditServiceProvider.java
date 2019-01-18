package datawave.microservice.audit;

import com.google.common.base.Preconditions;
import datawave.microservice.audit.config.AuditServiceProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;

import java.net.URI;
import java.util.List;

/**
 * Provides a {@link ServiceInstance} representing the remote audit service
 */
public class AuditServiceProvider {
    
    private static Logger logger = LoggerFactory.getLogger(AuditServiceProvider.class);
    
    protected final AuditServiceProperties properties;
    protected final DiscoveryClient discoveryClient;
    
    protected AuditServiceProvider() {
        this.properties = null;
        this.discoveryClient = null;
    }
    
    public AuditServiceProvider(AuditServiceProperties properties) {
        this(properties, null);
    }
    
    public AuditServiceProvider(AuditServiceProperties properties, DiscoveryClient client) {
        Preconditions.checkNotNull(properties, "AuditServiceProperties argument is null");
        this.properties = properties;
        this.discoveryClient = client;
    }
    
    /**
     * If internal {@link DiscoveryClient} is null, returns the configured default service instance, otherwise the audit service will be discovered
     * automatically
     * 
     * @return {@link ServiceInstance} representing the remote audit service
     */
    public ServiceInstance getServiceInstance() {
        if (null == this.discoveryClient) {
            // Discovery disabled
            if (logger.isDebugEnabled()) {
                logger.debug("Discovery disabled. Returning default ServiceInstance");
            }
            return getDefaultServiceInstance();
        }
        return discoverInstance(properties.getServiceId());
    }
    
    protected ServiceInstance discoverInstance(String serviceId) {
        if (logger.isDebugEnabled()) {
            logger.debug("Locating audit server by id (" + serviceId + ") via discovery");
        }
        List<ServiceInstance> instances = this.discoveryClient.getInstances(serviceId);
        if (instances.isEmpty()) {
            throw new IllegalStateException("No instances found of audit server (" + serviceId + ")");
        }
        if (instances.size() > 1) {
            logger.info("More than one audit server is available, but I only know how to select the first in the list");
        }
        ServiceInstance instance = instances.get(0);
        if (logger.isDebugEnabled()) {
            logger.debug("Located audit server (" + serviceId + ") via discovery: " + instance);
        }
        return instance;
    }
    
    protected ServiceInstance getDefaultServiceInstance() {
        Preconditions.checkNotNull(properties, "properties must not be null");
        final URI uri = URI.create(properties.getUri());
        return new DefaultServiceInstance(properties.getServiceId(), uri.getHost(), uri.getPort(), uri.getScheme().equals("https"));
    }
}
