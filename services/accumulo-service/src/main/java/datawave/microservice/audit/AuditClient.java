package datawave.microservice.audit;

import com.google.common.base.Preconditions;
import datawave.marking.SecurityMarking;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.util.ProxiedEntityUtils;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.common.audit.Auditor.AuditType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Collection;

/**
 * Simple rest client for submitting requests to the audit service
 *
 * @see Request
 * @see AuditServiceProvider
 */
@Service
@ConditionalOnProperty(name = "audit.enabled", havingValue = "true")
public class AuditClient {
    
    private static final String DEFAULT_REQUEST_PATH = "/v1/audit";
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    @Autowired
    private AuditServiceProvider serviceProvider;
    
    private final JWTRestTemplate jwtRestTemplate;
    
    public AuditClient(RestTemplateBuilder builder) {
        this.jwtRestTemplate = builder.build(JWTRestTemplate.class);
    }
    
    public void submit(Request request) {
        submit(request, DEFAULT_REQUEST_PATH);
    }
    
    public void submit(Request request, String requestPath) {
        
        Preconditions.checkNotNull(request, "request cannot be null");
        Preconditions.checkNotNull(requestPath, "requestPath cannot be null");
        
        if (log.isDebugEnabled()) {
            log.debug("Received audit request: " + request.getAuditParameters().toString());
        }
        
        if (AuditType.NONE.equals(request.getAuditType())) {
            // Avoiding network resource waste in this case
            if (log.isDebugEnabled()) {
                log.debug("Received audit request, but AuditType was " + AuditType.NONE);
            }
        } else {
            //@formatter:off
            ServiceInstance auditService = serviceProvider.getServiceInstance();
            UriComponents uri = UriComponentsBuilder.fromUri(auditService.getUri())
                .path(auditService.getServiceId() + requestPath)
                .queryParams(request.getAuditParametersAsMap())
                .build();

            if (log.isDebugEnabled()) {
                log.debug("Audit request URI: " + uri);
            }

            ResponseEntity<String> response = jwtRestTemplate.exchange(
                request.userDetails, HttpMethod.POST, uri, String.class);

            if (response.getStatusCode().value() != HttpStatus.OK.value()) {
                throw new RuntimeException(String.format("Audit request failed. Http Status: (%s, %s)",
                    response.getStatusCodeValue(),
                    response.getStatusCode().getReasonPhrase()));
            }
            //@formatter:on
        }
    }
    
    /**
     * Audit request for a given query
     *
     * @see Request.Builder
     */
    public static class Request {
        
        static final String INTERNAL_AUDIT_PARAM_PREFIX = "audit.";
        
        private final Logger log = LoggerFactory.getLogger(this.getClass());
        
        private AuditParameters auditParameters;
        private MultiValueMap<String,String> paramMap;
        private ProxiedUserDetails userDetails;
        
        private Request() {}
        
        protected Request(Builder b) {
            
            this.userDetails = b.proxiedUserDetails;
            
            final DatawaveUser dwUser = b.proxiedUserDetails.getPrimaryUser();
            final MultiValueMap<String,String> params = new LinkedMultiValueMap<>(b.params);
            
            // Remove internal audit-related params, in case those were passed in
            params.entrySet().removeIf(entry -> entry.getKey().startsWith(INTERNAL_AUDIT_PARAM_PREFIX));
            
            params.add(AuditParameters.QUERY_STRING, b.baseQuery);
            params.add(AuditParameters.QUERY_AUTHORIZATIONS, dwUser.getAuths().toString());
            params.set(AuditParameters.QUERY_AUDIT_TYPE, b.auditType.name());
            params.set(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, b.marking.toColumnVisibilityString());
            params.set(AuditParameters.USER_DN, dwUser.getDn().toString());
            params.set(AuditParameters.QUERY_LOGIC_CLASS, b.logicClass);
            
            this.auditParameters = new AuditParameters();
            this.auditParameters.validate(params);
            this.paramMap = params;
        }
        
        public AuditParameters getAuditParameters() {
            return auditParameters;
        }
        
        public Auditor.AuditType getAuditType() {
            return auditParameters.getAuditType();
        }
        
        public ProxiedUserDetails getUserDetails() {
            return userDetails;
        }
        
        private MultiValueMap<String,String> getAuditParametersAsMap() {
            return paramMap;
        }
        
        /**
         * Builder for base audit requests
         */
        public static class Builder {
            
            protected String baseQuery;
            protected String logicClass;
            protected Auditor.AuditType auditType;
            protected MultiValueMap<String,String> params;
            protected SecurityMarking marking;
            protected ProxiedUserDetails proxiedUserDetails;
            
            public Builder withBaseQuery(String baseQuery) {
                this.baseQuery = baseQuery;
                return this;
            }
            
            public Builder withLogicClass(String logicClass) {
                this.logicClass = logicClass;
                return this;
            }
            
            public Builder withAuditType(Auditor.AuditType auditType) {
                this.auditType = auditType;
                return this;
            }
            
            public Builder withParams(MultiValueMap<String,String> params) {
                this.params = params;
                return this;
            }
            
            public Builder withProxiedUserDetails(ProxiedUserDetails proxiedUserDetails) {
                this.proxiedUserDetails = proxiedUserDetails;
                return this;
            }
            
            public Builder withMarking(SecurityMarking marking) {
                this.marking = marking;
                return this;
            }
            
            public Request build() {
                return new Request(this);
            }
        }
    }
}
