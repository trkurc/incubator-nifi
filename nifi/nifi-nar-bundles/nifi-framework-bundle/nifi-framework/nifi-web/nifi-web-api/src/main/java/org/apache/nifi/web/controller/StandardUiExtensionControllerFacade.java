/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.web.controller;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.nifi.action.Action;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.web.security.user.NiFiUserDetails;
import org.apache.nifi.web.security.user.NiFiUserUtils;
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.util.WebUtils;
import org.apache.nifi.ui.extension.UiExtensionType;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.web.ClusterRequestException;
import org.apache.nifi.web.ComponentConfiguration;
import org.apache.nifi.web.ConfigurationSnapshot;
import org.apache.nifi.web.InvalidRevisionException;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.util.ClientResponseUtils;

/**
 * Implements the UiExtensionControllerFacade interface to supply a simplified 
 * interface over a NiFi controller in both clustered and standalone environments.
 */
public class StandardUiExtensionControllerFacade implements UiExtensionControllerFacade {

    private static final Logger logger = LoggerFactory.getLogger(StandardUiExtensionControllerFacade.class);
    
    private static final String CLIENT_ID_PARAM = "clientId";
    private static final String VERBOSE_PARAM = "verbose";

    private NiFiProperties properties;
    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private ControllerServiceLookup controllerServiceLookup;
    private AuditService auditService;

    @Override
    public ControllerService getControllerService(String serviceIdentifier) {
        return controllerServiceLookup.getControllerService(serviceIdentifier);
    }

    @Override
    @PreAuthorize("hasAnyRole('ROLE_DFM')")
    public void saveActions(final Collection<Action> actions) {
        Objects.requireNonNull(actions, "Actions cannot be null.");

        // - when running standalone or cluster ncm - actions from custom UIs are stored locally
        // - clustered nodes do not serve custom UIs directly to users so they should never be invoking this method

        try {
            // record the operations
            auditService.addActions(actions);
        } catch (Throwable t) {
            logger.warn("Unable to record actions: " + t.getMessage());
            if (logger.isDebugEnabled()) {
                logger.warn(StringUtils.EMPTY, t);
            }
        }
    }

    @Override
    public String getCurrentUserDn() {
        String userDn = NiFiUser.ANONYMOUS_USER_DN;

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user != null) {
            userDn = user.getDn();
        }

        return userDn;
    }

    @Override
    public String getCurrentUserName() {
        String userName = NiFiUser.ANONYMOUS_USER_DN;

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user != null) {
            userName = user.getUserName();
        }

        return userName;
    }

    @Override
    public ComponentConfiguration getComponentDetails(UiExtensionControllerRequest config) {
        final String id = config.getId();

        if (StringUtils.isBlank(id)) {
            throw new ResourceNotFoundException(String.format("Context config did not have a component ID."));
        }

        // ensure the path could be 
        if (config.getExtensionType() == null) {
            throw new ResourceNotFoundException(String.format("The type must be one of ['%s']", StringUtils.join(UiExtensionType.values(), "', '")));
        }
        
        // get the component facade for interacting directly with that type of object
        ComponentFacade componentFacade = null;
        switch (config.getExtensionType()) {
            case ProcessorConfiguration:
                componentFacade = new ProcessorFacade();
                break;
            case ControllerServiceConfiguration: 
                break;
            case ReportingTaskConfiguration:
                break;
        }
        
        if (componentFacade == null) {
            throw new ResourceNotFoundException(String.format("The type must be one of ['%s']", StringUtils.join(UiExtensionType.values(), "', '")));
        }
        
        return componentFacade.getComponentDetails(config);
    }
    
    @Override
    @PreAuthorize("hasAnyRole('ROLE_DFM')")
    public ComponentConfiguration setAnnotationData(UiExtensionControllerRequest config, String annotationData) {
        final String id = config.getId();

        if (StringUtils.isBlank(id)) {
            throw new ResourceNotFoundException(String.format("Context config did not have a component ID."));
        }

        // get the component facade for interacting directly with that type of object
        ComponentFacade componentFacade = null;
        switch (config.getExtensionType()) {
            case ProcessorConfiguration:
                componentFacade = new ProcessorFacade();
                break;
            case ControllerServiceConfiguration: 
                break;
            case ReportingTaskConfiguration:
                break;
        }
        
        if (componentFacade == null) {
            throw new ResourceNotFoundException(String.format("The type must be one of ['%s']", StringUtils.join(UiExtensionType.values(), "', '")));
        }
        
        return componentFacade.setAnnotationData(config, annotationData);
    }

    private interface ComponentFacade {
        ComponentConfiguration getComponentDetails(UiExtensionControllerRequest config);
        ComponentConfiguration setAnnotationData(UiExtensionControllerRequest config, String annotationData);
    }
    
    private class ProcessorFacade implements ComponentFacade {
        @Override
        public ComponentConfiguration getComponentDetails(UiExtensionControllerRequest config) {
            final Revision revision = config.getRevision();
            final String id = config.getId();
            
            final ProcessorDTO processor;
            if (properties.isClusterManager()) {
                // create the request URL
                URI requestUrl;
                try {
                    String path = "/nifi-api/cluster/processors/" + URLEncoder.encode(id, "UTF-8");
                    requestUrl = new URI(config.getScheme(), null, "localhost", 0, path, null, null);
                } catch (final URISyntaxException | UnsupportedEncodingException use) {
                    throw new ClusterRequestException(use);
                }

                // set the request parameters
                MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
                parameters.add(CLIENT_ID_PARAM, revision.getClientId());
                parameters.add(VERBOSE_PARAM, "true");

                // replicate request
                NodeResponse nodeResponse = clusterManager.applyRequest(HttpMethod.GET, requestUrl, parameters, getHeaders(config));

                // check for issues replicating request
                checkResponse(nodeResponse, revision, id);

                // return processor
                final ProcessorEntity entity = nodeResponse.getClientResponse().getEntity(ProcessorEntity.class);
                processor = entity.getProcessor();
            } else {
                processor = serviceFacade.getProcessor(id);
            }

            // return the processor info
            return getComponentConfiguration(processor);
        }

        @Override
        public ComponentConfiguration setAnnotationData(UiExtensionControllerRequest config, String annotationData) {
            final Revision revision = config.getRevision();
            final String id = config.getId();
            
            final ProcessorDTO processor;
            if (properties.isClusterManager()) {
                // create the request URL
                URI requestUrl;
                try {
                    String path = "/nifi-api/cluster/processors/" + URLEncoder.encode(id, "UTF-8");
                    requestUrl = new URI(config.getScheme(), null, "localhost", 0, path, null, null);
                } catch (final URISyntaxException | UnsupportedEncodingException use) {
                    throw new ClusterRequestException(use);
                }

                // create the revision
                RevisionDTO revisionDto = new RevisionDTO();
                revisionDto.setClientId(revision.getClientId());
                revisionDto.setVersion(revision.getVersion());

                // create the processor entity
                ProcessorEntity processorEntity = new ProcessorEntity();
                processorEntity.setRevision(revisionDto);

                // create the processor dto
                ProcessorDTO processorDto = new ProcessorDTO();
                processorEntity.setProcessor(processorDto);
                processorDto.setId(id);

                // create the processor configuration with the given annotation data
                ProcessorConfigDTO configDto = new ProcessorConfigDTO();
                processorDto.setConfig(configDto);
                configDto.setAnnotationData(annotationData);

                // set the content type to json
                final Map<String, String> headers = getHeaders(config);
                headers.put("Content-Type", "application/json");

                // replicate request
                NodeResponse nodeResponse = clusterManager.applyRequest(HttpMethod.PUT, requestUrl, processorEntity, headers);

                // check for issues replicating request
                checkResponse(nodeResponse, revision, id);
                
                // return processor
                final ProcessorEntity entity = nodeResponse.getClientResponse().getEntity(ProcessorEntity.class);
                processor = entity.getProcessor();
            } else {
                final ConfigurationSnapshot<ProcessorDTO> response = serviceFacade.setProcessorAnnotationData(revision, id, annotationData);
                processor = response.getConfiguration();
            }
            
            // return the processor info
            return getComponentConfiguration(processor);
        }
        
        private ComponentConfiguration getComponentConfiguration(final ProcessorDTO processor) {
            final ProcessorConfigDTO processorConfig = processor.getConfig();
            return new ComponentConfiguration.Builder()
                    .id(processor.getId())
                    .name(processor.getName())
                    .state(processor.getState())
                    .annotationData(processorConfig.getAnnotationData())
                    .properties(processorConfig.getProperties())
                    .validateErrors(processor.getValidationErrors()).build();
        }
    }
    
    /**
     * Gets the headers for the request to replicate to each node while
     * clustered.
     *
     * @param config
     * @return
     */
    private Map<String, String> getHeaders(final UiExtensionControllerRequest config) {
        final Map<String, String> headers = new HashMap<>();
        headers.put("Accept", "application/json,application/xml");
        if (StringUtils.isNotBlank(config.getProxiedEntitiesChain())) {
            headers.put("X-ProxiedEntitiesChain", config.getProxiedEntitiesChain());
        }

        // add the user's authorities (if any) to the headers
        final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null) {
            final Object userDetailsObj = authentication.getPrincipal();
            if (userDetailsObj instanceof NiFiUserDetails) {
                // serialize user details object
                final String hexEncodedUserDetails = WebUtils.serializeObjectToHex((Serializable) userDetailsObj);

                // put serialized user details in header
                headers.put("X-ProxiedEntityUserDetails", hexEncodedUserDetails);
            }
        }
        return headers;
    }
    
    /**
     * Checks the specified response and drains the stream appropriately.
     * 
     * @param nodeResponse
     * @param revision
     * @param id 
     */
    private void checkResponse(final NodeResponse nodeResponse, final Revision revision, final String id) {
        if (nodeResponse.hasThrowable()) {
            ClientResponseUtils.drainClientResponse(nodeResponse.getClientResponse());
            throw new ClusterRequestException(nodeResponse.getThrowable());
        } else if (nodeResponse.getClientResponse().getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            ClientResponseUtils.drainClientResponse(nodeResponse.getClientResponse());
            throw new InvalidRevisionException(String.format("Invalid revision specified %s", revision));
        } else if (nodeResponse.getClientResponse().getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            ClientResponseUtils.drainClientResponse(nodeResponse.getClientResponse());
            throw new ResourceNotFoundException("Unable to find component with id: " + id);
        } else if (nodeResponse.getClientResponse().getStatus() != Response.Status.OK.getStatusCode()) {
            ClientResponseUtils.drainClientResponse(nodeResponse.getClientResponse());
            throw new ClusterRequestException("Method resulted in an unsuccessful HTTP response code: " + nodeResponse.getClientResponse().getStatus());
        }
    }

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuditService(AuditService auditService) {
        this.auditService = auditService;
    }

    public void setControllerServiceLookup(ControllerServiceLookup controllerServiceLookup) {
        this.controllerServiceLookup = controllerServiceLookup;
    }

}
