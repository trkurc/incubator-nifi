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
package org.apache.nifi.ui.extension;

import org.apache.nifi.web.controller.UiExtensionControllerRequest;
import org.apache.nifi.web.controller.UiExtensionControllerFacade;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.component.details.ExtensionDetails;
import org.apache.nifi.action.details.ConfigureDetails;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.web.ClusterRequestException;
import org.apache.nifi.web.ComponentConfiguration;
import org.apache.nifi.web.ConfigurationAction;
import org.apache.nifi.web.InvalidRevisionException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.UiExtensionRequestContext;


/**
 *
 */
public class ConfigureComponentController extends HttpServlet {

    public static final String ID_PARAM = "id";
    public static final String CLIENT_ID_PARAM = "clientId";
    public static final String VERSION_PARAM = "version";
    
    /**
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // get the content
        final ServletContext servletContext = request.getServletContext();
        final Object extensionMappings = (Object) servletContext.getAttribute("nifi-ui-extension-mapping");
        final UiExtensionControllerFacade controllerFacade = (UiExtensionControllerFacade) servletContext.getAttribute("nifi-ui-extension-controller-facade");
        
        // get the component type
        final String type = request.getParameter("type");
        
        // ensure the request has 
        if (type == null) {
            response.getWriter().println("Request missing the component type.");
            return;
        }
        
        // change later
        final UiExtensionType extensionType = UiExtensionType.ProcessorConfiguration;
        
        // build the ui extension request context config
        final UiExtensionControllerRequest config = getRequestContextConig(extensionType, request);
        
        // get the initial component details
        final ComponentConfiguration details = controllerFacade.getComponentDetails(config);
        
        // lookup a viewer for the content
        final String uiExtensionUri = servletContext.getInitParameter(type);
        
        // ensure the registered viewer is found
        if (uiExtensionUri == null) {
            response.getWriter().println("No custom UI is registered for " + type);
        }
        
        // set the attribute for the custom ui to interact with nifi
        request.setAttribute(UiExtensionRequestContext.ATTRIBUTE_KEY, new UiExtensionRequestContext() {
            @Override
            public ControllerService getControllerService(final String serviceIdentifier) {
                return controllerFacade.getControllerService(serviceIdentifier);
            }

            @Override
            public void saveActions(Collection<ConfigurationAction> uiExtensionActions) {
                final Collection<Action> actions = new ArrayList<>();
                
                // conver the action models
                if (uiExtensionActions != null) {
                    final Date now = new Date();
                    
                    // create the extension details
                    ExtensionDetails extensionDetails = new ExtensionDetails();
                    extensionDetails.setType(details.getType());
                    
                    for (final ConfigurationAction extensionAction : uiExtensionActions) {
                        // create the action details
                        final ConfigureDetails actionDetails = new ConfigureDetails();
                        actionDetails.setName(extensionAction.getField());
                        actionDetails.setValue(extensionAction.getValue());
                        actionDetails.setPreviousValue(extensionAction.getPreviousValue());

                        // create a configuration action
                        Action configurationAction = new Action();
                        configurationAction.setUserDn(getCurrentUserDn());
                        configurationAction.setUserName(getCurrentUserName());
                        configurationAction.setOperation(Operation.Configure);
                        configurationAction.setTimestamp(now);
                        configurationAction.setSourceId(details.getId());
                        configurationAction.setSourceName(details.getName());
                        configurationAction.setSourceType(Component.Processor);
                        configurationAction.setComponentDetails(extensionDetails);
                        configurationAction.setActionDetails(actionDetails);
                        
                        // add the action
                        actions.add(configurationAction);
                    }
                }
                
                // save the actions
                if (!actions.isEmpty()) {
                    controllerFacade.saveActions(actions);
                }
            }

            @Override
            public String getCurrentUserDn() {
                return controllerFacade.getCurrentUserDn();
            }

            @Override
            public String getCurrentUserName() {
                return controllerFacade.getCurrentUserName();
            }

            @Override
            public ComponentConfiguration setAnnotationData(String annotationData) throws ClusterRequestException, InvalidRevisionException, ResourceNotFoundException{
                return controllerFacade.setAnnotationData(config, annotationData);
            }

            @Override
            public ComponentConfiguration getComponentDetails() throws ClusterRequestException, ResourceNotFoundException {
                return controllerFacade.getComponentDetails(config);
            }
        });
        
        // generate the content
        final ServletContext customUiContext = servletContext.getContext(uiExtensionUri);
        customUiContext.getRequestDispatcher("/configure").forward(request, response);
    }
    
    /**
     * Creates a UiExtensionRequestContextConfig from the specified request.
     * 
     * @param request
     * @return 
     */
    private UiExtensionControllerRequest getRequestContextConig(final UiExtensionType extensionType, final HttpServletRequest request) {
        return new UiExtensionControllerRequest() {

            @Override
            public UiExtensionType getExtensionType() {
                return extensionType;
            }

            @Override
            public String getScheme() {
                return request.getScheme();
            }

            @Override
            public String getId() {
                return request.getParameter(ID_PARAM);
            }

            @Override
            public Revision getRevision() {
                final String versionParamVal = request.getParameter(VERSION_PARAM);
                Long version;
                try {
                    version = Long.parseLong(versionParamVal);
                } catch (final Exception ex) {
                    version = null;
                }

                final String clientId = request.getParameter(CLIENT_ID_PARAM);

                return new Revision(version, clientId);
            }

            @Override
            public String getProxiedEntitiesChain() {
                String xProxiedEntitiesChain = request.getHeader("X-ProxiedEntitiesChain");
                final X509Certificate cert = extractClientCertificate(request);
                if (cert != null) {
                    final String extractedPrincipal = extractPrincipal(cert);
                    final String formattedPrincipal = formatProxyDn(extractedPrincipal);
                    if (xProxiedEntitiesChain == null || xProxiedEntitiesChain.trim().isEmpty()) {
                        xProxiedEntitiesChain = formattedPrincipal;
                    } else {
                        xProxiedEntitiesChain += formattedPrincipal;
                    }
                }

                return xProxiedEntitiesChain;
            }
        };
    }

    /**
     * Utility methods that have been copied into this class to reduce the
     * dependency footprint of this artifact. These utility methods typically
     * live in web-utilities but that would pull in spring, jersey, jackson,
     * etc.
     */
    
    private X509Certificate extractClientCertificate(HttpServletRequest request) {
        X509Certificate[] certs = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");

        if (certs != null && certs.length > 0) {
            return certs[0];
        }

        return null;
    }
    
    private String extractPrincipal(X509Certificate cert) {
        return cert.getSubjectDN().getName().trim();
    }

    private String formatProxyDn(String dn) {
        return "<" + dn + ">";
    }
}
