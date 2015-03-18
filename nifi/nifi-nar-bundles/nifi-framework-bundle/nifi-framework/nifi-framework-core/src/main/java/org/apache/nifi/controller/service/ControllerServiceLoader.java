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
package org.apache.nifi.controller.service;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.FlowFromDOMFactory;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.DomUtils;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 *
 */
public class ControllerServiceLoader {

    private static final Logger logger = LoggerFactory.getLogger(ControllerServiceLoader.class);


    public static List<ControllerServiceNode> loadControllerServices(final ControllerServiceProvider provider, final InputStream serializedStream, final StringEncryptor encryptor, final BulletinRepository bulletinRepo, final boolean autoResumeState) throws IOException {
        final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true);

        try (final InputStream in = new BufferedInputStream(serializedStream)) {
            final DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();

            builder.setErrorHandler(new org.xml.sax.ErrorHandler() {

                @Override
                public void fatalError(final SAXParseException err) throws SAXException {
                    logger.error("Config file line " + err.getLineNumber() + ", col " + err.getColumnNumber() + ", uri " + err.getSystemId() + " :message: " + err.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.error("Error Stack Dump", err);
                    }
                    throw err;
                }

                @Override
                public void error(final SAXParseException err) throws SAXParseException {
                    logger.error("Config file line " + err.getLineNumber() + ", col " + err.getColumnNumber() + ", uri " + err.getSystemId() + " :message: " + err.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.error("Error Stack Dump", err);
                    }
                    throw err;
                }

                @Override
                public void warning(final SAXParseException err) throws SAXParseException {
                    logger.warn(" Config file line " + err.getLineNumber() + ", uri " + err.getSystemId() + " : message : " + err.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.warn("Warning stack dump", err);
                    }
                    throw err;
                }
            });
            
            final Document document = builder.parse(in);
            final Element controllerServices = document.getDocumentElement();
            final List<Element> serviceElements = DomUtils.getChildElementsByTagName(controllerServices, "controllerService");
            return new ArrayList<ControllerServiceNode>(loadControllerServices(serviceElements, provider, encryptor, bulletinRepo, autoResumeState));
        } catch (SAXException | ParserConfigurationException sxe) {
            throw new IOException(sxe);
        }
    }
    
    public static Collection<ControllerServiceNode> loadControllerServices(final List<Element> serviceElements, final ControllerServiceProvider provider, final StringEncryptor encryptor, final BulletinRepository bulletinRepo, final boolean autoResumeState) {
        final Map<ControllerServiceNode, Element> nodeMap = new HashMap<>();
        for ( final Element serviceElement : serviceElements ) {
            final ControllerServiceNode serviceNode = createControllerService(provider, serviceElement, encryptor);
            // We need to clone the node because it will be used in a separate thread below, and 
            // Element is not thread-safe.
            nodeMap.put(serviceNode, (Element) serviceElement.cloneNode(true));
        }
        for ( final Map.Entry<ControllerServiceNode, Element> entry : nodeMap.entrySet() ) {
            configureControllerService(entry.getKey(), entry.getValue(), encryptor);
        }
        
        // Start services
        if ( autoResumeState ) {
            // determine the order to load the services. We have to ensure that if service A references service B, then B
            // is enabled first, and so on.
            final Map<String, ControllerServiceNode> idToNodeMap = new HashMap<>();
            for ( final ControllerServiceNode node : nodeMap.keySet() ) {
                idToNodeMap.put(node.getIdentifier(), node);
            }
            
            // We can have many Controller Services dependent on one another. We can have many of these
            // disparate lists of Controller Services that are dependent on one another. We refer to each
            // of these as a branch.
            final List<List<ControllerServiceNode>> branches = determineEnablingOrder(idToNodeMap);
            
            final ExecutorService executor = Executors.newFixedThreadPool(Math.min(10, branches.size()));
            
            for ( final List<ControllerServiceNode> branch : branches ) {
                final Runnable enableBranchRunnable = new Runnable() {
                    @Override
                    public void run() {
                        logger.debug("Enabling Controller Service Branch {}", branch);
                        
                        for ( final ControllerServiceNode serviceNode : branch ) {
                            try {
                                final Element controllerServiceElement = nodeMap.get(serviceNode);
    
                                final ControllerServiceDTO dto;
                                synchronized (controllerServiceElement.getOwnerDocument()) {
                                    dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor);
                                }
                                
                                final ControllerServiceState state = ControllerServiceState.valueOf(dto.getState());
                                final boolean enable = (state == ControllerServiceState.ENABLED);
                                if (enable) {
                                    if ( ControllerServiceState.DISABLED.equals(serviceNode.getState()) ) {
                                        logger.info("Enabling {}", serviceNode);
                                        try {
                                            provider.enableControllerService(serviceNode);
                                        } catch (final Exception e) {
                                            logger.error("Failed to enable " + serviceNode + " due to " + e);
                                            if ( logger.isDebugEnabled() ) {
                                                logger.error("", e);
                                            }
                                            
                                            bulletinRepo.addBulletin(BulletinFactory.createBulletin(
                                                    "Controller Service", Severity.ERROR.name(), "Could not start " + serviceNode + " due to " + e));
                                        }
                                    }
                                    
                                    // wait for service to finish enabling.
                                    while ( ControllerServiceState.ENABLING.equals(serviceNode.getState()) ) {
                                        try {
                                            Thread.sleep(100L);
                                        } catch (final InterruptedException ie) {}
                                    }
                                    
                                    logger.info("State for {} is now {}", serviceNode, serviceNode.getState());
                                }
                            } catch (final Exception e) {
                                logger.error("Failed to enable {} due to {}", serviceNode, e.toString());
                                if ( logger.isDebugEnabled() ) {
                                    logger.error("", e);
                                }
                            }
                        }
                    }
                };
                
                executor.submit(enableBranchRunnable);
            }
            
            executor.shutdown();
        }
        
        return nodeMap.keySet();
    }
    
    
    static List<List<ControllerServiceNode>> determineEnablingOrder(final Map<String, ControllerServiceNode> serviceNodeMap) {
        final List<List<ControllerServiceNode>> orderedNodeLists = new ArrayList<>();
        
        for ( final ControllerServiceNode node : serviceNodeMap.values() ) {
            if ( orderedNodeLists.contains(node) ) {
                continue;   // this node is already in the list.
            }
            
            final List<ControllerServiceNode> branch = new ArrayList<>();
            determineEnablingOrder(serviceNodeMap, node, branch, new HashSet<ControllerServiceNode>());
            orderedNodeLists.add(branch);
        }
        
        return orderedNodeLists;
    }
    
    
    private static void determineEnablingOrder(final Map<String, ControllerServiceNode> serviceNodeMap, final ControllerServiceNode contextNode, final List<ControllerServiceNode> orderedNodes, final Set<ControllerServiceNode> visited) {
        if ( visited.contains(contextNode) ) {
            return;
        }
        
        for ( final Map.Entry<PropertyDescriptor, String> entry : contextNode.getProperties().entrySet() ) {
            if ( entry.getKey().getControllerServiceDefinition() != null ) {
                final String referencedServiceId = entry.getValue();
                if ( referencedServiceId != null ) {
                    final ControllerServiceNode referencedNode = serviceNodeMap.get(referencedServiceId);
                    if ( !orderedNodes.contains(referencedNode) ) {
                        visited.add(contextNode);
                        determineEnablingOrder(serviceNodeMap, referencedNode, orderedNodes, visited);
                    }
                }
            }
        }

        if ( !orderedNodes.contains(contextNode) ) {
            orderedNodes.add(contextNode);
        }
    }
    
    private static ControllerServiceNode createControllerService(final ControllerServiceProvider provider, final Element controllerServiceElement, final StringEncryptor encryptor) {
        final ControllerServiceDTO dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor);
        
        final ControllerServiceNode node = provider.createControllerService(dto.getType(), dto.getId(), false);
        node.setName(dto.getName());
        node.setComments(dto.getComments());
        return node;
    }
    
    private static void configureControllerService(final ControllerServiceNode node, final Element controllerServiceElement, final StringEncryptor encryptor) {
        final ControllerServiceDTO dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor);
        node.setAnnotationData(dto.getAnnotationData());
        
        for (final Map.Entry<String, String> entry : dto.getProperties().entrySet()) {
            if (entry.getValue() == null) {
                node.removeProperty(entry.getKey());
            } else {
                node.setProperty(entry.getKey(), entry.getValue());
            }
        }
    }
}
