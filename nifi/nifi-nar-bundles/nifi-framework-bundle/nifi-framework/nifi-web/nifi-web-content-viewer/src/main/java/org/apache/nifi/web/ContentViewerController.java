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
package org.apache.nifi.web;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import javax.servlet.RequestDispatcher;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@WebServlet(name = "ContentViewerController", urlPatterns = {"/viewer"})
public class ContentViewerController extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(ContentViewerController.class);
    
    private static final String CONTENT_REQUEST_ATTRIBUTE = "org.apache.nifi.web.content";
    
    // context for accessing the extension mapping
//    private ServletContext servletContext;
//
//    @Override
//    public void init(final ServletConfig config) throws ServletException {
//        super.init(config);
//        servletContext = config.getServletContext();
//    }
    
    /**
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        logger.info(request.getServletPath());
        
        // get the content
        final ServletContext servletContext = request.getServletContext();
        final ContentAccess contentAccess = (ContentAccess) servletContext.getAttribute("nifi-content-access");
        final DownloadableContent downloadableContent = contentAccess.getContent(request.getParameter("ref"));
        
        // ensure the content is found
        if (downloadableContent == null) {
            
        }
        
        // detect the content type
        
        // lookup a viewer for the content
        final String contentViewerUri = servletContext.getInitParameter("application/xml");
        
        // handle no viewer for content type
        if (contentViewerUri == null) {
            
        }
        
        // generate the header
        final RequestDispatcher header = request.getRequestDispatcher("/WEB-INF/jsp/header.jsp");
        header.include(request, response);
        
        // create a request attribute for accessing the content
        request.setAttribute(CONTENT_REQUEST_ATTRIBUTE, new ViewableContent() {
            @Override
            public InputStream getContent() {
                return downloadableContent.getContent();
            }

            @Override
            public String getFileName() {
                return downloadableContent.getFilename();
            }
        });
        
        // generate the content
        final ServletContext viewerContext = servletContext.getContext(contentViewerUri);
        final RequestDispatcher content = viewerContext.getRequestDispatcher("/view-content");
        content.include(request, response);
        
        // remove the request attribute
        request.removeAttribute(CONTENT_REQUEST_ATTRIBUTE);
        
        // generate footer
        final RequestDispatcher footer = request.getRequestDispatcher("/WEB-INF/jsp/footer.jsp");
        footer.include(request, response);
    }

}
