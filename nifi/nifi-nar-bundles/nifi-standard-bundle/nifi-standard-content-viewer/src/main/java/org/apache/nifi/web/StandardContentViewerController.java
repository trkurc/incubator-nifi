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
import java.nio.charset.StandardCharsets;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 */
@WebServlet(name = "StandardContentViewer", urlPatterns = {"/view-content"})
public class StandardContentViewerController extends HttpServlet {

    /**
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final ViewableContent content = (ViewableContent) request.getAttribute(ViewableContent.CONTENT_REQUEST_ATTRIBUTE);
        final String contentString = content.getContent();
        
        // handle json/xml
        if ("application/json".equals(content.getContentType()) || "application/xml".equals(content.getContentType())) {
            final String formatted;
            
            // format the content
            if ("application/json".equals(content.getContentType())) {
                final ObjectMapper mapper = new ObjectMapper();
                final Object objectJson = mapper.readValue(contentString, Object.class);
                formatted = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectJson);
            } else {
                formatted = "";
            }
            
            // defer the jsp
            request.setAttribute("mode", content.getContentType());
            request.setAttribute("content", formatted);
            request.getRequestDispatcher("/WEB-INF/jsp/codemirror.jsp").include(request, response);
        } 
        
    }

}
