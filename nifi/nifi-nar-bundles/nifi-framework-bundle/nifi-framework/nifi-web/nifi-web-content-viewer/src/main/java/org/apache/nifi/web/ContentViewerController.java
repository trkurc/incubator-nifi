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
import java.util.Map;
import javax.servlet.RequestDispatcher;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 */
@WebServlet(name = "ContentViewerController", urlPatterns = {"/viewer"})
public class ContentViewerController extends HttpServlet {

    /**
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final ServletContext servletContext = request.getServletContext();
        final String contentViewerUri = servletContext.getInitParameter("application/xml");
        
        // header
        final RequestDispatcher header = request.getRequestDispatcher("/WEB-INF/jsp/header.jsp");
        header.include(request, response);
        
        // content
        final ServletContext viewerContext = servletContext.getContext(contentViewerUri);
        final RequestDispatcher content = viewerContext.getRequestDispatcher("/view-content");
        content.include(request, response);
        
        // footer
        final RequestDispatcher footer = request.getRequestDispatcher("/WEB-INF/jsp/footer.jsp");
        footer.include(request, response);
    }

}
