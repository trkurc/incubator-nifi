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
package org.apache.nifi.web.server;

import java.io.IOException;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 *
 */
public class DataHandler extends HttpServlet {

    private final WebAppContext dataApps;

    public DataHandler(WebAppContext dataApps) {
        this.dataApps = dataApps;
    }
    
    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        // detect file type and appropriate data app to forward to
        
        // header
        final RequestDispatcher header = request.getRequestDispatcher("/WEB-INF/jsp/header.jsp");
        header.include(request, response);
        
        // content
        final RequestDispatcher content = dataApps.getServletContext().getRequestDispatcher("/WEB-INF/jsp/content.jsp");
        content.include(request, response);
        
        // footer
        final RequestDispatcher footer = request.getRequestDispatcher("/WEB-INF/jsp/footer.jsp");
        footer.include(request, response);
    }
    
}
