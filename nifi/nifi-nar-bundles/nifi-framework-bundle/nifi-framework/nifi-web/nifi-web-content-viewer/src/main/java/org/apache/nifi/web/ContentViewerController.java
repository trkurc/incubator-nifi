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

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import javax.servlet.RequestDispatcher;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.web.ViewableContent.DisplayMode;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@WebServlet(name = "ContentViewerController", urlPatterns = {"/viewer"})
public class ContentViewerController extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(ContentViewerController.class);
    
    /**
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        // get the content
        final ServletContext servletContext = request.getServletContext();
        final ContentAccess contentAccess = (ContentAccess) servletContext.getAttribute("nifi-content-access");
        final DownloadableContent downloadableContent = contentAccess.getContent(getContentRequest(request));

        // ensure the content is found
        if (downloadableContent == null) {
            response.getWriter().println("No content...");
            return;
        }

        // determine how we want to view the data
        String mode = request.getParameter("mode");
        
        // if the name isn't set, use original
        if (mode == null) {
            mode = DisplayMode.Original.name();
        }
        
        // determine the display mode
        final DisplayMode displayMode;
        try {
            displayMode = DisplayMode.valueOf(mode);
        } catch (final IllegalArgumentException iae) {
            response.getWriter().println("Invalid display mode: " + mode);
            return;
        }
        
        // build the request url
        final StringBuffer requestUrl = request.getRequestURL();
        request.setAttribute("requestUrl", requestUrl.toString());
        request.setAttribute("dataRef", request.getParameter("ref"));
        
        // generate the header
        request.getRequestDispatcher("/WEB-INF/jsp/header.jsp").include(request, response);
        
        // remove the request url
        request.removeAttribute("requestUrl");
        request.removeAttribute("dataRef");
        
        // generate the markup for the content based on the display mode
        if (DisplayMode.Hex.equals(displayMode)) {
            // convert stream into the base 64 bytes
            final byte[] bytes = IOUtils.toByteArray(downloadableContent.getContent());
            final String base64 = Base64.encodeBase64String(bytes);
            
            // defer to the jsp
            request.setAttribute("content", base64);
            request.getRequestDispatcher("/WEB-INF/jsp/hexview.jsp").include(request, response);
        } else {
            // detect the content type
            final DefaultDetector detector = new DefaultDetector();

            // create the stream for tika to process, buffer to support reseting
            final BufferedInputStream bis = new BufferedInputStream(downloadableContent.getContent());
            final TikaInputStream tikaStream = TikaInputStream.get(bis);

            // provide a hint based on the filename
            final Metadata metadata = new Metadata();
            metadata.set(Metadata.RESOURCE_NAME_KEY, downloadableContent.getFilename());

            // Get mime type
            final MediaType mediatype = detector.detect(tikaStream, metadata);
            final String mimeType = mediatype.toString();

            // lookup a viewer for the content
            final String contentViewerUri = servletContext.getInitParameter(mimeType);

            // handle no viewer for content type
            if (contentViewerUri == null) {
                final PrintWriter out = response.getWriter();
                out.println("No viewer...");
                out.println("identified mime type: " + mimeType);
                out.println("filename: " + downloadableContent.getFilename());
                out.println("type: " + downloadableContent.getType());

                return;
            }
            
            // create a request attribute for accessing the content
            request.setAttribute(ViewableContent.CONTENT_REQUEST_ATTRIBUTE, new ViewableContent() {
                @Override
                public InputStream getContentStream() {
                    return bis;
                }

                @Override
                public String getContent() throws IOException {
                    // detect the charset
                    final CharsetDetector detector = new CharsetDetector();
                    detector.setText(bis);
                    detector.enableInputFilter(true);
                    final CharsetMatch match = detector.detect();

                    // ensure we were able to detect the charset
                    if (match == null) {
                        throw new IOException("Unable to detect character encoding.");
                    }

                    // convert the stream using the detected charset
                    return IOUtils.toString(bis, match.getName());
                }

                @Override
                public ViewableContent.DisplayMode getDisplayMode() {
                    return displayMode;
                }

                @Override
                public String getFileName() {
                    return downloadableContent.getFilename();
                }

                @Override
                public String getContentType() {
                    return mimeType;
                }
            });

            // generate the content
            final ServletContext viewerContext = servletContext.getContext(contentViewerUri);
            final RequestDispatcher content = viewerContext.getRequestDispatcher("/view-content");
            content.include(request, response);

            // remove the request attribute
            request.removeAttribute(ViewableContent.CONTENT_REQUEST_ATTRIBUTE);
        }

        // generate footer
        request.getRequestDispatcher("/WEB-INF/jsp/footer.jsp").include(request, response);
    }

    /**
     * Get the content request context based on the specified request.
     * @param request
     * @return 
     */
    private ContentRequestContext getContentRequest(final HttpServletRequest request) {
        return new ContentRequestContext() {
            @Override
            public String getDataUri() {
                return request.getParameter("ref");
            }

            @Override
            public String getClusterNodeId() {
                return request.getParameter("clusterNodeId");
            }

            @Override
            public String getClientId() {
                return request.getParameter("clientId");
            }

            @Override
            public String getProxiedEntitiesChain() {
                return request.getHeader("X-ProxiedEntitiesChain");
            }
        };
    }
}
