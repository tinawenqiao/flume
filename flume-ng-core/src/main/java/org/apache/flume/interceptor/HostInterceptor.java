/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.interceptor;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flume.interceptor.HostInterceptor.Constants.*;

/**
 * Simple Interceptor class that sets the host name or IP on all events
 * that are intercepted.<p>
 * The host header is named <code>host</code> and its format is either the FQDN
 * or IP of the host on which this interceptor is run.
 *
 *
 * Properties:<p>
 *
 *   preserveExisting: Whether to preserve an existing value for 'host'
 *                     (default is false)<p>
 *
 *   useIP: Whether to use IP address or fully-qualified hostname for 'host'
 *          header value (default is true)<p>
 *
 *  hostHeader: Specify the key to be used in the event header map for the
 *          host name. (default is "host") <p>
 *
 * Sample config:<p>
 *
 * <code>
 *   agent.sources.r1.channels = c1<p>
 *   agent.sources.r1.type = SEQ<p>
 *   agent.sources.r1.interceptors = i1<p>
 *   agent.sources.r1.interceptors.i1.type = host<p>
 *   agent.sources.r1.interceptors.i1.preserveExisting = true<p>
 *   agent.sources.r1.interceptors.i1.useIP = false<p>
 *   agent.sources.r1.interceptors.i1.useHostname = true<p>
 *   agent.sources.r1.interceptors.i1.ip = ip<p>
 *   agent.sources.r1.interceptors.i1.hostname = hostname<p>
 * </code>
 *
 */
public class HostInterceptor implements Interceptor {

  private static final Logger logger = LoggerFactory
          .getLogger(HostInterceptor.class);

  private final boolean preserveExisting;
  private final String ipHeader;
  private final String hostnameHeader;
  private String ip = null;
  private String hostname = null;

  /**
   * Only {@link HostInterceptor.Builder} can build me
   */
  private HostInterceptor(boolean preserveExisting,
      boolean useIP, boolean useHostname, String ipHeader, String hostnameHeader) {
    this.preserveExisting = preserveExisting;
    this.ipHeader = ipHeader;
    this.hostnameHeader = hostnameHeader;
    InetAddress addr;
    try {
      addr = InetAddress.getLocalHost();
      if (useIP) {
        ip = addr.getHostAddress();
      }
      if (useHostname) {
        hostname = addr.getCanonicalHostName();
      }
    } catch (UnknownHostException e) {
      logger.warn("Could not get local host address. Exception follows.", e);
    }

  }

  @Override
  public void initialize() {
    // no-op
  }

  /**
   * Modifies events in-place.
   */
  @Override
  public Event intercept(Event event) {
    Map<String, String> headers = event.getHeaders();

    if (preserveExisting && (headers.containsKey(ipHeader) || headers.containsKey(hostnameHeader)))
    {
      return event;
    }
    if (ip != null) {
      headers.put(ipHeader, ip);
    }
    if (hostname != null) {
      headers.put(hostnameHeader, hostname);
    }

    return event;
  }

  /**
   * Delegates to {@link #intercept(Event)} in a loop.
   * @param events
   * @return
   */
  @Override
  public List<Event> intercept(List<Event> events) {
    for (Event event : events) {
      intercept(event);
    }
    return events;
  }

  @Override
  public void close() {
    // no-op
  }

  /**
   * Builder which builds new instances of the HostInterceptor.
   */
  public static class Builder implements Interceptor.Builder {

    private boolean preserveExisting = PRESERVE_DFLT;
    private boolean useIP = USE_IP_DFLT;
    private boolean useHostname = USE_HOSTNAME_DFLT;
    private String ipHeader = "ip";
    private String hostnameHeader = "hostname";

    @Override
    public Interceptor build() {
      return new HostInterceptor(preserveExisting, useIP, useHostname, ipHeader, hostnameHeader);
    }

    @Override
    public void configure(Context context) {
      preserveExisting = context.getBoolean(PRESERVE, PRESERVE_DFLT);
      useIP = context.getBoolean(USE_IP, USE_IP_DFLT);
      useHostname = context.getBoolean(USE_HOSTNAME, USE_HOSTNAME_DFLT);
      ipHeader = context.getString(HOSTIP_HEADER, ipHeader);
      hostnameHeader = context.getString(HOSTNAME_HEADER, hostnameHeader);
    }

  }

  public static class Constants {
    //public static String HOST = "host";

    public static String PRESERVE = "preserveExisting";
    public static boolean PRESERVE_DFLT = false;

    public static String USE_IP = "useIP";
    public static boolean USE_IP_DFLT = true;

    public static String USE_HOSTNAME = "useHostname";
    public static boolean USE_HOSTNAME_DFLT = true;

    public static String HOSTIP_HEADER = "ip";
    public static String HOSTNAME_HEADER = "hostname";
  }

}
