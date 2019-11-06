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

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.HostInterceptor.Constants;
import org.junit.Assert;
import org.junit.Test;

public class TestHostInterceptor {

  /**
   * Ensure that the "ip" and "hostname" header gets set (to something)
   */
  @Test
  public void testBasic() throws Exception {
    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
            InterceptorType.HOST.toString());
    Interceptor interceptor = builder.build();

    Event eventBeforeIntercept = EventBuilder.withBody("test event",
            Charsets.UTF_8);
    Assert.assertNull(eventBeforeIntercept.getHeaders().get(Constants.HOSTIP_HEADER));
    Assert.assertNull(eventBeforeIntercept.getHeaders().get(Constants.HOSTNAME_HEADER));

    Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
    String actualHostIP = eventAfterIntercept.getHeaders().get(Constants.HOSTIP_HEADER);
    String actualHostname = eventAfterIntercept.getHeaders().get(Constants.HOSTNAME_HEADER);

    Assert.assertNotNull(actualHostIP);
    Assert.assertNotNull(actualHostname);
  }

  @Test
  public void testCustomHeader() throws Exception {
    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
            InterceptorType.HOST.toString());
    Context ctx = new Context();
    ctx.put("preserveExisting", "false");
    ctx.put("hostname", "hostnameKey");
    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    Event eventBeforeIntercept = EventBuilder.withBody("test event",
            Charsets.UTF_8);
    Assert.assertNull(eventBeforeIntercept.getHeaders().get("hostnameKey"));

    Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
    String actualHost = eventAfterIntercept.getHeaders().get("hostnameKey");

    Assert.assertNotNull(actualHost);
    Assert.assertEquals(InetAddress.getLocalHost().getHostAddress(),
        actualHost);
  }

  /**
   * Ensure hostname is NOT overwritten when preserveExisting=true.
   */
  @Test
  public void testPreserve() throws Exception {
    Context ctx = new Context();
    ctx.put("preserveExisting", "true");

    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
            InterceptorType.HOST.toString());
    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    final String ORIGINAL_HOST = "originalhost";
    Event eventBeforeIntercept = EventBuilder.withBody("test event",
            Charsets.UTF_8);
    eventBeforeIntercept.getHeaders().put(Constants.HOSTNAME_HEADER, ORIGINAL_HOST);
    Assert.assertEquals(ORIGINAL_HOST,
            eventBeforeIntercept.getHeaders().get(Constants.HOSTNAME_HEADER));

    String expectedHost = ORIGINAL_HOST;
    Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
    String actualHost = eventAfterIntercept.getHeaders().get(Constants.HOSTNAME_HEADER);

    Assert.assertNotNull(actualHost);
    Assert.assertEquals(expectedHost, actualHost);
  }

  /**
   * Ensure hostname is overwritten when preserveExisting=false.
   */
  @Test
  public void testClobber() throws Exception {
    Context ctx = new Context();
    ctx.put("preserveExisting", "false"); // default behavior

    Interceptor.Builder builder = InterceptorBuilderFactory
            .newInstance(InterceptorType.HOST.toString());
    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    final String ORIGINAL_HOST = "originalhost";
    Event eventBeforeIntercept = EventBuilder.withBody("test event",
            Charsets.UTF_8);
    eventBeforeIntercept.getHeaders().put(Constants.HOSTNAME_HEADER, ORIGINAL_HOST);
    Assert.assertEquals(ORIGINAL_HOST, eventBeforeIntercept.getHeaders()
            .get(Constants.HOSTNAME_HEADER));

    String expectedHost = InetAddress.getLocalHost().getHostAddress();
    Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
    String actualHost = eventAfterIntercept.getHeaders().get(Constants.HOSTNAME_HEADER);

    Assert.assertNotNull(actualHost);
    Assert.assertEquals(expectedHost, actualHost);
  }

  /**
   * Ensure host IP and hostname are used by default..
   */
  @Test
  public void testUseIPAndHostname() throws Exception {
    Context ctx = new Context();
    ctx.put(Constants.USE_IP, "true"); // default behavior
    ctx.put(Constants.USE_HOSTNAME, "true"); //default behavior

    Interceptor.Builder builder = InterceptorBuilderFactory
            .newInstance(InterceptorType.HOST.toString());
    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    final String ORIGINAL_HOST = "originalhost";
    Event eventBeforeIntercept = EventBuilder.withBody("test event",
            Charsets.UTF_8);
    eventBeforeIntercept.getHeaders().put(Constants.HOSTNAME_HEADER, ORIGINAL_HOST);
    Assert.assertEquals(ORIGINAL_HOST, eventBeforeIntercept.getHeaders()
            .get(Constants.HOSTNAME_HEADER));

    String expectedHostIP = InetAddress.getLocalHost().getHostAddress();
    String expectedHostname = InetAddress.getLocalHost().getCanonicalHostName();
    Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
    String actualHostIP = eventAfterIntercept.getHeaders().get(Constants.HOSTIP_HEADER);
    String actualHostname = eventAfterIntercept.getHeaders().get(Constants.HOSTNAME_HEADER);

    Assert.assertNotNull(actualHostIP);
    Assert.assertNotNull(actualHostname);
    Assert.assertEquals(expectedHostIP, actualHostIP);
    Assert.assertEquals(expectedHostname, actualHostname);
  }

  /**
   * Ensure only host IP is used.
   */
  @Test
  public void testUseIPOnly() throws Exception {
    Context ctx = new Context();
    ctx.put(Constants.USE_IP, "true"); // default behavior
    ctx.put(Constants.USE_HOSTNAME, "false");

    Interceptor.Builder builder = InterceptorBuilderFactory
            .newInstance(InterceptorType.HOST.toString());
    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    final String ORIGINAL_HOST = "originalhost";
    Event eventBeforeIntercept = EventBuilder.withBody("test event",
            Charsets.UTF_8);
    eventBeforeIntercept.getHeaders().put(Constants.HOSTIP_HEADER, ORIGINAL_HOST);
    Assert.assertEquals(ORIGINAL_HOST, eventBeforeIntercept.getHeaders()
            .get(Constants.HOSTIP_HEADER));

    String expectedHostIP = InetAddress.getLocalHost().getHostAddress();
    Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
    String actualHostIP = eventAfterIntercept.getHeaders().get(Constants.HOSTIP_HEADER);

    Assert.assertNotNull(actualHostIP);
    Assert.assertEquals(expectedHostIP, actualHostIP);
  }

  /**
   * Ensure only hostname is used.
   */
  @Test
  public void testUseHostnameOnly() throws Exception {
    Context ctx = new Context();
    ctx.put(Constants.USE_IP, "false");
    ctx.put(Constants.USE_HOSTNAME, "true");

    Interceptor.Builder builder = InterceptorBuilderFactory
            .newInstance(InterceptorType.HOST.toString());
    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    final String ORIGINAL_HOST = "originalhost";
    Event eventBeforeIntercept = EventBuilder.withBody("test event",
            Charsets.UTF_8);
    eventBeforeIntercept.getHeaders().put(Constants.HOSTNAME_HEADER, ORIGINAL_HOST);
    Assert.assertEquals(ORIGINAL_HOST, eventBeforeIntercept.getHeaders()
            .get(Constants.HOSTNAME_HEADER));

    String expectedHost = InetAddress.getLocalHost().getCanonicalHostName();
    Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
    String actualHost = eventAfterIntercept.getHeaders().get(Constants.HOSTNAME_HEADER);

    Assert.assertNotNull(actualHost);
    Assert.assertEquals(expectedHost, actualHost);
  }

}
