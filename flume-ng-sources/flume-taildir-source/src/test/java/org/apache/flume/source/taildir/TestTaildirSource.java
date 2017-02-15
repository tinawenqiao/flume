/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flume.source.taildir;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestTaildirSource {
  static TaildirSource source;
  static MemoryChannel channel;
  private File tmpDir;
  private String posFilePath;

  @Before
  public void setUp() {
    source = new TaildirSource();
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
    tmpDir = Files.createTempDir();
    posFilePath = tmpDir.getAbsolutePath() + "/taildir_position_test.json";
  }

  @After
  public void tearDown() {
    for (File f : tmpDir.listFiles()) {
      f.delete();
    }
    tmpDir.delete();
  }

  @Test
  public void testRegexFileNameFilteringEndToEnd() throws IOException {
    File f1 = new File(tmpDir, "a.log");
    File f2 = new File(tmpDir, "a.log.1");
    File f3 = new File(tmpDir, "b.log");
    File f4 = new File(tmpDir, "c.log.yyyy-MM-01");
    File f5 = new File(tmpDir, "c.log.yyyy-MM-02");
    Files.write("a.log\n", f1, Charsets.UTF_8);
    Files.write("a.log.1\n", f2, Charsets.UTF_8);
    Files.write("b.log\n", f3, Charsets.UTF_8);
    Files.write("c.log.yyyy-MM-01\n", f4, Charsets.UTF_8);
    Files.write("c.log.yyyy-MM-02\n", f5, Charsets.UTF_8);

    Context context = new Context();
    context.put(POSITION_FILE, posFilePath);
    context.put(FILE_GROUPS, "ab c");
    // Tail a.log and b.log
    context.put(FILE_GROUPS_PREFIX + "ab", tmpDir.getAbsolutePath() + "/[ab].log");
    // Tail files that starts with c.log
    context.put(FILE_GROUPS_PREFIX + "c", tmpDir.getAbsolutePath() + "/c.log.*");

    Configurables.configure(source, context);
    source.start();
    source.process();
    Transaction txn = channel.getTransaction();
    txn.begin();
    List<String> out = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      Event e = channel.take();
      if (e != null) {
        out.add(TestTaildirEventReader.bodyAsString(e));
      }
    }
    txn.commit();
    txn.close();

    assertEquals(4, out.size());
    // Make sure we got every file
    assertTrue(out.contains("a.log"));
    assertFalse(out.contains("a.log.1"));
    assertTrue(out.contains("b.log"));
    assertTrue(out.contains("c.log.yyyy-MM-01"));
    assertTrue(out.contains("c.log.yyyy-MM-02"));
  }

  @Test
  public void testHeaderMapping() throws IOException {
    File f1 = new File(tmpDir, "file1");
    File f2 = new File(tmpDir, "file2");
    File f3 = new File(tmpDir, "file3");
    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);
    Files.write("file3line1\nfile3line2\n", f3, Charsets.UTF_8);

    Context context = new Context();
    context.put(POSITION_FILE, posFilePath);
    context.put(FILE_GROUPS, "f1 f2 f3");
    context.put(FILE_GROUPS_PREFIX + "f1", tmpDir.getAbsolutePath() + "/file1$");
    context.put(FILE_GROUPS_PREFIX + "f2", tmpDir.getAbsolutePath() + "/file2$");
    context.put(FILE_GROUPS_PREFIX + "f3", tmpDir.getAbsolutePath() + "/file3$");
    context.put(HEADERS_PREFIX + "f1.headerKeyTest", "value1");
    context.put(HEADERS_PREFIX + "f2.headerKeyTest", "value2");
    context.put(HEADERS_PREFIX + "f2.headerKeyTest2", "value2-2");

    Configurables.configure(source, context);
    source.start();
    source.process();
    Transaction txn = channel.getTransaction();
    txn.begin();
    for (int i = 0; i < 6; i++) {
      Event e = channel.take();
      String body = new String(e.getBody(), Charsets.UTF_8);
      String headerValue = e.getHeaders().get("headerKeyTest");
      String headerValue2 = e.getHeaders().get("headerKeyTest2");
      if (body.startsWith("file1")) {
        assertEquals("value1", headerValue);
        assertNull(headerValue2);
      } else if (body.startsWith("file2")) {
        assertEquals("value2", headerValue);
        assertEquals("value2-2", headerValue2);
      } else if (body.startsWith("file3")) {
        // No header
        assertNull(headerValue);
        assertNull(headerValue2);
      }
    }
    txn.commit();
    txn.close();
  }

  @Test
  public void testLifecycle() throws IOException, InterruptedException {
    File f1 = new File(tmpDir, "file1");
    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);

    Context context = new Context();
    context.put(POSITION_FILE, posFilePath);
    context.put(FILE_GROUPS, "f1");
    context.put(FILE_GROUPS_PREFIX + "f1", tmpDir.getAbsolutePath() + "/file1$");
    Configurables.configure(source, context);

    for (int i = 0; i < 3; i++) {
      source.start();
      source.process();
      assertTrue("Reached start or error", LifecycleController.waitForOneOf(
          source, LifecycleState.START_OR_ERROR));
      assertEquals("Server is started", LifecycleState.START,
          source.getLifecycleState());

      source.stop();
      assertTrue("Reached stop or error",
          LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
      assertEquals("Server is stopped", LifecycleState.STOP,
          source.getLifecycleState());
    }
  }

  @Test
  public void testFileConsumeOrder() throws IOException {
    System.out.println(tmpDir.toString());
    // 1) Create 1st file
    File f1 = new File(tmpDir, "file1");
    String line1 = "file1line1\n";
    String line2 = "file1line2\n";
    String line3 = "file1line3\n";
    Files.write(line1 + line2 + line3, f1, Charsets.UTF_8);
    try {
      Thread.sleep(1000); // wait before creating a new file
    } catch (InterruptedException e) {
    }

    // 1) Create 2nd file
    String line1b = "file2line1\n";
    String line2b = "file2line2\n";
    String line3b = "file2line3\n";
    File f2 = new File(tmpDir, "file2");
    Files.write(line1b + line2b + line3b, f2, Charsets.UTF_8);
    try {
      Thread.sleep(1000); // wait before creating next file
    } catch (InterruptedException e) {
    }

    // 3) Create 3rd file
    String line1c = "file3line1\n";
    String line2c = "file3line2\n";
    String line3c = "file3line3\n";
    File f3 = new File(tmpDir, "file3");
    Files.write(line1c + line2c + line3c, f3, Charsets.UTF_8);

    try {
      Thread.sleep(1000); // wait before creating a new file
    } catch (InterruptedException e) {
    }


    // 4) Create 4th file
    String line1d = "file4line1\n";
    String line2d = "file4line2\n";
    String line3d = "file4line3\n";
    File f4 = new File(tmpDir, "file4");
    Files.write(line1d + line2d + line3d, f4, Charsets.UTF_8);

    try {
      Thread.sleep(1000); // wait before creating a new file
    } catch (InterruptedException e) {
    }


    // 5) Now update the 3rd file so that its the latest file and gets consumed last
    f3.setLastModified(System.currentTimeMillis());

    // 4) Consume the files
    ArrayList<String> consumedOrder = Lists.newArrayList();
    Context context = new Context();
    context.put(POSITION_FILE, posFilePath);
    context.put(FILE_GROUPS, "g1");
    context.put(FILE_GROUPS_PREFIX + "g1", tmpDir.getAbsolutePath() + "/.*");

    Configurables.configure(source, context);
    source.start();
    source.process();
    Transaction txn = channel.getTransaction();
    txn.begin();
    for (int i = 0; i < 12; i++) {
      Event e = channel.take();
      String body = new String(e.getBody(), Charsets.UTF_8);
      consumedOrder.add(body);
    }
    txn.commit();
    txn.close();

    System.out.println(consumedOrder);

    // 6) Ensure consumption order is in order of last update time
    ArrayList<String> expected = Lists.newArrayList(line1, line2, line3,    // file1
                                                    line1b, line2b, line3b, // file2
                                                    line1d, line2d, line3d, // file4
                                                    line1c, line2c, line3c  // file3
                                                   );
    for (int i = 0; i != expected.size(); ++i) {
      expected.set(i, expected.get(i).trim());
    }
    assertArrayEquals("Files not consumed in expected order", expected.toArray(),
                      consumedOrder.toArray());
  }

  @Test
  public void testPutFilenameHeader() throws IOException {
    File f1 = new File(tmpDir, "file1");
    Files.write("f1\n", f1, Charsets.UTF_8);

    Context context = new Context();
    context.put(POSITION_FILE, posFilePath);
    context.put(FILE_GROUPS, "fg");
    context.put(FILE_GROUPS_PREFIX + "fg", tmpDir.getAbsolutePath() + "/file.*");
    context.put(FILENAME_HEADER, "true");
    context.put(FILENAME_HEADER_KEY, "path");

    Configurables.configure(source, context);
    source.start();
    source.process();
    Transaction txn = channel.getTransaction();
    txn.begin();
    Event e = channel.take();
    txn.commit();
    txn.close();

    assertNotNull(e.getHeaders().get("path"));
    assertEquals(f1.getAbsolutePath(),
            e.getHeaders().get("path"));
  }

  @Test
  public void testMultilineSetToTrue() throws IOException {
    File f1 = new File(tmpDir, "a.log");
    //File f2 = new File(tmpDir, "b.log");
    //File f3 = new File(tmpDir, "c.log");
    Files.write("2016-05-17 00:12:06,713 [Thread-7] TRACE \n f1\n g1 \n h1\n \n 2016-05-18 00:12:06,714 [Thread-7] TRACE \n" +
            " f1f1 \n" +
            " f1f1f1", f1, Charsets.UTF_8);
    /*
    String inStr = "";
    for (int i=0; i<20001; i++) {
      inStr = inStr + "a";
    }
    Files.write(inStr + "b.log\nf1\n2016-05-17 00:12:06,713 [Thread-7] TRACE \n f1 \n 2016-05-18 00:12:06,714 [Thread-7] TRACE \n" +
            " f1f1 \n" +
            " f1f1f1", f1, Charsets.UTF_8);
    */
    //Files.write(" 2016-05-19 00:12:06,713 [Thread-7] TRACE \n f2 \n f2f2\n2016-05-20 00:12:06,713 [Thread-7] TRACE\n ", f2, Charsets.UTF_8);
    //Files.write("c.log\n", f3, Charsets.UTF_8);

    Context context = new Context();
    context.put(POSITION_FILE, posFilePath);
    context.put(FILE_GROUPS, "f1");
    // Tail a.log and b.log
    context.put(FILE_GROUPS_PREFIX + "f1", tmpDir.getAbsolutePath() + "/[abc].log");
    context.put(MULTILINE, "true");
    context.put(MULTILINE_PATTERN, "\\d\\d\\d\\d-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d,\\d\\d\\d");
    //context.put(MULTILINE_PATTERN_BELONG, "next");
    context.put(MULTILINE_PATTERN_BELONG, "previous");
    context.put(MULTILINE_PATTERN_MATCHED, "false");
    context.put(MULTILINE_MAX_BYTES, "9000");
    context.put(MULTILINE_MAX_LINES, "2");

    Configurables.configure(source, context);
    source.start();
    for (int i = 0; i<5; i++) {
      source.process();
      Transaction txn = channel.getTransaction();
      txn.begin();
      List<String> out = Lists.newArrayList();
      for (int j=0; j<5; j++) {
        System.out.println("j=" + j);
        Event e = channel.take();
        if (e != null) {
          System.out.println("j=" + j + ": " + TestTaildirEventReader.bodyAsString(e));
          out.add(TestTaildirEventReader.bodyAsString(e));
        }
      }
      txn.commit();
      txn.close();
    }
    /*
    assertEquals(3, out.size());

    assertTrue(out.get(0).equals("a.log\n"));
    assertTrue(out.get(1).equals("\n2016-05-17 00:12:06,713 [Thread-7] TRACE \n" +
            " f1 \n"));

    assertTrue(out.get(2).equals(" 2016-05-19 00:12:06,713 [Thread-7] TRACE \n" +
            " f2 \n" +
            " f2f2"));
            */
  }

  @Test
  public void testMultilineBelongPreMatchedFalse() throws IOException {
    File f1 = new File(tmpDir, "file1");
    Files.write("2017-01-01 00:00:01,111 line1\nline2\n" +
            "2017-01-02 00:00:02,222 line3\nline4\nline5\n" +
            "2017-01-03 00:00:03,333 line6\nline7\nline8\nline9\n", f1, Charsets.UTF_8);

    Context context = new Context();
    context.put(POSITION_FILE, posFilePath);
    context.put(FILE_GROUPS, "f1");
    context.put(FILE_GROUPS_PREFIX + "f1", tmpDir.getAbsolutePath() + "/file1");
    context.put(MULTILINE, "true");
    context.put(MULTILINE_PATTERN, "\\d\\d\\d\\d-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d,\\d\\d\\d");
    context.put(MULTILINE_PATTERN_BELONG, "previous");
    context.put(MULTILINE_PATTERN_MATCHED, "false");

    Configurables.configure(source, context);
    source.start();
    source.process();
    Transaction txn = channel.getTransaction();
    txn.begin();
    List<String> out = Lists.newArrayList();
    for (int j=0; j<9; j++) {
      Event e = channel.take();
      if (e != null) {
        out.add(TestTaildirEventReader.bodyAsString(e));
      }
    }
    txn.commit();
    txn.close();
    assertEquals(2, out.size());

    assertTrue(out.get(0).equals("2017-01-01 00:00:01,111 line1\nline2\n"));
    assertTrue(out.get(1).equals("2017-01-02 00:00:02,222 line3\nline4\nline5\n"));
  }

  @Test
  public void testMultilineBelongPreMatchedTrue() throws IOException {
    File f1 = new File(tmpDir, "file1");
    Files.write("2017-01-01 00:00:01,111 line1\nline2\n" +
            "2017-01-02 00:00:02,222 line3\nline4\nline5\n" +
            "2017-01-03 00:00:03,333 line6\nline7\nline8\nline9\n", f1, Charsets.UTF_8);

    Context context = new Context();
    context.put(POSITION_FILE, posFilePath);
    context.put(FILE_GROUPS, "f1");
    context.put(FILE_GROUPS_PREFIX + "f1", tmpDir.getAbsolutePath() + "/file1");
    context.put(MULTILINE, "true");
    context.put(MULTILINE_PATTERN, "\\d\\d\\d\\d-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d,\\d\\d\\d");
    context.put(MULTILINE_PATTERN_BELONG, "previous");
    context.put(MULTILINE_PATTERN_MATCHED, "true");

    Configurables.configure(source, context);
    source.start();
    source.process();
    Transaction txn = channel.getTransaction();
    txn.begin();
    List<String> out = Lists.newArrayList();
    for (int j=0; j<9; j++) {
      Event e = channel.take();
      if (e != null) {
        System.out.println("j=" + j + ": " + TestTaildirEventReader.bodyAsString(e));
        out.add(TestTaildirEventReader.bodyAsString(e));
      }
    }
    txn.commit();
    txn.close();
    assertEquals(6, out.size());

    assertTrue(out.get(0).equals("2017-01-01 00:00:01,111 line1\n"));
    assertTrue(out.get(1).equals("line2\n2017-01-02 00:00:02,222 line3\n"));
    assertTrue(out.get(2).equals("line4\n"));
    assertTrue(out.get(3).equals("line5\n2017-01-03 00:00:03,333 line6\n"));
    assertTrue(out.get(4).equals("line7\n"));
    assertTrue(out.get(5).equals("line8\n"));
  }

  @Test
  public void testMultilineBelongNextMatchedFalse() throws IOException {
    File f1 = new File(tmpDir, "file1");
    Files.write("2017-01-01 00:00:01,111 line1\nline2\n" +
            "2017-01-02 00:00:02,222 line3\nline4\nline5\n" +
            "2017-01-03 00:00:03,333 line6\nline7\nline8\nline9\n", f1, Charsets.UTF_8);

    Context context = new Context();
    context.put(POSITION_FILE, posFilePath);
    context.put(FILE_GROUPS, "f1");
    context.put(FILE_GROUPS_PREFIX + "f1", tmpDir.getAbsolutePath() + "/file1");
    context.put(MULTILINE, "true");
    context.put(MULTILINE_PATTERN, "\\d\\d\\d\\d-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d,\\d\\d\\d");
    context.put(MULTILINE_PATTERN_BELONG, "next");
    context.put(MULTILINE_PATTERN_MATCHED, "false");

    Configurables.configure(source, context);
    source.start();
    source.process();
    Transaction txn = channel.getTransaction();
    txn.begin();
    List<String> out = Lists.newArrayList();
    for (int j=0; j<9; j++) {
      Event e = channel.take();
      if (e != null) {
        System.out.println("j=" + j + ": " + TestTaildirEventReader.bodyAsString(e));
        out.add(TestTaildirEventReader.bodyAsString(e));
      }
    }
    txn.commit();
    txn.close();
    assertEquals(3, out.size());

    assertTrue(out.get(0).equals("2017-01-01 00:00:01,111 line1\n"));
    assertTrue(out.get(1).equals("line2\n2017-01-02 00:00:02,222 line3\n"));
    assertTrue(out.get(2).equals("line4\nline5\n2017-01-03 00:00:03,333 line6\n"));
  }

  @Test
  public void testMultilineBelongNextMatchedTrue() throws IOException {
    File f1 = new File(tmpDir, "file1");
    Files.write("2017-01-01 00:00:01,111 line1\nline2\n" +
            "2017-01-02 00:00:02,222 line3\nline4\nline5\n" +
            "2017-01-03 00:00:03,333 line6\nline7\nline8\nline9\n", f1, Charsets.UTF_8);

    Context context = new Context();
    context.put(POSITION_FILE, posFilePath);
    context.put(FILE_GROUPS, "f1");
    context.put(FILE_GROUPS_PREFIX + "f1", tmpDir.getAbsolutePath() + "/file1");
    context.put(MULTILINE, "true");
    context.put(MULTILINE_PATTERN, "\\d\\d\\d\\d-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d,\\d\\d\\d");
    context.put(MULTILINE_PATTERN_BELONG, "next");
    context.put(MULTILINE_PATTERN_MATCHED, "true");

    Configurables.configure(source, context);
    source.start();
    source.process();
    Transaction txn = channel.getTransaction();
    txn.begin();
    List<String> out = Lists.newArrayList();
    for (int j=0; j<9; j++) {
      Event e = channel.take();
      if (e != null) {
        System.out.println("j=" + j + ": " + TestTaildirEventReader.bodyAsString(e));
        out.add(TestTaildirEventReader.bodyAsString(e));
      }
    }
    txn.commit();
    txn.close();
    assertEquals(6, out.size());

    assertTrue(out.get(0).equals("2017-01-01 00:00:01,111 line1\nline2\n"));
    assertTrue(out.get(1).equals("2017-01-02 00:00:02,222 line3\nline4\n"));
    assertTrue(out.get(2).equals("line5\n"));
    assertTrue(out.get(3).equals("2017-01-03 00:00:03,333 line6\nline7\n"));
    assertTrue(out.get(4).equals("line8\n"));
    assertTrue(out.get(5).equals("line9\n"));
  }

  @Test
  public void testMultilineMaxBytesAndMaxLines() throws IOException {
    File f1 = new File(tmpDir, "file1");
    String longStr = "";
    for (int i=0; i<10; i++) {
      longStr = longStr + "long";
    }
    Files.write("2017-01-01 00:00:01,111 line11\nline12\nline13\nline14\nline15\n" +
            "2017-01-02 00:00:02,222 line21\nline22\nline23\nline24\nline25\n" +
            "2017-01-03 00:00:03,333 line31\nline32\nline33\nline34\nline35\n",
            f1, Charsets.UTF_8);

    Context context = new Context();
    context.put(POSITION_FILE, posFilePath);
    context.put(FILE_GROUPS, "f1");
    context.put(FILE_GROUPS_PREFIX + "f1", tmpDir.getAbsolutePath() + "/file1");
    context.put(MULTILINE, "true");
    context.put(MULTILINE_PATTERN, "\\d\\d\\d\\d-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d,\\d\\d\\d");
    context.put(MULTILINE_PATTERN_BELONG, "previous");
    context.put(MULTILINE_PATTERN_MATCHED, "false");
    context.put(MULTILINE_MAX_BYTES, "25");
    context.put(MULTILINE_MAX_LINES, "4");

    Configurables.configure(source, context);
    source.start();
    source.process();
    Transaction txn = channel.getTransaction();
    txn.begin();
    List<String> out = Lists.newArrayList();
    for (int j=0; j<90; j++) {
      Event e = channel.take();
      if (e != null) {
        System.out.println("j=" + j + ": " + TestTaildirEventReader.bodyAsString(e));
        out.add(TestTaildirEventReader.bodyAsString(e));
      }
    }
    txn.commit();
    txn.close();

    /*
    assertEquals(6, out.size());

    assertTrue(out.get(0).equals("2017-01-01 00:00:01,111 line11\nline12\nline13\nline14\n"));
    assertTrue(out.get(1).equals("line15\n"));
    assertTrue(out.get(2).equals("2017-01-02 00:00:02,222 " + longStr.substring(0, 10000-24)));
    assertTrue(out.get(3).equals("2017-01-03 00:00:03,333 line31\nline32\nline33\nline34\n"));
    */
  }

  @Test
  public void testMultilineEventTimeout() throws IOException {
    File f1 = new File(tmpDir, "file1");
    Files.write("2017-01-01 00:00:01,111 line1\nline2\n" +
            "2017-01-02 00:00:02,222 line3\nline4\nline5\n" +
            "2017-01-03 00:00:03,333 line6\nline7\nline8\nline9\n", f1, Charsets.UTF_8);

    Context context = new Context();
    context.put(POSITION_FILE, posFilePath);
    context.put(FILE_GROUPS, "fg");
    context.put(FILE_GROUPS_PREFIX + "fg", tmpDir.getAbsolutePath() + "/file1");
    context.put(MULTILINE, "true");
    context.put(MULTILINE_PATTERN, "\\d\\d\\d\\d-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d,\\d\\d\\d");
    context.put(MULTILINE_PATTERN_BELONG, "previous");
    context.put(MULTILINE_PATTERN_MATCHED, "false");
    context.put(MULTILINE_EVENT_TIMEOUT_SECONDS, "60");

    Configurables.configure(source, context);
    source.start();
    source.process();
    Transaction txn = channel.getTransaction();
    txn.begin();
    List<String> out = Lists.newArrayList();
    for (int j=0; j<2; j++) {
      System.out.println("++++j=" + j);
      Event e = channel.take();
      if (e != null) {
        System.out.println("j=" + j + ": " + TestTaildirEventReader.bodyAsString(e));
        out.add(TestTaildirEventReader.bodyAsString(e));
      }
    }
    txn.commit();
    txn.close();

    try {
      Thread.sleep(60000);
    } catch (InterruptedException e) {
    }

    source.process();
    txn = channel.getTransaction();
    txn.begin();
    for (int j=0; j<2; j++) {
      System.out.println("++++2222j=" + j);
      Event e = channel.take();
      if (e != null) {
        System.out.println("22222j=" + j + ": " + TestTaildirEventReader.bodyAsString(e));
        out.add(TestTaildirEventReader.bodyAsString(e));
      }
    }
    txn.commit();
    txn.close();

    assertEquals(3, out.size());

    assertTrue(out.get(0).equals("2017-01-01 00:00:01,111 line1\nline2\n"));
    assertTrue(out.get(1).equals("2017-01-02 00:00:02,222 line3\nline4\nline5\n"));
    assertTrue(out.get(2).equals("2017-01-03 00:00:03,333 line6\nline7\nline8\nline9\n"));
  }

}
