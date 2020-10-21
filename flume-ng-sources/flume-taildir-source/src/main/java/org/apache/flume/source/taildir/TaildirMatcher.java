/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.source.taildir;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

/**
 * Identifies and caches the files matched by single file pattern for {@code TAILDIR} source.
 * <p></p>
 * Since file patterns only apply to the fileNames and not the parent dictionaries, this
 * implementation checks the parent directory for modification (additional or removed files
 * update modification time of parent dir)
 * If no modification happened to the parent dir that means the underlying files could only be
 * written to but no need to rerun the pattern matching on fileNames.
 * <p></p>
 * This implementation provides lazy caching or no caching. Instances of this class keep the
 * result file list from the last successful execution of {@linkplain #getMatchingFiles()}
 * function invocation, and may serve the content without hitting the FileSystem for performance
 * optimization.
 * <p></p>
 * <b>IMPORTANT:</b> It is assumed that the hosting system provides at least second granularity
 * for both {@code System.currentTimeMillis()} and {@code File.lastModified()}. Also
 * that system clock is used for file system timestamps. If it is not the case then configure it
 * as uncached. Class is solely for package only usage. Member functions are not thread safe.
 *
 * @see TaildirSource
 * @see ReliableTaildirEventReader
 * @see TaildirSourceConfigurationConstants
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TaildirMatcher {
  private static final Logger logger = LoggerFactory.getLogger(TaildirMatcher.class);
  private static final FileSystem FS = FileSystems.getDefault();

  // flag from configuration to switch off caching completely
  private final boolean cachePatternMatching;
  // id from configuration
  private final String fileGroup;
  // matcher created with file group full path's regex
  private final PathMatcher fileMatcher;

  // plain string of the desired files from configuration
  private final String filePattern;

  // directory monitored for changes
  private final File parentDir;

  // system time in milliseconds, stores the last modification time of the
  // parent directory seen by the last check, rounded to seconds
  // initial value is used in first check only when it will be replaced instantly
  // (system time is positive)
  private long lastSeenParentDirMTime = -1;
  // system time in milliseconds, time of the last check, rounded to seconds
  // initial value is used in first check only when it will be replaced instantly
  // (system time is positive)
  private long lastCheckedTime = -1;
  // cached content, files which matched the pattern within the parent directory
  private List<File> lastMatchedFiles = Lists.newArrayList();
  // hour from configuration
  private final int ignoreHourBefore;

  /**
   * Package accessible constructor. From configuration context it represents a single
   * <code>filegroup</code> and encapsulates the corresponding <code>filePattern</code>.
   * <code>dirPath</code> a valid path to an existing parent directory
   * <code>filePath</code> a valid regex of path to a file, directory can be included in the path
   * {@link java.util.regex.Pattern} that match any non-hidden file names within parent directory
   * . A valid example for filePath is <code>dir1/.*</code> given
   * <code>/dir0</code> is an existing directory structure readable by the running user.
   * <p></p>
   * An instance of this class is created for each fileGroup
   *
   * @param fileGroup arbitrary name of the group given by the config
   * @param dirPath Absolute parent directory path of the file group. Neither wildcards nor regular
   *                expressions (and not file system patterns) are allowed in directory.
   * @param filePath Relative file path of the file group's parent directory. Directory can be
   *                 included. Regular expression (and not file system patterns) can be used
   *                 in directory and filename.
   * @param cachePatternMatching default true, recommended in every setup especially with huge
   *                             parent directories. Don't set when local system clock is not used
   *                             for stamping mtime (eg: remote filesystems)
   * @see TaildirSourceConfigurationConstants
   */
  TaildirMatcher(String fileGroup, String dirPath, String filePath,
                 boolean cachePatternMatching,int ignoreHourBefore) {
    // store whatever came from configuration
    this.fileGroup = fileGroup;
    File f = new File(dirPath + File.separator + filePath);
    this.filePattern = f.getAbsolutePath();
    this.cachePatternMatching = cachePatternMatching;
    this.parentDir = new File(dirPath);
    this.fileMatcher = FS.getPathMatcher("regex:" + filePattern);
    this.ignoreHourBefore = ignoreHourBefore;
    try {
      // sanity check
      Preconditions.checkState(parentDir.exists(),
              "Directory does not exist: " + parentDir.getAbsolutePath());
    } catch (IllegalStateException e) {
      logger.error("Directory does not exist: " + parentDir.getAbsolutePath());
    }
  }

  /**
   * Lists those files within the parentDir that match regex pattern passed in during object
   * instantiation. Designed for frequent periodic invocation
   * {@link org.apache.flume.source.PollableSourceRunner}.
   * <p></p>
   * Based on the modification of the parentDir this function may trigger cache recalculation by
   * calling {@linkplain #getMatchingFilesNoCache()} or
   * return the value stored in {@linkplain #lastMatchedFiles}.
   * Parentdir is allowed to be a symbolic link.
   * <p></p>
   * Files returned by this call are weakly consistent (see {@link DirectoryStream}).
   * It does not freeze the directory while iterating,
   * so it may (or may not) reflect updates to the directory that occur during the call,
   * In which case next call
   * will return those files (as mtime is increasing it won't hit cache but trigger recalculation).
   * It is guaranteed that invocation reflects every change which was observable at the time of
   * invocation.
   * <p></p>
   * Matching file list recalculation is triggered when caching was turned off or
   * if mtime is greater than the previously seen mtime
   * (including the case of cache hasn't been calculated before).
   * Additionally if a constantly updated directory was configured as parentDir
   * then multiple changes to the parentDir may happen
   * within the same second so in such case (assuming at least second granularity of reported mtime)
   * it is impossible to tell whether a change of the dir happened before the check or after
   * (unless the check happened after that second).
   * Having said that implementation also stores system time of the previous invocation and previous
   * invocation has to happen strictly after the current mtime to avoid further cache refresh
   * (because then it is guaranteed that previous invocation resulted in valid cache content).
   * If system clock hasn't passed the second of
   * the current mtime then logic expects more changes as well
   * (since it cannot be sure that there won't be any further changes still in that second
   * and it would like to avoid data loss in first place)
   * hence it recalculates matching files. If system clock finally
   * passed actual mtime then a subsequent invocation guarantees that it picked up every
   * change from the passed second so
   * any further invocations can be served from cache associated with that second
   * (given mtime is not updated again).
   *
   * @return List of files matching the pattern sorted by last modification time. No recursion.
   * No directories. If nothing matches then returns an empty list. If I/O issue occurred then
   * returns the list collected to the point when exception was thrown.
   *
   * @see #getMatchingFilesNoCache()
   */
  List<File> getMatchingFiles() {
    long now = TimeUnit.SECONDS.toMillis(
        TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
    long currentParentDirMTime = parentDir.lastModified();
    List<File> result;

    // calculate matched files if
    // - we don't want to use cache (recalculate every time) OR
    // - directory was clearly updated after the last check OR
    // - last mtime change wasn't already checked for sure
    //   (system clock hasn't passed that second yet)
    if (!cachePatternMatching ||
        lastSeenParentDirMTime < currentParentDirMTime ||
        !(currentParentDirMTime < lastCheckedTime)) {
      lastMatchedFiles = sortByLastModifiedTime(getMatchingFilesNoCache());
      lastSeenParentDirMTime = currentParentDirMTime;
      lastCheckedTime = now;
    }

    return lastMatchedFiles;
  }

  /**
   * Provides the actual files within the parentDir which
   * files are matching the regex pattern. Each invocation uses {@link FileVisitor}
   * to identify matching files.
   *
   * Files returned by this call is a visitor of each file in a file tree (see {@link FileVisitor}).
   *
   * @return List of files matching the pattern unsorted.
   * If nothing matches then returns an empty list. If I/O issue occurred then returns the list
   * collected to the point when exception was thrown.
   *
   * @see FileVisitor
   */
  private List<File> getMatchingFilesNoCache() {
    final List<File> result = Lists.newArrayList();
    try {
      Set options = EnumSet.of(FOLLOW_LINKS);
      Files.walkFileTree(Paths.get(parentDir.toString()), options, Integer.MAX_VALUE,
              new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            if (fileMatcher.matches(file.toAbsolutePath())) {
              if (file.toFile().lastModified() >= getDateMs()) {
                result.add(file.toFile());
              }
            }
            return FileVisitResult.CONTINUE;
          }
          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) {
            return FileVisitResult.CONTINUE;
          }
          });
    } catch (IOException e) {
      logger.error("I/O exception occurred while listing parent directory. " +
                   "Files already matched will be returned. " + parentDir.toPath(), e);
    } catch (IllegalArgumentException e) {
      logger.error("Filegroup Pattern (" + getFileGroup() + "=" + filePattern + ") Is Illegal.", e);
    }
    return result;
  }

  /**
   * Utility function to sort matched files based on last modification time.
   * Sorting itself use only a snapshot of last modification times captured before the sorting
   * to keep the number of stat system calls to the required minimum.
   *
   * @param files list of files in any order
   * @return sorted list
   */
  private static List<File> sortByLastModifiedTime(List<File> files) {
    final HashMap<File, Long> lastModificationTimes = new HashMap<File, Long>(files.size());
    for (File f: files) {
      lastModificationTimes.put(f, f.lastModified());
    }
    Collections.sort(files, new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        return lastModificationTimes.get(o1).compareTo(lastModificationTimes.get(o2));
      }
    });
    return files;
  }
  //diy 获取配置文件中指定的多少小时之前的时间戳
  public Long getDateMs(){
    Calendar calendar = Calendar.getInstance();
    calendar.set(Calendar.HOUR_OF_DAY,calendar.get(Calendar.HOUR_OF_DAY)-this.ignoreHourBefore);
    Long ignoreHourtoMillis = calendar.getTimeInMillis();
    logger.debug(
            "the timestamp from"+ignoreHourBefore+"hours ago was"+ignoreHourtoMillis
    );
    return ignoreHourtoMillis;
  }

  @Override
  public String toString() {
    return "{" +
        "filegroup='" + fileGroup + '\'' +
        ", filePattern='" + filePattern + '\'' +
        ", cached=" + cachePatternMatching +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TaildirMatcher that = (TaildirMatcher) o;

    return fileGroup.equals(that.fileGroup);

  }

  @Override
  public int hashCode() {
    return fileGroup.hashCode();
  }

  public String getFileGroup() {
    return fileGroup;
  }

}
