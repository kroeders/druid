/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.common.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.druid.client.indexing.ClientKillUnusedSegmentsTaskQuery;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.RetrieveUnusedSegmentsAction;
import org.apache.druid.indexing.common.actions.SegmentNukeAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskLocks;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The client representation of this task is {@link ClientKillUnusedSegmentsTaskQuery}.
 * JSON serialization fields of this class must correspond to those of {@link
 * ClientKillUnusedSegmentsTaskQuery}, except for "id" and "context" fields.
 */
public class KillUnusedSegmentsTask extends AbstractFixedIntervalTask
{
  private final Integer numThreads;
  private ExecutorService executorService;
  private static final String PATH_KEY = "path";
  private static final Logger log = new Logger(KillUnusedSegmentsTask.class);
  
  @JsonCreator
  public KillUnusedSegmentsTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("numThreads") Integer numThreads,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        getOrMakeId(id, "kill", dataSource, interval),
        dataSource,
        interval,
        context
    );
    this.numThreads = numThreads==null?1:numThreads;
  }

  @Override
  public String getType(){
    return "kill";
  }

  @JsonProperty("numThreads")
  public Integer getNumThreads() {
	  return numThreads;
  }
  
  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
	 if(this.executorService==null) {
	    this.executorService = Execs.multiThreaded(
                this.numThreads,
                StringUtils.format(
                    "KillUnusedSegmentTask-%s-%%d",
                    this.getId()
                )
            );		 
	 }
	final NavigableMap<DateTime, List<TaskLock>> taskLockMap = getTaskLockMap(toolbox.getTaskActionClient());
    // List unused segments
    final List<DataSegment> unusedSegments = toolbox
        .getTaskActionClient()
        .submit(new RetrieveUnusedSegmentsAction(getDataSource(), getInterval()));

    if (!TaskLocks.isLockCoversSegments(taskLockMap, unusedSegments)) {
      throw new ISE(
          "Locks[%s] for task[%s] can't cover segments[%s]",
          taskLockMap.values().stream().flatMap(List::stream).collect(Collectors.toList()),
          getId(),
          unusedSegments
      );
    }

    // Kill segments
    toolbox.getTaskActionClient().submit(new SegmentNukeAction(new HashSet<>(unusedSegments)));
    CountDownLatch latch = new CountDownLatch(unusedSegments.size());
    for (DataSegment segment : unusedSegments) {
    	executorService.submit(() -> {
	    	try {
				toolbox.getDataSegmentKiller().kill(segment);
			} catch (SegmentLoadingException e) {
				log.error(e, "Segment Loading Exception for %s with path%s ", segment.getId(), getPath(segment));
			} catch (Throwable t) {
				log.error(t, "Error Killing Segment %s with path ", segment.getId(), getPath(segment));
			} finally {
		    	latch.countDown();				
			}
    	});
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      log.error(e, "Kill Unused Segments Task interrupted");
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    return TaskStatus.success(getId());
  }
  private String getPath(DataSegment segment) {
	  return MapUtils.getString(segment.getLoadSpec(), PATH_KEY); 
  }

  private NavigableMap<DateTime, List<TaskLock>> getTaskLockMap(TaskActionClient client) throws IOException
  {
    final NavigableMap<DateTime, List<TaskLock>> taskLockMap = new TreeMap<>();
    getTaskLocks(client).forEach(
        taskLock -> taskLockMap.computeIfAbsent(taskLock.getInterval().getStart(), k -> new ArrayList<>()).add(taskLock)
    );
    return taskLockMap;
  }
}
