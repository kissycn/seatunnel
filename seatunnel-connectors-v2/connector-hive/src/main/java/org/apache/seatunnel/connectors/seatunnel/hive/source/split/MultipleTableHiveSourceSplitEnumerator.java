/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hive.source.split;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.hive.source.config.HiveSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.source.config.MultipleTableHiveSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.source.state.HiveSourceState;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class MultipleTableHiveSourceSplitEnumerator
        implements SourceSplitEnumerator<HiveSourceSplit, HiveSourceState> {

    private final Context<HiveSourceSplit> context;
    private final Set<HiveSourceSplit> pendingSplit;
    private final Set<HiveSourceSplit> assignedSplit;
    private final Map<String, List<String>> filePathMap;

    public MultipleTableHiveSourceSplitEnumerator(
            Context<HiveSourceSplit> context,
            MultipleTableHiveSourceConfig multipleTableLocalFileSourceConfig) {
        this.context = context;
        this.filePathMap =
                multipleTableLocalFileSourceConfig.getHiveSourceConfigs().stream()
                        .collect(
                                Collectors.toMap(
                                        localFileSourceConfig ->
                                                localFileSourceConfig
                                                        .getCatalogTable()
                                                        .getTableId()
                                                        .toTablePath()
                                                        .toString(),
                                        HiveSourceConfig::getFilePaths));
        this.assignedSplit = new HashSet<>();
        this.pendingSplit = new HashSet<>();
    }

    public MultipleTableHiveSourceSplitEnumerator(
            Context<HiveSourceSplit> context,
            MultipleTableHiveSourceConfig multipleTableLocalFileSourceConfig,
            HiveSourceState localFileSourceState) {
        this(context, multipleTableLocalFileSourceConfig);
        this.assignedSplit.addAll(localFileSourceState.getAssignedSplit());
    }

    @Override
    public void addSplitsBack(List<HiveSourceSplit> splits, int subtaskId) {
        if (CollectionUtils.isEmpty(splits)) {
            return;
        }
        pendingSplit.addAll(splits);
        assignSplit(subtaskId);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {
        for (Map.Entry<String, List<String>> filePathEntry : filePathMap.entrySet()) {
            String tableId = filePathEntry.getKey();
            List<String> filePaths = filePathEntry.getValue();
            for (String filePath : filePaths) {
                pendingSplit.add(new HiveSourceSplit(tableId, filePath));
            }
        }
        assignSplit(subtaskId);
    }

    @Override
    public HiveSourceState snapshotState(long checkpointId) {
        return new HiveSourceState(assignedSplit);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing.
    }

    private void assignSplit(int taskId) {
        List<HiveSourceSplit> currentTaskSplits = new ArrayList<>();
        if (context.currentParallelism() == 1) {
            // if parallelism == 1, we should assign all the splits to reader
            currentTaskSplits.addAll(pendingSplit);
        } else {
            // if parallelism > 1, according to hashCode of split's id to determine whether to
            // allocate the current task
            for (HiveSourceSplit fileSourceSplit : pendingSplit) {
                int splitOwner =
                        getSplitOwner(fileSourceSplit.splitId(), context.currentParallelism());
                if (splitOwner == taskId) {
                    currentTaskSplits.add(fileSourceSplit);
                }
            }
        }
        // assign splits
        context.assignSplit(taskId, currentTaskSplits);
        // save the state of assigned splits
        assignedSplit.addAll(currentTaskSplits);
        // remove the assigned splits from pending splits
        currentTaskSplits.forEach(pendingSplit::remove);
        log.info(
                "SubTask {} is assigned to [{}]",
                taskId,
                currentTaskSplits.stream()
                        .map(HiveSourceSplit::splitId)
                        .collect(Collectors.joining(",")));
        context.signalNoMoreSplits(taskId);
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    @Override
    public void open() {
        // do nothing
    }

    @Override
    public void run() throws Exception {
        // do nothing
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
