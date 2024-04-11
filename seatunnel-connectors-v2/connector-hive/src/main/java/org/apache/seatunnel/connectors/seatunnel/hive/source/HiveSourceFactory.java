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

package org.apache.seatunnel.connectors.seatunnel.hive.source;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;
import org.apache.seatunnel.connectors.seatunnel.hive.source.config.HiveSourceOptions;

import java.io.Serializable;

@AutoService(Factory.class)
public class HiveSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return HiveConstants.CONNECTOR_NAME;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new HiveSource(context.getOptions());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(HiveSourceOptions.TABLE_NAME)
                .optional(HiveSourceOptions.METASTORE_URI)
                .optional(HiveSourceOptions.TABLE_CONFIGS)
                .optional(HiveSourceOptions.HIVE_SITE_PATH)
                .optional(
                        HdfsSourceConfigOptions.HDFS_SITE_PATH,
                        HdfsSourceConfigOptions.REMOTE_USER,
                        HdfsSourceConfigOptions.KERBEROS_PRINCIPAL,
                        HdfsSourceConfigOptions.KERBEROS_KEYTAB_PATH)
                .optional(HdfsSourceConfigOptions.READ_PARTITIONS)
                .optional(HdfsSourceConfigOptions.READ_COLUMNS)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return HiveSource.class;
    }
}
