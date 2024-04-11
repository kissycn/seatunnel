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

package org.apache.seatunnel.connectors.seatunnel.hive.source.config;

import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.*;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveHadoopConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTableUtils;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTypeConvertor;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.*;

@Getter
public class HiveSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Table table;
    private final CatalogTable catalogTable;
    private final FileFormat fileFormat;
    private final ReadStrategy readStrategy;
    private final List<String> filePaths;
    private final HiveHadoopConfig hiveHadoopConfig;

    @SneakyThrows
    public HiveSourceConfig(ReadonlyConfig readonlyConfig) {
        readonlyConfig
                .getOptional(HdfsSourceConfigOptions.READ_PARTITIONS)
                .ifPresent(this::validatePartitions);
        this.table = HiveTableUtils.getTableInfo(readonlyConfig);
        this.hiveHadoopConfig = parseHiveHadoopConfig(readonlyConfig, table);
        this.fileFormat = HiveTableUtils.parseFileFormat(table);
        this.readStrategy = parseReadStrategy(table, readonlyConfig, fileFormat, hiveHadoopConfig);
        this.filePaths = parseFilePaths(table, readStrategy);
        this.catalogTable =
                parseCatalogTable(
                        readonlyConfig,
                        readStrategy,
                        fileFormat,
                        hiveHadoopConfig,
                        filePaths,
                        table);
    }

    private void validatePartitions(List<String> partitionsList) {
        if (CollectionUtils.isEmpty(partitionsList)) {
            throw new HiveConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "Partitions list is empty, please check");
        }
        int depth = partitionsList.get(0).replaceAll("\\\\", "/").split("/").length;
        long count =
                partitionsList.stream()
                        .map(partition -> partition.replaceAll("\\\\", "/").split("/").length)
                        .filter(length -> length != depth)
                        .count();
        if (count > 0) {
            throw new HiveConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "Every partition that in partition list should has the same directory depth");
        }
    }

    private ReadStrategy parseReadStrategy(
            Table table,
            ReadonlyConfig readonlyConfig,
            FileFormat fileFormat,
            HiveHadoopConfig hiveHadoopConfig) {

        ReadStrategy readStrategy = ReadStrategyFactory.of(fileFormat.name());
        Config config = readonlyConfig.toConfig();

        switch (fileFormat) {
            case TEXT:
                // if the file format is text, we set the delim.
                Map<String, String> parameters = table.getSd().getSerdeInfo().getParameters();
                config =
                        config.withValue(
                                        FIELD_DELIMITER.key(),
                                        ConfigValueFactory.fromAnyRef(
                                                parameters.get("field.delim")))
                                .withValue(
                                        ROW_DELIMITER.key(),
                                        ConfigValueFactory.fromAnyRef(parameters.get("line.delim")))
                                .withValue(
                                        FILE_FORMAT_TYPE.key(),
                                        ConfigValueFactory.fromAnyRef(FileFormat.TEXT.name()));
                break;
            case ORC:
                config =
                        config.withValue(
                                FILE_FORMAT_TYPE.key(),
                                ConfigValueFactory.fromAnyRef(FileFormat.ORC.name()));
                break;
            case PARQUET:
                config =
                        config.withValue(
                                FILE_FORMAT_TYPE.key(),
                                ConfigValueFactory.fromAnyRef(FileFormat.PARQUET.name()));
                break;
            default:
        }
        readStrategy.setPluginConfig(config);
        readStrategy.init(hiveHadoopConfig);
        return readStrategy;
    }

    private HiveHadoopConfig parseHiveHadoopConfig(ReadonlyConfig readonlyConfig, Table table) {
        String fsDefaultName = parseFsDefaultName(table);
        HiveHadoopConfig hiveHadoopConfig =
                new HiveHadoopConfig(
                        fsDefaultName,
                        readonlyConfig.get(HiveSourceOptions.METASTORE_URI),
                        readonlyConfig.get(HiveSourceOptions.HIVE_SITE_PATH));
        readonlyConfig
                .getOptional(HdfsSourceConfigOptions.HDFS_SITE_PATH)
                .ifPresent(hiveHadoopConfig::setHdfsSitePath);
        readonlyConfig
                .getOptional(HdfsSourceConfigOptions.KERBEROS_PRINCIPAL)
                .ifPresent(hiveHadoopConfig::setKerberosPrincipal);
        readonlyConfig
                .getOptional(HdfsSourceConfigOptions.KERBEROS_KEYTAB_PATH)
                .ifPresent(hiveHadoopConfig::setKerberosKeytabPath);
        readonlyConfig
                .getOptional(HdfsSourceConfigOptions.REMOTE_USER)
                .ifPresent(hiveHadoopConfig::setRemoteUser);
        return hiveHadoopConfig;
    }

    private List<String> parseFilePaths(Table table, ReadStrategy readStrategy) {
        String hdfsPath = parseHdfsPath(table);
        try {
            return readStrategy.getFileNamesByPath(hdfsPath);
        } catch (Exception e) {
            String errorMsg = String.format("Get file list from this path [%s] failed", hdfsPath);
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_LIST_GET_FAILED, errorMsg, e);
        }
    }

    private String parseFsDefaultName(Table table) {
        String hdfsLocation = table.getSd().getLocation();
        try {
            URI uri = new URI(hdfsLocation);
            String path = uri.getPath();
            return hdfsLocation.replace(path, "");
        } catch (URISyntaxException e) {
            String errorMsg =
                    String.format(
                            "Get hdfs namenode host from table location [%s] failed,"
                                    + "please check it",
                            hdfsLocation);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HDFS_NAMENODE_HOST_FAILED, errorMsg, e);
        }
    }

    private String parseHdfsPath(Table table) {
        String hdfsLocation = table.getSd().getLocation();
        try {
            URI uri = new URI(hdfsLocation);
            return uri.getPath();
        } catch (URISyntaxException e) {
            String errorMsg =
                    String.format(
                            "Get hdfs namenode host from table location [%s] failed,"
                                    + "please check it",
                            hdfsLocation);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HDFS_NAMENODE_HOST_FAILED, errorMsg, e);
        }
    }

    private CatalogTable parseCatalogTable(
            ReadonlyConfig readonlyConfig,
            ReadStrategy readStrategy,
            FileFormat fileFormat,
            HiveHadoopConfig hiveHadoopConfig,
            List<String> filePaths,
            Table table) {
        switch (fileFormat) {
            case PARQUET:
            case ORC:
                return parseCatalogTableFromRemotePath(
                        readonlyConfig, hiveHadoopConfig, filePaths, table);
            case TEXT:
                return parseCatalogTableFromTable(readonlyConfig, readStrategy, table);
            default:
                throw new HiveConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "Hive connector only support [text parquet orc] table now");
        }
    }

    private CatalogTable parseCatalogTableFromRemotePath(
            ReadonlyConfig readonlyConfig,
            HiveHadoopConfig hiveHadoopConfig,
            List<String> filePaths,
            Table table) {
        if (CollectionUtils.isEmpty(filePaths)) {
            // When the directory is empty, distribute default behavior schema
            return buildEmptyCatalogTable(readonlyConfig, table);
        }
        CatalogTable catalogTable = buildEmptyCatalogTable(readonlyConfig, table);
        try {
            SeaTunnelRowType seaTunnelRowTypeInfo =
                    readStrategy.getSeaTunnelRowTypeInfo(filePaths.get(0));
            return CatalogTableUtil.newCatalogTable(catalogTable, seaTunnelRowTypeInfo);
        } catch (FileConnectorException e) {
            String errorMsg =
                    String.format("Get table schema from file [%s] failed", filePaths.get(0));
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED, errorMsg, e);
        }
    }

    private CatalogTable parseCatalogTableFromTable(
            ReadonlyConfig readonlyConfig, ReadStrategy readStrategy, Table table) {
        List<FieldSchema> cols = table.getSd().getCols();
        String[] fieldNames = new String[cols.size()];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            FieldSchema col = cols.get(i);
            fieldNames[i] = col.getName();
            fieldTypes[i] =
                    HiveTypeConvertor.covertHiveTypeToSeaTunnelType(col.getName(), col.getType());
        }

        SeaTunnelRowType seaTunnelRowType = new SeaTunnelRowType(fieldNames, fieldTypes);
        readStrategy.setSeaTunnelRowTypeInfo(seaTunnelRowType);
        final SeaTunnelRowType finalSeatunnelRowType = readStrategy.getActualSeaTunnelRowTypeInfo();

        CatalogTable catalogTable = buildEmptyCatalogTable(readonlyConfig, table);
        return CatalogTableUtil.newCatalogTable(catalogTable, finalSeatunnelRowType);
    }

    private CatalogTable buildEmptyCatalogTable(ReadonlyConfig readonlyConfig, Table table) {
        TablePath tablePath = TablePath.of(table.getDbName(), table.getTableName());
        return CatalogTable.of(
                TableIdentifier.of(HiveConstants.CONNECTOR_NAME, tablePath),
                TableSchema.builder().build(),
                new HashMap<>(),
                new ArrayList<>(),
                readonlyConfig.get(TableSchemaOptions.TableIdentifierOptions.COMMENT));
    }
}
