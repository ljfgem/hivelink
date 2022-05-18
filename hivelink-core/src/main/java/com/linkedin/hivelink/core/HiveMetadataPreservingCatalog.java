/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.hivelink.core;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveCatalog;


/**
 * A {@link HiveCatalog} which uses {@link com.linkedin.hivelink.core.HiveMetadataPreservingTableOperations} underneath.
 *
 * This catalog should be only be used by metadata publishers wanting to publish/update Iceberg metadata to an existing
 * Hive table while preserving the current Hive metadata
 */
public class HiveMetadataPreservingCatalog extends HiveCatalog {

  public HiveMetadataPreservingCatalog(Configuration conf) {
    super(conf);
  }

  private static final Cache<String, HiveMetadataPreservingCatalog> HIVE_METADATA_PRESERVING_CATALOG_CACHE =
      Caffeine.newBuilder().build();

  /**
   * @deprecated Use {@link #loadHiveMetadataPreservingCatalog(Configuration)} instead
   */
  @Deprecated
  public static HiveCatalog loadCustomCatalog(Configuration conf) {
    return loadHiveMetadataPreservingCatalog(conf);
  }

  public static HiveCatalog loadHiveMetadataPreservingCatalog(Configuration conf) {
    // metastore URI can be null in local mode
    String metastoreUri = conf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
    return HIVE_METADATA_PRESERVING_CATALOG_CACHE.get(metastoreUri, uri -> new HiveMetadataPreservingCatalog(conf));
  }

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    return new HiveMetadataPreservingTableOperations(conf(), clientPool(), new HadoopFileIO(conf()), name(), dbName,
        tableName);
  }
}
