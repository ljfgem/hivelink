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
package com.linkedin.hivelink.spark;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalogs;

import com.linkedin.hivelink.core.LegacyHiveCatalog;


public class HivelinkSourceCommonUtils {
  public static Table findTable(Map<String, String> options, Configuration conf) {
    String path = options.get("path");
    TableIdentifier tableIdentifier = TableIdentifier.parse(path);

    try {
      // Here we need to choose which metadata to use for the table, currently Iceberg v/s Hive
      Catalog hiveMetadataBasedCatalog = LegacyHiveCatalog.loadLegacyCatalog(conf);
      Table hiveMetadataBasedTable = hiveMetadataBasedCatalog.loadTable(tableIdentifier);
      String tableType = hiveMetadataBasedTable.properties().getOrDefault("table_type", "");
      if ("iceberg".equalsIgnoreCase(tableType)) {
        Catalog catalog = HiveCatalogs.loadCatalog(conf);
        return catalog.loadTable(tableIdentifier);
      } else {
        return hiveMetadataBasedTable;
      }
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new IllegalArgumentException("Table not found: " + path);
    }
  }
}
