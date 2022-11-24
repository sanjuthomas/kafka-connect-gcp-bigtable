/*
 *
 *  Copyright (c) 2023 Sanju Thomas
 *
 *  Licensed under the MIT License (the "License");
 *  you may not use this file except in compliance with the License.
 *
 *  You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  either express or implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 */

package com.sanjuthomas.gcp.bigtable.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.ToString;
import org.apache.kafka.common.annotation.InterfaceStability.Stable;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.sanjuthomas.gcp.bigtable.config.WriterConfig.ErrorHandlerConfig;

/**
 * 
 * An in memory copy of the configuration. An example configuration is given below.
 * 
 * keyFile: secret-key-file-to-connect-to-gcp
 * project: name-of-the-gcp-project
 * instance: name-of-the-bigtable-instance
 * table: name-of-the-bigtable-table
 * continueAfterWriteError: true
 * bulkMutateRowsMaxSize: 1024
 * transformer: canonical-name-of-the-transformer-class
 * errorHandler:
 *  maxRetryCount: count-in-integer
 *  retryBackoffSeconds: seconds-in-integer
 *  exponentialBackoff: true or false
 *  keyQualifiers:
 *   - element-name-1
 *   - element-name-n
 *  keyDelimiter: delimiter-to-combine-key-qualifiers
 *   families:
 *     - family-name-one
 *     - family-name-n
 * familyQualifiers:
 *  - family-name-one:
 *   - column-name-one
 *   - column-name-n
 *  - family-name-n:
 *   - column-name-one
 *   - column-name-n
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Stable
@ToString
public class Config {

  private String keyFile;
  private String project;
  private String instance;
  private String table;
  private Integer bulkMutateRowsMaxSize;
  private String transformer;
  private List<String> keyQualifiers;
  private String keyDelimiter;
  private List<String> families;
  private List<Map<String, List<String>>> familyQualifiers;
  private Integer maxRetryCount;
  private Integer retryBackoffSeconds;
  private Boolean exponentialBackoff;
  private Boolean continueAfterWriteError;

  public Map<String, List<String>> familyQualifiersMappings() {
    final Map<String, List<String>> familyQualifiersMappings = new HashMap<>();
    for (final Map<String, List<String>> familyQualifiers : this.familyQualifiers()) {
      familyQualifiersMappings.putAll(familyQualifiers);
    }
    return familyQualifiersMappings;
  }

  public WriterConfig getWriterConfig() {
    final WriterConfig writerConfg = new WriterConfig(keyFile, project, instance, table, bulkMutateRowsMaxSize(), continueAfterWriteError());
    writerConfg.setErrorHandlerConfig(
        new ErrorHandlerConfig(maxRetryCount(), retryBackoffSeconds(), exponentialBackoff()));
    return writerConfg;
  }

  public String transformer() {
    return MoreObjects.firstNonNull(transformer, "com.sanjuthomas.gcp.bigtable.transform.JsonEventTransformer");
  }

  public List<String> keyQualifiers() {
    return MoreObjects.firstNonNull(this.keyQualifiers, new ArrayList<String>(0));
  }

  public String keyDelimiter() {
    return MoreObjects.firstNonNull(this.keyDelimiter, "");
  }

  public List<String> families() {
    return this.families;
  }

  private List<Map<String, List<String>>> familyQualifiers() {
    return MoreObjects.firstNonNull(this.familyQualifiers,
        new ArrayList<>(0));
  }

  public void setKeyFile(final String keyFile) {
    Preconditions.checkNotNull(keyFile, "keyFile is a mandatory configuration");
    this.keyFile = keyFile;
  }

  public void setProject(final String project) {
    Preconditions.checkNotNull(project, "project is a mandatory configuration");
    this.project = project;
  }

  public void setInstance(final String instance) {
    Preconditions.checkNotNull(instance, "instance is a mandatory configuration");
    this.instance = instance;
  }

  public void setTable(final String table) {
    Preconditions.checkNotNull(table, "table is a mandatory configuration.");
    this.table = table;
  }

  public void setTransformer(final String transformer) {
    this.transformer = transformer;
  }

  public void setKeyQualifiers(final List<String> keyQualifiers) {
    this.keyQualifiers = keyQualifiers;
  }

  public void setKeyDelimiter(final String keyDelimiter) {
    this.keyDelimiter = keyDelimiter;
  }

  public void setFamilies(final List<String> families) {
    Preconditions.checkNotNull(families, "family is a mandatory configuration.");
    Preconditions.checkArgument(families.size() > 0, "at least one family should be given.");
    this.families = families;
  }

  public void setFamilyQualifiers(final List<Map<String, List<String>>> familyQualifiers) {
    this.familyQualifiers = familyQualifiers;
  }

  public void setErrorHandler(final Map<String, Object> errorHandler) {
    this.maxRetryCount = Integer.valueOf(Objects.toString(errorHandler.get("maxRetryCount"), "3"));
    this.retryBackoffSeconds = Integer.valueOf(Objects.toString(errorHandler.get("retryBackoffSeconds"), "3"));
    this.exponentialBackoff = Boolean.valueOf(Objects.toString(errorHandler.get("exponentialBackoff"), "true"));
  }
  
  private Integer maxRetryCount() {
    return Integer.valueOf(Objects.toString(maxRetryCount, "3"));
  }
  
  private Integer retryBackoffSeconds() {
    return Integer.valueOf(Objects.toString(retryBackoffSeconds, "3"));
  }
  
  private Boolean exponentialBackoff() {
    return Boolean.valueOf(Objects.toString(exponentialBackoff, "true"));
  }
  
  public void setBulkMutateRowsMaxSize(final Integer bulkMutateRowsMaxSize) {
    this.bulkMutateRowsMaxSize = bulkMutateRowsMaxSize;
  }
  
  private Integer bulkMutateRowsMaxSize() {
    return Integer.valueOf(Objects.toString(bulkMutateRowsMaxSize, "3"));
  }
  
  public void setContinueAfterWriteError(final Boolean continueAfterWriteError) {
    this.continueAfterWriteError = continueAfterWriteError;
  }

  private Boolean continueAfterWriteError() {
    return Boolean.valueOf(Objects.toString(continueAfterWriteError, "false"));
  }
}