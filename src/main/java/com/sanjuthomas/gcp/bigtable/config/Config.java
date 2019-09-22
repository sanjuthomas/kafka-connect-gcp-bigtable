package com.sanjuthomas.gcp.bigtable.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.sanjuthomas.gcp.bigtable.config.WriterConfig.ErrorHandlerConfig;

/**
 *
 * @author Sanju Thomas
 *
 */
public class Config {

  private String keyFile;
  private String project;
  private String instance;
  private String table;
  private String transformer;
  private List<String> keyQualifiers;
  private String keyDelimiter;
  private List<String> families;
  private List<Map<String, List<String>>> familyQualifiers;
  private int maxRetryCount;
  private int retryBackoffSeconds;
  private boolean exponentialBackoff;

  public Map<String, List<String>> familyQualifiersMappings() {
    final Map<String, List<String>> familyQualifiersMappings = new HashMap<>();
    for (final Map<String, List<String>> familyQualifiers : this.getFamilyQualifiers()) {
      familyQualifiersMappings.putAll(familyQualifiers);
    }
    return familyQualifiersMappings;
  }

  public WriterConfig getWriterConfig() {
    final WriterConfig writerConfg = new WriterConfig(keyFile, project, instance, table);
    writerConfg.setErrorHandlerConfig(
        new ErrorHandlerConfig(maxRetryCount, retryBackoffSeconds, exponentialBackoff));
    return writerConfg;
  }

  public String getTransformer() {
    return this.transformer;
  }

  public List<String> getKeyQualifiers() {
    return MoreObjects.firstNonNull(this.keyQualifiers, new ArrayList<String>(0));
  }

  public String getKeyDelimiter() {
    return MoreObjects.firstNonNull(this.keyDelimiter, "");
  }

  public List<String> getFamilies() {
    return this.families;
  }

  private List<Map<String, List<String>>> getFamilyQualifiers() {
    return MoreObjects.firstNonNull(this.familyQualifiers,
        new ArrayList<Map<String, List<String>>>(0));
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
    this.transformer =
        MoreObjects.firstNonNull(transformer, "com.sanjuthomas.gcp.transform.JsonEventTransformer");
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

  public void setErrorHandler(Map<String, Object> errorHandler) {
    this.maxRetryCount = Integer.valueOf(Objects.toString(errorHandler.get("maxRetryCount"), "3"));
    this.retryBackoffSeconds = Integer.valueOf(Objects.toString(errorHandler.get("retryBackoffSeconds"), "3"));
    this.exponentialBackoff = Boolean.valueOf(Objects.toString(errorHandler.get("exponentialBackoff"), "true"));
  }

}
