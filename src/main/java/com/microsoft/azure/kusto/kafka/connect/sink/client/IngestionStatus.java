package com.microsoft.azure.kusto.kafka.connect.sink.client;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import com.microsoft.azure.storage.table.TableServiceEntity;

/// <summary>
/// This class represents an ingestion status.
/// </summary>
/// <remarks>
/// Any change to this class must be made in a backwards/forwards-compatible manner.
/// </remarks>
public class IngestionStatus extends TableServiceEntity {
    /// <summary>
    /// The updated status of the ingestion. The ingestion status will be 'Pending'
    /// during the ingestion's process
    /// and will be updated as soon as the ingestion completes.
    /// </summary>
    public OperationStatus status;

    public String getStatus() {
        return status.toString();
    }
    
    public void setStatus(String s) {
        status = OperationStatus.valueOf(s);
    }

    /// <summary>
    /// A unique identifier representing the ingested source. Can be supplied during
    /// the ingestion execution.
    /// </summary>
    public UUID ingestionSourceId;

    public UUID getIngestionSourceId() {
        return ingestionSourceId;
    }

    public void setIngestionSourceId(UUID id) {
        ingestionSourceId = id;
    }

    /// <summary>
    /// The URI of the blob, potentially including the secret needed to access
    /// the blob. This can be a file system URI (on-premises deployments only),
    /// or an Azure Blob Storage URI (including a SAS key or a semicolon followed
    /// by the account key)
    /// </summary>
    public String ingestionSourcePath;

    public String getIngestionSourcePath() {
        return ingestionSourcePath;
    }

    public void setIngestionSourcePath(String path) {
        ingestionSourcePath = path;
    }

    /// <summary>
    /// The name of the database holding the target table.
    /// </summary>
    public String database;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String db) {
        database = db;
    }

    /// <summary>
    /// The name of the target table into which the data will be ingested.
    /// </summary>
    public String table;

    public String getTable() {
        return table;
    }

    public void setTable(String t) {
        table = t;
    }

    /// <summary>
    /// The last updated time of the ingestion status.
    /// </summary>
    public Date updatedOn;

    public Date getUpdatedOn() {
        return updatedOn;
    }

    public void setUpdatedOn(Date lastUpdated) {
        updatedOn = lastUpdated;
    }

    /// <summary>
    /// The ingestion's operation Id.
    /// </summary>
    public UUID operationId;

    public UUID getOperationId() {
        return operationId;
    }

    public void setOperationId(UUID id) {
        operationId = id;
    }

    /// <summary>
    /// The ingestion's activity Id.
    /// </summary>
    public UUID activityId;

    public UUID getActivityId() {
        return activityId;
    }

    public void setActivityId(UUID id) {
        activityId = id;
    }

    /// <summary>
    /// In case of a failure - indicates the failure's error code.
    /// </summary>
    public IngestionErrorCode errorCode;

    public String getErrorCode() {
        return (errorCode != null ? errorCode : IngestionErrorCode.Unknown).toString();
    }

    public void setErrorCode(String code) {
        errorCode = code == null ? IngestionErrorCode.Unknown : IngestionErrorCode.valueOf(code);
    }

    /// <summary>
    /// In case of a failure - indicates the failure's status.
    /// </summary>
    public IngestionFailureInfo.FailureStatusValue failureStatus;

    public String getFailureStatus() {
        return (failureStatus != null ? failureStatus : IngestionFailureInfo.FailureStatusValue.Unknown).toString();
    }

    public void setFailureStatus(String status) {
        failureStatus = IngestionFailureInfo.FailureStatusValue.valueOf(status);
    }

    /// <summary>
    /// In case of a failure - indicates the failure's details.
    /// </summary>
    public String details;

    public String getDetails() {
        return details;
    }

    public void setDetails(String d) {
        details = d;
    }

    /// <summary>
    /// In case of a failure - indicates whether or not the failures originate from
    /// an Update Policy.
    /// </summary>
    public boolean originatesFromUpdatePolicy;

    public boolean getOriginatesFromUpdatePolicy() {
        return originatesFromUpdatePolicy;
    }

    public void setOriginatesFromUpdatePolicy(boolean fromUpdatePolicy) {
        originatesFromUpdatePolicy = fromUpdatePolicy;
    }

    public IngestionStatus() {
    }

    public IngestionStatus(UUID uuid) {
        super(uuid.toString(), uuid.toString());
    }
}
