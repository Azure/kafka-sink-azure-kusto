package com.microsoft.azure.kusto.kafka.connect.sink.client;

public enum IngestionErrorCode {
    /// <summary>
    /// Unknown error occurred
    /// </summary>
    Unknown,
    
    /// <summary>
    /// Low memory condition.
    /// </summary>
    Stream_LowMemoryCondition,
        
    /// <summary>
    /// Wrong number of fields.
    /// </summary>
    Stream_WrongNumberOfFields,
    
    /// <summary>
    /// Input stream/record/field too large.
    /// </summary>
    Stream_InputStreamTooLarge,
    
    /// <summary>
    /// No data streams to ingest
    /// </summary>
    Stream_NoDataToIngest,
    
    /// <summary>
    /// Invalid csv format - closing quote missing.
    /// </summary>
    Stream_ClosingQuoteMissing,
    
    /// <summary>
    /// Failed to download source from Azure storage - source not found
    /// </summary>
    Download_SourceNotFound,
    
    /// <summary>
    /// Failed to download source from Azure storage - access condition not satisfied
    /// </summary>
    Download_AccessConditionNotSatisfied,
    
    /// <summary>
    /// Failed to download source from Azure storage - access forbidden
    /// </summary>
    Download_Forbidden,
    
    /// <summary>
    /// Failed to download source from Azure storage - account not found
    /// </summary>
    Download_AccountNotFound,
    
    /// <summary>
    /// Failed to download source from Azure storage - bad request
    /// </summary>
    Download_BadRequest,
    
    /// <summary>
    /// Failed to download source from Azure storage - not transient error
    /// </summary>
    Download_NotTransient,
    
    /// <summary>
    /// Failed to download source from Azure storage - unknown error
    /// </summary>
    Download_UnknownError,
    
    /// <summary>
    /// Failed to invoke update policy. Query schema does not match table schema
    /// </summary>
    UpdatePolicy_QuerySchemaDoesNotMatchTableSchema,
    
    /// <summary>
    /// Failed to invoke update policy. Failed descendant transactional update policy
    /// </summary>
    UpdatePolicy_FailedDescendantTransaction,
    
    /// <summary>
    /// Failed to invoke update policy. Ingestion Error occurred
    /// </summary>
    UpdatePolicy_IngestionError,
    
    /// <summary>
    /// Failed to invoke update policy. Unknown error occurred
    /// </summary>
    UpdatePolicy_UnknownError,
    
    /// <summary>
    /// Json pattern was not ingested with jsonMapping parameter
    /// </summary>
    BadRequest_MissingJsonMappingtFailure,
    
    /// <summary>
    /// Blob is invalid or empty zip archive
    /// </summary>
    BadRequest_InvalidOrEmptyBlob,
    
    /// <summary>
    /// Database does not exist
    /// </summary>
    BadRequest_DatabaseNotExist,
    
    /// <summary>
    /// Table does not exist
    /// </summary>
    BadRequest_TableNotExist,
    
    /// <summary>
    /// Invalid kusto identity token
    /// </summary>
    BadRequest_InvalidKustoIdentityToken,
    
    /// <summary>
    /// Blob path without SAS from unknown blob storage
    /// </summary>
    BadRequest_UriMissingSas,
    
    /// <summary>
    /// File too large
    /// </summary>
    BadRequest_FileTooLarge,
    
    /// <summary>
    /// No valid reply from ingest command
    /// </summary>
    BadRequest_NoValidResponseFromEngine,
    
    /// <summary>
    /// Access to table is denied
    /// </summary>
    BadRequest_TableAccessDenied,
    
    /// <summary>
    /// Message is exhausted
    /// </summary>
    BadRequest_MessageExhausted,
    
    /// <summary>
    /// Bad request
    /// </summary>
    General_BadRequest,
    
    /// <summary>
    /// Internal server error occurred
    /// </summary>
    General_InternalServerError,
    
    /// <summary>
    /// Failed to invoke update policy. Cyclic update is not allowed
    /// </summary>
    UpdatePolicy_Cyclic_Update_Not_Allowed,
    
    /// <summary>
    /// Failed to invoke update policy. Transactional update policy is not allowed in streaming ingestion
    /// </summary>
    UpdatePolicy_Transactional_Not_Allowed_In_Streaming_Ingestion,
    
    /// <summary>
    /// Failed to parse csv mapping.
    /// </summary>
    BadRequest_InvalidCsvMapping,
    
    /// <summary>
    /// Invalid mapping reference.
    /// </summary>
    BadRequest_InvalidMappingReference,
    
    /// <summary>
    /// Mapping reference wasn't found.
    /// </summary>
    BadRequest_MappingReferenceWasNotFound,
    
    /// <summary>
    /// Failed to parse json mapping.
    /// </summary>
    BadRequest_InvalidJsonMapping,
    
    /// <summary>
    /// Format is not supported
    /// </summary>
    BadRequest_FormatNotSupported,
    
    /// <summary>
    /// Ingestion properties contains ingestion mapping and ingestion mapping reference.
    /// </summary>
    BadRequest_DuplicateMapping,
    
    /// <summary>
    /// Message is corrupted
    /// </summary>
    BadRequest_CorruptedMessage,
    
    /// <summary>
    /// Inconsistent ingestion mapping
    /// </summary>
    BadRequest_InconsistentMapping,
    
    /// <summary>
    /// Syntax error
    /// </summary>
    BadRequest_SyntaxError,
    
    /// <summary>
    /// Abandoned ingestion.
    /// </summary>
    General_AbandonedIngestion,
    
    /// <summary>
    /// Throttled ingestion.
    /// </summary>
    General_ThrottledIngestion,
    
    /// <summary>
    /// Schema of target table at start time doesn't match the one at commit time.
    /// </summary>
    General_TransientSchemaMismatch,
}
