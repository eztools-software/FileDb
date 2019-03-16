/* Copyright (C) EzTools Software - All Rights Reserved
 * Released under Mozilla Public License 2.0
 * Written and maintained by Brett Goodman <eztools-software.com>
 */
using System;

namespace FileDbNs
{
    public enum FileDbExceptionsEnum
    {
        NoOpenDatabase,
        IndexOutOfRange,
        InvalidDatabaseSignature,
        CantOpenNewerDbVersion,
        DatabaseFileNotFound,
        InvalidTypeInSchema,
        InvalidPrimaryKeyType,
        MissingPrimaryKey,
        DuplicatePrimaryKey,
        DatabaseAlreadyHasPrimaryKey,
        PrimaryKeyCannotBeAdded,
        PrimaryKeyValueNotFound,
        InvalidFieldName,
        NeedIntegerKey,
        FieldNameAlreadyExists,
        NonArrayValue,
        InvalidDataType,
        MismatchedKeyFieldTypes,
        InvalidKeyFieldType,
        DatabaseEmpty,
        InvalidFilterConstruct,
        FieldSpecifiedTwice,
        IteratorPastEndOfFile,
        HashSetExpected,
        CantAddOrRemoveFieldWithDeletedRecords,
        DatabaseReadOnlyMode,
        InvalidMetaDataType,
        CantConvertTypeToGuid,
        GuidTypeMustBeGuidOrByteArray,
        ErrorConvertingValueForField,
        CannotDeletePrimaryKeyField,
        FieldListIsEmpty,
        FieldNameIsEmpty,
        InvalidOrderByFieldName,
        CannotOrderByOnArrayField,
        AsyncOperationTimeout,
        MissingTransactionFile,
        EmptyFilename,
        StreamMustBeWritable,
        NoCurrentTransaction,
        NoEncryptor,
        DbSchemaIsUpToDate,
        DbIsEncrypted
    }

    // This exception is raised whenever a statement cannot be compiled.
    public class FileDbException : Exception
    {
        #region Strings
        internal const string IndexOutOfRange = "Index out of range";

        internal const string RecordNumOutOfRange = "Record index out of range - {0}.";

        internal const string CantOpenNewerDbVersion = "Cannot open newer database version {0}.{1}.  Current version is {2}";

        internal const string StrInvalidDataType = "Invalid data type encountered in data file ({0})";

        internal const string StrInvalidDataType2 = "Invalid data type for field '{0}' - expected '{1}' but got '{2}'";

        internal const string InvalidFieldName = "Field name not in table: {0}";

        internal const string InvalidKeyFieldType = "Invalid key field type (record number) - must be type Int32";

        internal const string InvalidDateTimeType = "Invalid DateTime type";

        internal const string DatabaseEmpty = "There are no records in the database";

        internal const string NoOpenDatabase = "No open database";

        internal const string DatabaseFileNotFound = "The database file doesn't exist";

        internal const string NeedIntegerKey = "If there is no primary key on the database, the key must be the integer record number";

        internal const string NonArrayValue = "Non array value passed for array field '{0}'";

        internal const string InValidBoolType = "Invalid Bool type";

        internal const string MismatchedKeyFieldTypes = "Mismatched key field types";

        internal const string InvalidDatabaseSignature = "Invalid signature in database";

        internal const string InvalidTypeInSchema = "Invalid type in schema: {0}";

        internal const string InvalidPrimaryKeyType = "Primary key field '{0}' must be type Int or String and must not be Array type";

        internal const string MissingPrimaryKey = "Primary key field {0} cannot be null or missing";

        internal const string DuplicatePrimaryKey = "Duplicate key violation - Field: '{0}' - Value: '{1}'";

        internal const string PrimaryKeyValueNotFound = "Primary key field value not found";

        internal const string InvalidFilterConstruct = "Invalid Filter construct near '{0}'";

        internal const string FieldSpecifiedTwice = "Field name cannot be specified twice in list - {0}";

        internal const string IteratorPastEndOfFile = "The current position is past the last record - call MoveFirst to reset the current position";

        internal const string HashSetExpected = "HashSet<object> expected as the SearchVal when using Equality.In";

        internal const string CantAddOrRemoveFieldWithDeletedRecords = "Cannot add or remove fields with deleted records in the database - call Clean first";

        internal const string CannotDeletePrimaryKeyField = "You cannot delete the primary key field ({0})";

        internal const string FieldListIsEmpty = "The field list is null or empty";

        internal const string DatabaseAlreadyHasPrimaryKey = "This database already has a primary key field ({0})";

        internal const string PrimaryKeyCannotBeAdded = "Primary key fields can only be added if there are no records in the database: {0}";

        internal const string FieldNameAlreadyExists = "Cannot add field because the field name already exists: {0}";

        internal const string DatabaseReadOnlyMode = "Database is open in read-only mode";

        internal const string InvalidMetaDataType = "Invalid meta data type - must be String or Byte[]";

        internal const string CantConvertTypeToGuid = "Cannot convert type {0} to Guid";

        internal const string GuidTypeMustBeGuidOrByteArray = "Guid type must be Guid or Byte array";

        internal const string ErrorConvertingValueForField = "Error converting value for field name: {0}  value: {1}";

        internal const string FieldNameIsEmpty = "The field name is null or empty";

        internal const string InvalidOrderByFieldName = "Invalid OrderBy field name - {0}";

        internal const string CannotOrderByOnArrayField = "Cannot OrderBy on an array field";

        internal const string AsyncOperationTimeout = "Async operation timeout";

        internal const string MissingTransactionFile = "Missing transaction file";

        internal const string EmptyFilename = "The database filename cannot be null or empty";

        internal const string StreamMustBeWritable = "The Stream must be writable to create a database";

        internal const string NoCurrentTransaction = "There is no current transaction";

        internal const string NoEncryptor = "An Encryptor was not provided when the DB was opened";

        internal const string DbSchemaIsUpToDate = "The database schema is already up to date";

        internal const string DbIsEncrypted = "This database is encrypted but no Encryptor was provided";

        #endregion Strings

        /////////////////////////////////////////
        FileDbExceptionsEnum _id;

        public FileDbExceptionsEnum ID
        {
            get { return _id; }
        }

        public FileDbException(string message, FileDbExceptionsEnum id)
            : base(message)
        {
            _id = id;
        }

        public FileDbException(string message, FileDbExceptionsEnum id, Exception cause)
            : base(message, cause)
        {
            _id = id;
        }
    }
}

