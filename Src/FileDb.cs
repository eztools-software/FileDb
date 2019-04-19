/* Copyright (C) EzTools Software - All Rights Reserved
 * Released under Mozilla Public License 2.0
 * Written and maintained by Brett Goodman <eztools-software.com>
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Diagnostics;
using System.Text.RegularExpressions;


// TODO: Support for array field searches by adding BoolOp.In

namespace FileDbNs
{
    //=====================================================================
    internal class FileDbEngine
    {
        internal event FileDb.RecordUpdatedHandler RecordUpdated;
        internal event FileDb.RecordAddedHandler RecordAdded;
        internal event FileDb.RecordDeletedHandler RecordDeleted;

        enum FileModeEnum
        {
            CreateNew = 1,
            Create = 2,
            Open = 3,
            OpenOrCreate = 4,
            //Truncate = 5,
            //Append = 6,
        }

        // Introduced in ver 6
        [Flags]
        enum FlagsEnum : UInt32
        {
            NoFlags = 0,
            IsEncrypted = 1 // ver 6
        }

        ///////////////////////////////////////////////////////////////////////
        #region Consts

        const string StrIndex = "index";

        //const int NoLock = 0,
        //          ReadLock = 1,
        //          WriteLock = 2;

        // at ver 2.2 we started supporting NULL fields
        const int VerNullValueSupport = 202;

        const int DateTimeByteLen = 10;
        const int GuidByteLen = 16;

        // Automatically incremented Int32 type
        internal const Int32 AutoIncField = 0x1;

        // Array type
        internal const Int32 ArrayField = 0x2;

        // Major version of the FileDb assembly
        // Note: A new version that changes the DB schema will take the next major version
        const byte VERSION_MAJOR = 6;
        // Minor version of the FileDb assembly
        const byte VERSION_MINOR = 0;

        // Signature to help validate file
        const Int32 SIGNATURE = 0x0123BABE;

        const Int32 FLAGS_OFFSET = 6,
                    RESERVED_OFFSET = 10;

        #region variable offsets depending on version

        // Location of the 'records count' offset in the index
        Int32 _header_end_offset;
        Int32 _num_recs_offset;

        // Location of the 'deleted count' offset in the index
        // Always the next 'Int32 size' offset after the 'records count' offset
        // Internal use only
        Int32 _index_deleted_offset;

        // Location of the Index offset, which is always written at the end of the data file
        Int32 _index_offset;

        #endregion

        // Size of the field specifing the size of a record in the index
        const Int32 INDEX_RBLOCK_SIZE = 4;

        const int AsyncWaitTimeout = 10000;

        #endregion Consts

        ///////////////////////////////////////////////////////////////////////
        #region Fields

        bool _isOpen,
             _disposed,
             _isReadOnly,
             _autoFlush,
             _isAutoIncSuspended;

        string _dbFileName,
               _primaryKey;

        //FolderLocEnum _folderLoc;

        Stream _dbStream,
               _transDbStream;

#if !(NETFX_CORE || PCL)
        String _transFilename;
#endif

        BinaryReader _dataReader;

        BinaryWriter _dataWriter;

        MemoryStream _testStrm;
        BinaryWriter _testWriter;

        IEncryptor _encryptor;

        Int32 _numRecords,
              _numDeleted,
              _autoCleanThreshold,
              _dataStartPos,
              _indexStartPos,
              _iteratorIndex;

        byte _ver_major,
             _ver_minor;

        Int32 _ver;

        FlagsEnum _flags;

        Fields _fields;

        float _userVersion;

        Field _primaryKeyField;

        List<Int32> _index,
                    _deletedRecords;

        object _userData;

        #endregion Fields

        ///////////////////////////////////////////////////////////////////////
        #region IDisposable

        // Do not make this method virtual.
        // A derived class should not be able to override this method.
        //
        public void Dispose()
        {
            Dispose(true);

            // This object will be cleaned up by the Dispose method.
            // Therefore you should call GC.SupressFinalize to take this object off the finalization queue
            // and prevent finalization code for this object from executing a second time.

            GC.SuppressFinalize(this);
        }

        // Dispose(bool disposing) executes in two distinct scenarios.
        // If disposing equals true, the method has been called directly
        // or indirectly by a user's code. Managed and unmanaged resources
        // can be disposed.
        // If disposing equals false, the method has been called by the
        // runtime from inside the finalizer and you should not reference
        // other objects. Only unmanaged resources can be disposed.
        //
        protected virtual void Dispose(bool disposing)
        {
            // Check to see if Dispose has already been called.

            if (!this._disposed)
            {
                // If disposing equals true, dispose all managed and unmanaged resources

                if (disposing)
                {
                    // Dispose managed resources.
                    Close();
                }

                // Call the appropriate methods to clean up unmanaged resources here.
                // If disposing is false, only the following code is executed.
                // if this is used, implement C# destructor and call Dispose(false) in it

                // e.g. CloseHandle( handle );

                // Note disposing has been done.
                _disposed = true;
            }
        }

        ~FileDbEngine()
        {
            Dispose(false);
        }

        #endregion IDisposable

        ///////////////////////////////////////////////////////////////////////
        #region internal

        #region Properties

        public string DbFileName
        {
            get
            {
                string retVal = _dbFileName;
                if (retVal == null && _dbStream != null)
                {
                    retVal = "memory DB";
                }
                return retVal;
            }
        }

        internal float UserVersion
        {
            get { return _userVersion; }
            set { _userVersion = value; }
        }

        internal bool IsEncrypted { get { return (_flags & FlagsEnum.IsEncrypted) == FlagsEnum.IsEncrypted; } }

        internal Fields Fields
        {
            get { checkIsDbOpen(); return _fields; }
        }

        internal Int32 NumDeleted
        {
            get { checkIsDbOpen(); return _numDeleted; }
            set { _numDeleted = value; }
        }

        internal Int32 NumRecords
        {
            get { checkIsDbOpen(); return _numRecords; }
            set { _numRecords = value; }
        }

        internal bool IsOpen
        {
            get { return _isOpen; }
            set { _isOpen = value; }
        }

        internal bool AutoFlush
        {
            get { return _autoFlush; }
            set
            {
                // if transitioning from Off to On we must flush now because
                // if anything has been done requiring flushing it won't happen otherwise
                if (_isOpen && !_autoFlush && value == true)
                {
                    Flush(true);
                }
                _autoFlush = value;
            }
        }

        internal object UserData
        {
            get { return _userData; }
            set
            {
                _userData = value;
                // if AutoFlush is true we must write the index because it won't happen if
                // no other actions are taken before closing
                if (AutoFlush)
                    Flush(true);
            }
        }

        #endregion Properties

        /// <summary>
        /// Constructor
        /// </summary>
        internal FileDbEngine()
        {
            _autoCleanThreshold = -1;
            _isOpen = false;
            _isReadOnly = false;
            _autoFlush = false;
        }

        internal void SetEncryptor(IEncryptor encryptor)
        {
            _encryptor = encryptor;
        }

#if PCL || NETFX_CORE
        internal void Open(Stream dbStream, IEncryptor encryptor)
#else
        internal void Open(string dbFileName, Stream dbStream, IEncryptor encryptor, bool readOnly) // FolderLocEnum folderLoc )
#endif
        {
            // Close existing databases first
            if (_isOpen)
                Close();

            try
            {
                // Open the database files

                _dbFileName = null;

#if PCL || NETFX_CORE
                if (dbStream == null)
                {
                    _dbStream = new MemoryStream(4096);
                    _isReadOnly = false;
                }
                else
                {
                    _isReadOnly = !dbStream.CanWrite;
                    _dbStream = dbStream;
                    _dbStream.Seek(0, SeekOrigin.Begin);
                }
#else
                // allow null for memory DB
                //if( string.IsNullOrWhitespace(dbFileName) )
                //    throw new throw new FileDbException( FileDbException.EmptyFilename, FileDbExceptionsEnum.EmptyFilename );
                _isReadOnly = readOnly;
                openDbFileOrStream(dbFileName, dbStream, FileModeEnum.Open); // folderLoc );
                _dbFileName = dbFileName;
                //_folderLoc = folderLoc;
#endif

                getReaderWriter();

                _isOpen = true;
                _iteratorIndex = 0;

                _ver_major = 0;
                _ver_minor = 0;
                _ver = 0;

                //try
                //{
                //lockRead( false );

                // Read and verify the signature

                Int32 sig = _dataReader.ReadInt32();
                if (sig != SIGNATURE)
                {
                    throw new FileDbException(FileDbException.InvalidDatabaseSignature, FileDbExceptionsEnum.InvalidDatabaseSignature);
                }

                // Read the version
                _ver_major = _dataReader.ReadByte();
                _ver_minor = _dataReader.ReadByte();
                _ver = _ver_major * 100 + _ver_minor;

                if (_ver_major >= 6)
                {
                    _header_end_offset = 14;
                    // read new fields
                    _flags = (FlagsEnum) _dataReader.ReadUInt32();
                    int reserved = _dataReader.ReadInt32();

                    if (IsEncrypted && encryptor == null)
                        throw new FileDbException(FileDbException.DbIsEncrypted, FileDbExceptionsEnum.DbIsEncrypted);
                }
                else
                    _header_end_offset = 6;

                _num_recs_offset = _header_end_offset;
                _index_deleted_offset = _header_end_offset + 4;
                _index_offset = _index_deleted_offset + 4;

                // Make sure we only read databases of the same major version or less,
                // because major version change means file format changed

                // we go by major version for schema changes
                // that is, a new version that changes the DB schema will take the next major version
                if (_ver_major > VERSION_MAJOR)
                {
                    throw new FileDbException(string.Format(FileDbException.CantOpenNewerDbVersion,
                                    _ver_major, _ver_minor, VERSION_MAJOR), FileDbExceptionsEnum.CantOpenNewerDbVersion);
                }

                // Read the schema and database statistics
                readSchema(_dataReader);

                _index = readIndex();

                if (encryptor != null)
                {
                    _encryptor = encryptor;

                    if (_ver_major >= 6 && IsEncrypted == false)
                    {
                        if (_numRecords > 0)
                            throw new Exception("The database already has records added without encryption set - cannot open with encryption after adding records unencrypted");

                        _flags |= FlagsEnum.IsEncrypted;
                        // write it to the header
                        writeDbHeader(_dataWriter);
                    }
                    if (_numRecords > 0)
                    {
                        // test the encryptor to see if it works
                        GetRecordByIndex(0, null, false);
                    }
                }

                // NOTE: if we wanted to auto-upgrade a DB file we would call cleanup
                // check the major version -- if older we can update the schema
                // which means we must call cleanup to do the job for us
                // NOTE: on version 5.x and below we used to auto-upgrade older DBs
                // but we won't do that now
                //if( _ver_major < VERSION_MAJOR || _ver < VerNullValueSupport )
                //    cleanup( true );
                //}
                //finally
                //{
                //}
            }
            catch (Exception ex)
            {
                Close();
                throw ex;
            }
        }

#if false // WINDOWS_PHONE_APP
        StorageFolder getStorageFolder()
        {
            return getStorageFolder( _folderLoc );
        }

        static StorageFolder getStorageFolder( FolderLocEnum folderLoc )
        {
            StorageFolder storageFolder;

            switch( folderLoc )
            {
                case FolderLocEnum.RoamingFolder:
                storageFolder = ApplicationData.Current.RoamingFolder;
                break; ;

                case FolderLocEnum.TempFolder:
                storageFolder = ApplicationData.Current.TempFolder;
                break; ;

                default:
                case FolderLocEnum.LocalFolder:
                    storageFolder = ApplicationData.Current.LocalFolder;
                    break; ;
            }
            return storageFolder;
        }
#endif

#if !(PCL || NETFX_CORE)
        /// <summary>
        /// Open the database files
        /// </summary>
        /// <param name="dbFileName"></param>
        /// <param name="dbStream"></param>
        /// <param name="mode"></param>
        /// 
        void openDbFileOrStream(string dbFileName, Stream dbStream, FileModeEnum mode) // FolderLocEnum folderLoc )
        {
            if (!string.IsNullOrWhiteSpace(dbFileName))
            {
                // Open the database files
#if false // WINDOWS_PHONE_APP

                StorageFile storageFile = null;

                if( mode == FileModeEnum.Create || mode == FileModeEnum.CreateNew || mode == FileModeEnum.OpenOrCreate )
                {
                    // find out if the file exists by getting it
                    storageFile = openStorageFile( dbFileName, folderLoc );

                    if( storageFile != null )
                    {
                        deleteStorageFile( storageFile );
                        storageFile = null;
                    }
                    storageFile = RunSynchronously( getStorageFolder().CreateFileAsync( dbFileName ) );
                }
                else
                {
                    storageFile = RunSynchronously( getStorageFolder().GetFileAsync( dbFileName ) );
                    if( storageFile == null )
                        throw new FileDbException( FileDbException.DatabaseFileNotFound, FileDbExceptionsEnum.DatabaseFileNotFound );
                }

                if( _openReadOnly )
                    _dbStream = RunSynchronously( storageFile.OpenStreamForReadAsync() );
                else
                    _dbStream = RunSynchronously( storageFile.OpenStreamForWriteAsync() );

#else
                if (!(mode == FileModeEnum.Create || mode == FileModeEnum.CreateNew || mode == FileModeEnum.OpenOrCreate) &&
                        !File.Exists(dbFileName))
                {
                    throw new FileDbException(FileDbException.DatabaseFileNotFound, FileDbExceptionsEnum.DatabaseFileNotFound);
                }
                FileAccess access;
                if (_isReadOnly)
                    access = FileAccess.Read;
                else
                    access = FileAccess.ReadWrite;

                // we must allow read sharing access else operations that require copying to a temp file (copy operation) will fail
                _dbStream = File.Open(dbFileName, (FileMode) mode, access, FileShare.Read);
#endif
            }
            else // memory DB
            {
                if (dbStream != null)
                    _dbStream = dbStream;
                else
                    _dbStream = new MemoryStream(4096);
            }
        }
#endif

        void getReaderWriter()
        {
            _dataReader = new BinaryReader(_dbStream);
            if (!_isReadOnly)
                _dataWriter = new BinaryWriter(_dbStream);
        }

#if false // WINDOWS_PHONE_APP

        StorageFile openStorageFile( string fileName, FolderLocEnum folderLoc )
        {
            StorageFile storageFile = null;

            // find out if the file exists by getting it
            try
            {
                storageFile = RunSynchronously( getStorageFolder( folderLoc ).GetFileAsync( fileName ) );
            }
            catch( Exception ex )
            {
                // throws -2147024894 / x80070002 exception if file doesn't exist
                // for some reason we must coerce them both to UInt32
                if( (UInt32) ex.HResult != (UInt32) 0x80070002 )
                    throw; // rethrow exception if its some other error???
            }

            return storageFile;
        }
#endif

        /// <summary>
        /// Flushes the Stream and detaches it, rendering this FileDb closed
        /// </summary>
        ///
        Stream detachDataStreamAndClose()
        {
            Stream dbStream = _dbStream;

            Close(false);

            return dbStream;
        }

        internal void Close(bool disposeDataStrm = true)
        {
            if (_isOpen)
            {
                try
                {
                    // if AutoFlush is true then we shouldn't need to flush the index
                    Flush(!AutoFlush);
                    //#if !WINDOWS_PHONE_APP
                    // I don't think this is needed in any case
                    //_dbStream.Close();
                    //#endif
                    _dbStream.Dispose();
                }
                finally
                {
                    _autoCleanThreshold = -1;
                    _dataStartPos = 0;
                    _dbStream = null;
                    _dataWriter = null;
                    _dataReader = null;
                    _dataReader = null;
                    _isOpen = false;
                    _dbFileName = null;
                    //_folderLoc = FolderLocEnum.Default;
                    _fields = null;
                    _primaryKey = null;
                    _primaryKeyField = null;
                    _encryptor = null;
                    _userData = null;
                    _flags = FlagsEnum.NoFlags;

#if NETFX_CORE || PCL
                    _transDbStream = null;
#else
                    if (_transFilename != null)
                    {
                        File.Delete(_transFilename);
                        _transFilename = null;
                    }
#endif
                }
            }
        }

#if !(PCL || NETFX_CORE)
        internal static bool Exists(string dbFileName) // FolderLocEnum folderLoc )
        {
            bool retVal = false;

            if (!string.IsNullOrWhiteSpace(dbFileName))
            {
#if false // WINDOWS_PHONE_APP
                StorageFile storageFile = null;

                // find out if the file exists by getting it
                try
                {
                    storageFile = RunSynchronously( getStorageFolder( folderLoc ).GetFileAsync( dbFileName ) );
                }
                catch( Exception ex )
                {
                    // throws -2147024894 / x80070002 exception if file doesn't exist
                    // for some reason we must coerce them both to UInt32
                    if( (UInt32) ex.HResult != (UInt32) 0x80070002 )
                        throw;
                }

                retVal = storageFile != null;

#else
                retVal = File.Exists(dbFileName);
#endif
            }

            return retVal;
        }
#endif

        void checkIsDbOpen()
        {
            if (!_isOpen)
            {
                throw new FileDbException(FileDbException.NoOpenDatabase, FileDbExceptionsEnum.NoOpenDatabase);
            }
        }

        void checkReadOnly()
        {
            if (_isReadOnly)
            {
                throw new FileDbException(FileDbException.DatabaseReadOnlyMode, FileDbExceptionsEnum.NoOpenDatabase);
            }
        }

#if !(PCL || NETFX_CORE)
        internal void Drop(string dbFileName)
        {
            if (dbFileName == _dbFileName && _isOpen)
            {
                Close();
            }

#if false // WINDOWS_PHONE_APP
            var storageFile = RunSynchronously( getStorageFolder().GetFileAsync( dbFileName ) );
            deleteStorageFile( storageFile );
#else
            File.Delete(dbFileName);
#endif
        }
#endif

#if PCL || NETFX_CORE
        internal void Create(Stream dbStream, Field[] schema, IEncryptor encryptor)
#else
        internal void Create(string dbFileName, Field[] schema, IEncryptor encryptor) // FolderLocEnum folderLoc )
#endif
        {
            // Close any existing DB first
            if (_isOpen)
                Close();

            // Find the primary key and do error checking on the schema
            _fields = new Fields();
            _primaryKey = string.Empty;

            for (Int32 i = 0; i < schema.Length; i++)
            {
                Field field = schema[i];

                switch (field.DataType)
                {
                    case DataTypeEnum.Byte:
                    case DataTypeEnum.Int32:
                    case DataTypeEnum.UInt32:
                    case DataTypeEnum.String:
                    case DataTypeEnum.Float:
                    case DataTypeEnum.Double:
                    case DataTypeEnum.Bool:
                    case DataTypeEnum.DateTime:
                    case DataTypeEnum.Int64:
                    case DataTypeEnum.Decimal:
                    case DataTypeEnum.Guid:
                        break;

                    default: // Unknown type..!                        
                        throw new FileDbException(string.Format(FileDbException.InvalidTypeInSchema, (Int32) field.DataType),
                                        FileDbExceptionsEnum.InvalidTypeInSchema);
                }

                if (field.IsPrimaryKey && string.IsNullOrEmpty(_primaryKey))
                {
                    // Primary key!
                    // Is the key an array or boolean?  
                    // If so, don't allow them to be primary keys...

                    if (!(field.DataType == DataTypeEnum.Int32 || field.DataType == DataTypeEnum.String))
                    {
                        throw new FileDbException(string.Format(FileDbException.InvalidPrimaryKeyType, field.Name),
                                        FileDbExceptionsEnum.InvalidPrimaryKeyType);
                    }

                    if (field.IsArray)
                    {
                        throw new FileDbException(string.Format(FileDbException.InvalidPrimaryKeyType, field.Name),
                                        FileDbExceptionsEnum.InvalidPrimaryKeyType);
                    }

                    _primaryKey = field.Name.ToUpper();
                    _primaryKeyField = field;
                }

                _fields.Add(field);
            }

            // Open the database files

            _dbFileName = null;
            _transDbStream = null;
#if !(NETFX_CORE || PCL)
            _transFilename = null;
#endif

#if PCL || NETFX_CORE
            if (dbStream == null)
            {
                _dbStream = new MemoryStream(4096);
                _isReadOnly = false;
            }
            else
            {
                _isReadOnly = !dbStream.CanWrite;
                //if (_isReadOnly) -- not sure if we need this
                //    throw new FileDbException(FileDbException.StreamMustBeWritable, FileDbExceptionsEnum.StreamMustBeWritable);
                _dbStream = dbStream;
                _dbStream.Seek(0, SeekOrigin.Begin);
            }
#else
            openDbFileOrStream(dbFileName, null, FileModeEnum.Create); // folderLoc );
            _dbFileName = dbFileName;
            //_folderLoc = folderLoc;
#endif

            getReaderWriter();

            _isOpen = true;
            _iteratorIndex = 0;

            _ver_major = VERSION_MAJOR;
            _ver_minor = VERSION_MINOR;
            _ver = _ver_major * 100 + _ver_minor;

            _numRecords = 0;
            _numDeleted = 0;

            if (encryptor != null)
            {
                _encryptor = encryptor;
                if (_ver_major >= 6)
                {
                    _flags |= FlagsEnum.IsEncrypted;
                }
            }

            // Write the schema
            writeDbHeader(_dataWriter);
            writeSchema(_dataWriter);

            // the indexStart is the same as dataStart until records are added
            _indexStartPos = _dataStartPos;

            // we must write the indexStart location AFTER writeSchema the first time
            writeIndexStart(_dataWriter);

            // brettg: read it back in because the field order will have changed if the primary key wasn't the first field
            readSchema();

            _index = new List<int>(100);
            _deletedRecords = new List<int>();
        }

        internal int AddRecord(FieldValues record)
        {
            int newIndex = -1;

            checkIsDbOpen();
            checkReadOnly();

            // Verify record as compared to the schema
            verifyRecordSchema(record);

            try
            {
                // Add the item to the data file
                // set the autoinc vals into the record

                foreach (Field field in _fields)
                {
                    if (field.IsAutoInc)
                    {
                        // if there are no records in the DB, start at the beginning
                        if (_numRecords == 0)
                            field.CurAutoIncVal = field.AutoIncStart;

                        // if the field is absent this will add it regardless which is what we want
                        // because its an autoinc field
                        if (!_isAutoIncSuspended)
                            record[field.Name] = field.CurAutoIncVal.Value;
                    }
                }

                // Check the index.  To enable a binary search, we must read in the 
                // entire index, insert our item then write it back out.
                // Where there is no primary key, we can't do a binary search so skip
                // this sorting business.

                if (!string.IsNullOrEmpty(_primaryKey))
                {
                    if (_numRecords > 0) // Bug fix 2/8/12: was > 1
                    {
                        // Do a binary search to find the insertion position
                        object data = null;
                        if (record.ContainsKey(_primaryKey))
                            data = record[_primaryKey];

                        if (data == null)
                            throw new FileDbException(string.Format(FileDbException.MissingPrimaryKey,
                                _primaryKey), FileDbExceptionsEnum.MissingPrimaryKey);

                        Int32 pos = bsearch(_index, 0, _index.Count - 1, data);

                        // Ensure we don't have a duplicate key in the database
                        if (pos > 0)
                            // Oops... duplicate key
                            throw new FileDbException(string.Format(FileDbException.DuplicatePrimaryKey,
                                _primaryKey, data.ToString()), FileDbExceptionsEnum.DuplicatePrimaryKey);

                        // Revert the result from bsearch to the proper insertion position
                        pos = (-pos) - 1;
                        newIndex = pos;
                    }
                }

                byte[] nullmask;
                Int32 newOffset = _indexStartPos,
                      recordSize = getRecordSize(record, out nullmask),
                      deletedIndex = -1;

                if (_numDeleted > 0)
                {
                    // look for an existing deleted record hole big enough to hold the new record

                    for (int ndx = 0; ndx < _deletedRecords.Count; ndx++)
                    {
                        Int32 holePos = _deletedRecords[ndx];
                        _dbStream.Seek(holePos, SeekOrigin.Begin);
                        Int32 holeSize = _dataReader.ReadInt32();
                        Debug.Assert(holeSize < 0);
                        holeSize = -holeSize;
                        if (holeSize >= recordSize)
                        {
                            newOffset = holePos;
                            deletedIndex = ndx;
                            break;
                        }
                    }
                }

                _dbStream.Seek(newOffset, SeekOrigin.Begin);

                writeRecord(_dataWriter, record, recordSize, nullmask, false);

                if (newIndex < 0)
                {
                    _index.Add(newOffset);
                    newIndex = _index.Count - 1;
                }
                else
                    _index.Insert(newIndex, newOffset);

                if (deletedIndex > -1)
                {
                    // this means we have one less deleted record
                    _deletedRecords.RemoveAt(deletedIndex);
                    _numDeleted--;
                    Debug.Assert(_deletedRecords.Count == _numDeleted);
                }

                // update the autoinc vals
                if (!_isAutoIncSuspended)
                {
                    foreach (Field field in _fields)
                    {
                        if (field.IsAutoInc)
                            field.CurAutoIncVal += 1;
                    }
                }

                // check to see if we went past the previous _indexStartPos
                int newDataEndPos = (int) _dbStream.Position;
                if (newDataEndPos > _indexStartPos)
                {
                    // capture the new index pos - the end of the last record is the start of the index
                    _indexStartPos = newDataEndPos;
                    // writeSchema will write _indexStartPos to the file
                }

                // We have a new entry
                ++_numRecords;

                // Write out the newly updated schema (autoinc values, numRecords)
                writeSchema(_dataWriter);
            }
            finally
            {
                if (AutoFlush) Flush(true);
            }

            if (RecordAdded != null)
            {
                try
                {
                    RecordAdded(newIndex);
                }
                catch { }
            }

            return newIndex;
        }

        ///----------------------------------------------------------------------------------------
        /// <summary>
        /// record must have all fields
        /// </summary>
        /// 
        internal void UpdateRecordByKey(FieldValues record, object key)
        {
            checkIsDbOpen();
            checkReadOnly();

            if (_numRecords == 0)
                throw new FileDbException(FileDbException.DatabaseEmpty, FileDbExceptionsEnum.DatabaseEmpty);

            // find the index of the record
            // we only need to get a single field because really just want the index
            string fieldToGet = null;
            if (string.IsNullOrEmpty(_primaryKey))
                fieldToGet = _fields[0].Name;
            else
                fieldToGet = _fields[_primaryKey].Name;
            object[] existingRecord = this.GetRecordByKey(key, new string[] { fieldToGet }, true);
            if (existingRecord == null)
                throw new FileDbException(FileDbException.PrimaryKeyValueNotFound, FileDbExceptionsEnum.PrimaryKeyValueNotFound);

            // the index is in the last column

            UpdateRecordByIndex(record, (int) existingRecord[existingRecord.Length - 1]);
        }

        ///----------------------------------------------------------------------------------------
        /// <summary>
        /// record must have all fields
        /// </summary>
        /// 
        internal void UpdateRecordByIndex(FieldValues record, Int32 index)
        {
            checkIsDbOpen();
            checkReadOnly();

            if (_numRecords == 0)
                throw new FileDbException(FileDbException.DatabaseEmpty, FileDbExceptionsEnum.DatabaseEmpty);

            bool indexUpdated;
            updateRecordByIndex(record, index, _index, true, true, out indexUpdated);

            if (AutoFlush) Flush(indexUpdated);

            // Do an auto-cleanup if required
            checkAutoClean();
        }

        // Helper for the other updateRecord methods - if you call this you MUST call writeIndex if indexUpdated is true
        //
        void updateRecordByIndex(FieldValues record, Int32 index, List<Int32> lstIndex, bool bNormalizeFieldNames, bool bVerifyRecordSchema,
            out bool indexUpdated)
        {
            indexUpdated = false;

            // make field names uppercase
            //if( bNormalizeFieldNames )
            //    record = normalizeFieldNames( record );

            // Verify record as compared to the schema
            if (bVerifyRecordSchema)
                verifyRecordSchema(record);

            Int32 oldSize = 0;

            if (!string.IsNullOrEmpty(_primaryKey) && record.ContainsKey(_primaryKey))
            {
                // Do a binary search to find the index position of any other records that may already
                // have this key so as to not allow duplicate keys

                Int32 pos = bsearch(lstIndex, 0, _numRecords - 1, record[_primaryKey]);

                // Ensure the item to edit IS in the database, 
                // as the new one takes its place.

                if (pos >= 0)
                {
                    pos -= 1;

                    // a record was found - check if its the same one
                    if (pos != index)
                    {
                        // its not the same record and we cannot allow a duplicate key
                        throw new FileDbException(string.Format(FileDbException.DuplicatePrimaryKey,
                                        _primaryKey, record[_primaryKey].ToString()), FileDbExceptionsEnum.DuplicatePrimaryKey);
                    }
                }

                // Revert the result from bsearch to the proper position
                //recordNum = pos;
            }
            else
            {
                // Ensure the record number is a number within range
                if ((index < 0) || (index > _numRecords - 1))
                {
                    throw new FileDbException(string.Format(FileDbException.RecordNumOutOfRange, index), FileDbExceptionsEnum.IndexOutOfRange);
                }
            }

            // Read the size of the record.  If it is the same or bigger than 
            // the new one, then we can just place it in its original position
            // and not worry about a deleted record.

            int origRecordOffset = lstIndex[index];
            _dbStream.Seek(origRecordOffset, SeekOrigin.Begin);
            oldSize = _dataReader.ReadInt32();
            Debug.Assert(oldSize >= 0);

            // fill in any field values from the DB that were not supplied
            FieldValues fullRecord = record;
            bool isFullRecord = record.Count >= _fields.Count; // the index field may be in there

            if (!isFullRecord)
            {
                object[] row = readRecord(origRecordOffset, false);

                fullRecord = new FieldValues(row.Length);

                // copy record to fullRecord
                foreach (string fieldName in record.Keys)
                {
                    fullRecord.Add(fieldName, record[fieldName]);
                }

                foreach (Field field in _fields)
                {
                    if (!fullRecord.ContainsKey(field.Name))
                    {
                        fullRecord.Add(field.Name, row[field.Ordinal]);
                    }
                }
            }

            // Get the size of the new record for calculations below
            byte[] nullmask;
            Int32 newSize = getRecordSize(fullRecord, out nullmask),
                    deletedIndex = -1,
                    newPos = _indexStartPos;

            if (newSize > oldSize)
            {
                // Record is too big for the "hole" - look through the deleted records
                // for a hole large enough to hold the new record

                int ndx = 0;
                foreach (Int32 holePos in _deletedRecords)
                {
                    _dbStream.Seek(holePos, SeekOrigin.Begin);
                    Int32 holeSize = _dataReader.ReadInt32();
                    Debug.Assert(holeSize < 0);
                    holeSize = -holeSize;
                    if (holeSize >= newSize)
                    {
                        newPos = holePos;
                        deletedIndex = ndx;
                        break;
                    }
                    ndx++;
                }
            }
            else
                newPos = origRecordOffset;

            // Write the record to the database file
            _dbStream.Seek(newPos, SeekOrigin.Begin);
            writeRecord(_dataWriter, fullRecord, newSize, nullmask, false);

            // check to see if we went past the previous _indexStartPos
            int newDataEndPos = (int) _dbStream.Position;
            if (newDataEndPos > _indexStartPos)
            {
                // capture the new index pos - the end of the last record is the start of the index
                _indexStartPos = newDataEndPos;
                writeIndexStart(_dataWriter);
            }

            if (newSize > oldSize)
            {
                // add the previous offset to the deleted collection
                _deletedRecords.Add(origRecordOffset);

                // did we find a hole?
                if (deletedIndex < 0)
                {
                    // no hole
                    // this means we have a new deleted entry (the old record) because we couldn't
                    // find a large enough hole and we are writing to the end of the data section
                    ++_numDeleted;
                }
                else
                {
                    // found a hole - remove the old deleted index
                    _deletedRecords.RemoveAt(deletedIndex);
                }

                // update the index with new pos

                lstIndex[index] = newPos;
                indexUpdated = true;

                // make the old record's size be negative to indicate deleted
                _dbStream.Seek(origRecordOffset, SeekOrigin.Begin);
                _dataWriter.Write(-oldSize);

                // Write the number of deleted records                    
                _dbStream.Seek(_index_deleted_offset, SeekOrigin.Begin);
                _dataWriter.Write(_numDeleted);
            }

            if (RecordUpdated != null)
            {
                try
                {
                    RecordUpdated(index, record);
                }
                catch { }
            }
        }

        // Update selected records
        //
        internal Int32 UpdateRecords(FilterExpression searchExp, FieldValues record)
        {
            var searchExpGrp = new FilterExpressionGroup();
            searchExpGrp.Add(BoolOpEnum.And, searchExp);
            return UpdateRecords(searchExpGrp, record);
        }

        // Update selected records
        //
        internal Int32 UpdateRecords(FilterExpressionGroup searchExpGrp, FieldValues record)
        {
            checkIsDbOpen();
            checkReadOnly();

            if (_numRecords == 0)
                return 0;

            // make field names uppercase
            //record = normalizeFieldNames( record );

            // Verify record as compared to the schema
            verifyRecordSchema(record);

            bool isFullRecord = record.Count >= _fields.Count; // the index field may be in there
            Int32 updateCount = 0;
            bool indexUpdated = false;

            try
            {
                // Read and delete selected records
                for (Int32 recordNum = 0; recordNum < _numRecords; ++recordNum)
                {
                    // Read the record
                    object[] row = readRecord(_index[recordNum], false);

                    bool isMatch = Evaluate(searchExpGrp, row, _fields);

                    if (isMatch)
                    {
                        FieldValues fullRecord = record;

                        if (!isFullRecord)
                        {
                            fullRecord = new FieldValues(row.Length);

                            // copy record to fullRecord
                            foreach (string fieldName in record.Keys)
                            {
                                fullRecord.Add(fieldName, record[fieldName]);
                            }

                            // ensure all fields are in the record so that updateRecord will not have to read them in again
                            foreach (Field field in _fields)
                            {
                                if (!fullRecord.ContainsKey(field.Name))
                                {
                                    fullRecord.Add(field.Name, row[field.Ordinal]);
                                }
                            }
                        }

                        bool tempIndexUpdated;
                        updateRecordByIndex(fullRecord, recordNum, _index, false, false, out tempIndexUpdated);
                        if (tempIndexUpdated)
                            indexUpdated = tempIndexUpdated;
                        ++updateCount;
                    }
                }
            }
            finally
            {
                if (AutoFlush) Flush(indexUpdated);
            }

            if (updateCount > 0)
            {
                // Do an auto-cleanup if required
                checkAutoClean();
            }

            return updateCount;
        }

        /// <summary>
        /// Configures autoclean.  When an edit or delete is made, the
        /// record is normally not removed from the data file - only the index.
        /// After repeated edits/deletions, the data file may become very big with
        /// deleted (non-removed) records.  A cleanup is normally done with the
        /// cleanup method.  Autoclean will do this automatically, keeping the
        /// number of deleted records to under the threshold value.
        /// To turn off autoclean, set threshold to a negative value.
        /// </summary>
        /// <param name="threshold">number of deleted records to have at any one time</param>
        ///
        internal void SetAutoCleanThreshold(Int32 threshold)
        {
            _autoCleanThreshold = threshold;

            // Do an auto-cleanup if required
            if ((_isOpen) &&
                (_autoCleanThreshold >= 0) &&
                (_numDeleted > _autoCleanThreshold))
            {
                Cleanup(false);
            }
        }

        internal Int32 GetAutoCleanThreshold()
        {
            return _autoCleanThreshold;
        }

        void checkAutoClean()
        {
            if (_isOpen && _autoCleanThreshold >= 0 && _numDeleted > _autoCleanThreshold)
            {
                Cleanup(false);
            }
        }

        ///----------------------------------------------------------------------------------------
        /// <summary>
        /// Read all records to create new index.
        /// </summary>
        /// 
        internal void Reindex()
        {
            checkIsDbOpen();
            checkReadOnly();

            if (_numRecords == 0)
                return;

            try
            {
                int numRecs = _numRecords + _numDeleted;

                var index = new List<int>(numRecs);
                _deletedRecords = new List<int>();

                _dbStream.Seek(_dataStartPos, SeekOrigin.Begin);
                Int32 newOffset = _dataStartPos;

                for (Int32 recordNum = 0; recordNum < numRecs; ++recordNum)
                {
                    // Read in the size of the block allocated for the record
                    _dbStream.Seek(newOffset, SeekOrigin.Begin);
                    Int32 recordSize;

                    // Read the record
                    bool deleted;
                    object[] record = readRecord(newOffset, false, out recordSize, out deleted);

                    if (!deleted)
                    {
                        if (_primaryKeyField != null) // !string.IsNullOrEmpty( _primaryKey ) )
                        {
                            // Do a binary search to find the insertion position
                            object data = record[_primaryKeyField.Ordinal];

                            Int32 pos = bsearch(index, 0, index.Count - 2, data);

                            // Revert the result from bsearch to the proper insertion position
                            pos = (-pos) - 1;

                            // Insert the new item to the correct position
                            index.Insert(pos, newOffset);
                        }
                        else
                        {
                            index.Add(newOffset);
                        }
                    }
                    else
                        _deletedRecords.Add(newOffset);

                    newOffset += recordSize + sizeof(Int32); // recordSize doesn't include the length of the int size
                }

                _indexStartPos = newOffset;

                _dbStream.Seek(_num_recs_offset, SeekOrigin.Begin);

                _dataWriter.Write(_numRecords = index.Count);

                _dataWriter.Write(_numDeleted = _deletedRecords.Count);

                _dataWriter.Write(_indexStartPos);

                _index = index;
            }
            finally
            {
                Flush(true);
            }
        }

        /// <summary>
        /// Remove all deleted records. schemaChange should only ever be true when the only purpose is to upgrade the DB 
        /// </summary>
        /// 
        internal void Cleanup(bool schemaChange)
        {
            checkIsDbOpen();
            // this causes us to fail opening as readonly and a schemaChange is needed
            // we should not need this check anyway because we shouldn't have any deleted records if readonly
            //checkReadOnly();

            // Don't bother if the database is clean
            if (!schemaChange && _numDeleted == 0)
                return;

            // let the caller know that the schema was already up to date
            if (schemaChange)
            {
                if (_ver_major == VERSION_MAJOR && _ver_minor == VERSION_MINOR)
                    throw new FileDbException(FileDbException.DbSchemaIsUpToDate, FileDbExceptionsEnum.DbSchemaIsUpToDate);
            }

            // Read in the index, and rebuild it along with the database data
            // into a separate file.  Then move that new file back over the old
            // database.

            // Note that we attempt the file creation under the DB lock, so
            // that another process doesn't try to create the same file at the
            // same time.
            //string tmpFilename = Path.GetFileNameWithoutExtension( _dbFileName ) + ".tmp.fdb";
            //tmpFilename = Path.Combine( Path.GetDirectoryName( _dbFileName ), tmpFilename );

            Stream tmpStrm = null;

#if NETFX_CORE || PCL
            // use MemoryStream rather than temp file
            tmpStrm = new MemoryStream((int) _dbStream.Length);
#else
            string tmpFilename = null;
            if (_dbFileName == null)
            {
                tmpStrm = new MemoryStream((int) _dbStream.Length);
            }
            else
            {
                tmpFilename = getTempDbFilename();
                tmpStrm = File.Open(tmpFilename, (FileMode) FileModeEnum.OpenOrCreate, FileAccess.Write, FileShare.None);
                tmpStrm.SetLength(0);
            }
#endif

            int tempNumDeleted = _numDeleted,
                tempIndexStart = _indexStartPos;
            byte curVerMajor = _ver_major, curVerMinor = _ver_minor;

            try
            {
                var tmpDataWriter = new BinaryWriter(tmpStrm);

                // create a new index list
                var newIndex = new List<Int32>(_index.Count);

                // Set the number of (unclean) deleted items to zero and write the schema
                _numDeleted = 0;

                if (schemaChange)
                {
                    _ver_major = VERSION_MAJOR;
                    _ver_minor = VERSION_MINOR;
                    // DO NOT change _ver yet because readRecord below uses it and it must be preserved until after
                    //_ver = _ver_major * 100 + _ver_minor;
                }

                // Write the schema
                writeDbHeader(tmpDataWriter);
                writeSchema(tmpDataWriter);

                // For each item in the index, move it from the current database file to the new one

                for (Int32 idx = 0; idx < _index.Count; ++idx)
                {
                    Int32 offset = _index[idx];
                    bool deleted;

                    // Save the new file offset index
                    newIndex.Add((Int32) tmpStrm.Position);

                    // Read in the entire record
                    if (schemaChange)
                    {
                        int size;
                        object[] record = readRecord(_dataReader, offset, false, out size, out deleted);
                        Debug.Assert(!deleted);

                        FieldValues fullRecord = new FieldValues(_fields.Count);

                        foreach (Field field in _fields)
                            fullRecord.Add(field.Name, record[field.Ordinal]);

                        writeRecord(tmpDataWriter, fullRecord, -1, null, false);
                    }
                    else
                    {
                        byte[] record = readRecordRaw(_dataReader, offset, out deleted);
                        Debug.Assert(!deleted);
                        writeRecordRaw(tmpDataWriter, record, false);
                    }
                }
                // now set _ver
                _ver = _ver_major * 100 + _ver_minor;

                _indexStartPos = (int) tmpStrm.Position;
                writeIndexStart(tmpDataWriter);
                _deletedRecords = new List<Int32>();
                _index = newIndex;
                tmpStrm.Flush();
                writeIndex(tmpStrm, tmpDataWriter, _index);
                tmpDataWriter.Flush();
                tmpStrm.Flush();
            }
            catch
            {
                _ver_major = curVerMajor;
                _ver_minor = curVerMinor;

                // set everything back the way it was
                _indexStartPos = tempIndexStart;
                _numDeleted = tempNumDeleted;

                if (tmpStrm != null)
                    tmpStrm.Dispose();

#if !(NETFX_CORE || PCL)
                if (tmpFilename != null)
                    File.Delete(tmpFilename);
#endif
                throw;
            }

            // get the dbFileName, etc. before we close
            string dbFileName = _dbFileName;
            //Stream dbStream = _dbStream; not needed
            //FolderLocEnum folderLoc = _folderLoc;
            IEncryptor encryptor = _encryptor;
            bool isReadOnly = _isReadOnly;
            Close();

#if NETFX_CORE || PCL
            // reopen with the new Stream
            Open(tmpStrm, encryptor);
#else
            // Move the temporary file over the original database file and reopen
            if (tmpFilename != null)
            {
                File.Delete(dbFileName);
                tmpStrm.Flush(); //.Close();
                tmpStrm.Dispose();
                File.Move(tmpFilename, dbFileName);
                // Re-open the database
                Open(dbFileName, null, encryptor, isReadOnly); // folderLoc );
            }
            else
            {
                Open(null, tmpStrm, encryptor, isReadOnly); // folderLoc );
            }
#endif
        }

        void setRecordDeleted(int pos, bool deleted)
        {
            _dbStream.Seek(pos, SeekOrigin.Begin);
            Int32 size = _dataReader.ReadInt32();
            if (size > 0)
                size = -size;
            _dbStream.Seek(pos, SeekOrigin.Begin);
            _dataWriter.Write(size);
        }

        internal Int32 RemoveAll()
        {
            checkIsDbOpen();
            checkReadOnly();

            Int32 numDeleted = 0;

            if (_numRecords == 0)
                return numDeleted;

            //try
            //{
            _dbStream.Seek(_header_end_offset, SeekOrigin.Begin);

            _numRecords = _numDeleted = 0;
            _indexStartPos = _dataStartPos;

            _index.Clear();
            _deletedRecords.Clear();

            writeSchema(_dataWriter);

            Flush(true);
            //}
            //finally
            //{
            //}

            return numDeleted;
        }

        /// <summary>
        /// Removes an entry from the database INDEX only - it appears
        /// deleted, but the actual data is only removed from the file when a 
        /// cleanup() is called.
        /// </summary>
        /// <param name="key">Int32 or string primary key used to identify record to remove.  For
        /// databases without primary keys, it is the record number (zero based) in
        /// the table.</param>
        /// <returns>true if a record was removed, false otherwise</returns>
        /// 
        internal bool RemoveByKey(object key)
        {
            checkIsDbOpen();
            checkReadOnly();

            if (_numRecords == 0)
                return false;

            Int32 index = -1;

            try
            {
                if (!string.IsNullOrEmpty(_primaryKey))
                {
                    // Do a binary search to find the item
                    index = bsearch(_index, 0, _numRecords - 1, key);

                    if (index < 0)
                    {
                        // Not found!
                        return false;
                    }

                    // Revert the result from bsearch to the proper insertion position
                    --index;
                }
                else
                {
                    if (key.GetType() != typeof(Int32))
                    {
                        throw new FileDbException(FileDbException.InvalidKeyFieldType, FileDbExceptionsEnum.InvalidKeyFieldType);
                    }

                    index = (Int32) key;

                    // Ensure the "key" is the item number within range
                    if (index < 0 || index >= _numRecords)
                    {
                        throw new FileDbException(string.Format(FileDbException.RecordNumOutOfRange, index), FileDbExceptionsEnum.IndexOutOfRange);
                    }
                }

                setRecordDeleted(_index[index], true);
                _deletedRecords.Add(_index[index]);
                _index.RemoveAt(index);

                _dbStream.Seek(_header_end_offset, SeekOrigin.Begin);

                // Write the number of records
                _dataWriter.Write(--_numRecords);

                // Write the number of (unclean) deleted records
                _dataWriter.Write(++_numDeleted);
                Debug.Assert(_deletedRecords.Count == _numDeleted);
            }
            finally
            {
                if (AutoFlush) Flush(true);
            }

            // Do an auto-cleanup if required
            checkAutoClean();

            if (RecordDeleted != null)
            {
                try
                {
                    RecordDeleted(index);
                }
                catch { }
            }

            return true;
        }

        /// <summary>
        /// Removes an entry from the database INDEX only - it appears
        /// deleted, but the actual data is only removed from the file when a 
        /// cleanup() is called.
        /// </summary>
        /// <param name="index">The record number (zero based) in the table to remove</param>
        /// <returns>true on success, false otherwise</returns>
        /// 
        internal bool RemoveByIndex(Int32 index)
        {
            checkIsDbOpen();
            checkReadOnly();

            if (_numRecords == 0)
            {
                return false;
            }

            // All we do here is remove the item from the index.
            // Read in the index, check to see if it exists, delete the item,
            // then rebuild the index on disk.

            try
            {
                // Ensure it is within range
                if ((index < 0) || (index >= _numRecords))
                {
                    throw new FileDbException(string.Format(FileDbException.RecordNumOutOfRange, index), FileDbExceptionsEnum.IndexOutOfRange);
                }

                setRecordDeleted(_index[index], true);
                _deletedRecords.Add(_index[index]);

                _index.RemoveAt(index);

                _dbStream.Seek(_header_end_offset, SeekOrigin.Begin);

                // Write the number of records
                _dataWriter.Write(--_numRecords);

                // Write the number of (unclean) deleted records
                _dataWriter.Write(++_numDeleted);
                Debug.Assert(_deletedRecords.Count == _numDeleted);
            }
            finally
            {
                if (AutoFlush) Flush(true);
            }

            // Do an auto-cleanup if required
            checkAutoClean();

            if (RecordDeleted != null)
            {
                try
                {
                    RecordDeleted(index);
                }
                catch { }
            }

            return true;
        }

        /// <summary>
        /// Removes entries from the database INDEX only, based on the
        /// result of a regular expression match on a given field - records appear 
        /// deleted, but the actual data is only removed from the file when a 
        /// cleanup() is called.
        /// </summary>
        /// <param name="searchExp"></param>
        /// <returns>number of records removed</returns>
        internal Int32 RemoveByValue(FilterExpression searchExp)
        {
            checkIsDbOpen();
            checkReadOnly();

            if (_numRecords == 0)
                return 0;

            string fieldName = searchExp.FieldName;
            if (fieldName[0] == '~')
            {
                fieldName = fieldName.Substring(1);
                searchExp.MatchType = MatchTypeEnum.IgnoreCase;
            }

            // Check the field name is valid
            if (!_fields.ContainsKey(fieldName))
                throw new FileDbException(string.Format(FileDbException.InvalidFieldName, searchExp.FieldName), FileDbExceptionsEnum.InvalidFieldName);

            Field field = _fields[fieldName];
            Int32 deleteCount = 0;

            try
            {
                Regex regex = null;

                // Read and delete selected records
                for (Int32 index = 0; index < _numRecords; ++index)
                {
                    // Read the record
                    bool deleted;
                    object[] record = readRecord(_index[index], false, out deleted);
                    Debug.Assert(!deleted);

                    //object val = record[field.Ordinal].ToString();

                    if (searchExp.Equality == ComparisonOperatorEnum.Regex && regex == null)
                        regex = new Regex(searchExp.SearchVal.ToString(), RegexOptions.IgnoreCase);

                    bool isMatch = evaluate(field, searchExp, record, regex);

                    if (isMatch)
                    {
                        setRecordDeleted(_index[index], true);
                        _deletedRecords.Add(_index[index]);

                        _index.RemoveAt(index);

                        --_numRecords;
                        ++_numDeleted;

                        // Make sure we don't skip over the next item in the for() loop
                        --index;
                        ++deleteCount;

                        if (RecordDeleted != null)
                        {
                            try
                            {
                                RecordDeleted(index);
                            }
                            catch { }
                        }
                    }
                }

                if (deleteCount > 0)
                {
                    _dbStream.Seek(_header_end_offset, SeekOrigin.Begin);

                    // Write the number of records
                    _dataWriter.Write(_numRecords);

                    // Write the number of (unclean) deleted records
                    _dataWriter.Write(_numDeleted);

                    Debug.Assert(_deletedRecords.Count == _numDeleted);
                }
            }
            finally
            {
                if (AutoFlush) Flush(true);
            }

            // Do an auto-cleanup if required
            checkAutoClean();

            return deleteCount;
        }

        // Read in each record once at a time, and remove it from
        // the index if the select function determines it to be deleted
        // Rebuild the index on disc if there items were deleted
        //
        internal Int32 RemoveByValues(FilterExpressionGroup searchExpGrp)
        {
            checkIsDbOpen();
            checkReadOnly();

            if (_numRecords == 0)
                return 0;

            Int32 deleteCount = 0;

            try
            {
                // Read and delete selected records
                for (Int32 recordNum = 0; recordNum < _numRecords; ++recordNum)
                {
                    // Read the record
                    bool deleted;
                    object[] record = readRecord(_index[recordNum], false, out deleted);
                    Debug.Assert(!deleted);

                    bool isMatch = Evaluate(searchExpGrp, record, _fields);

                    if (isMatch)
                    {
                        setRecordDeleted(_index[recordNum], true);
                        _deletedRecords.Add(_index[recordNum]);

                        _index.RemoveAt(recordNum);

                        --_numRecords;
                        ++_numDeleted;

                        // Make sure we don't skip over the next item in the for() loop
                        --recordNum;
                        ++deleteCount;

                        try
                        {
                            RecordDeleted(recordNum);
                        }
                        catch { }
                    }
                }

                if (deleteCount > 0)
                {
                    _dbStream.Seek(_header_end_offset, SeekOrigin.Begin);

                    // Write the number of records
                    _dataWriter.Write(_numRecords);

                    // Write the number of (unclean) deleted records
                    _dataWriter.Write(_numDeleted);

                    Debug.Assert(_deletedRecords.Count == _numDeleted);
                }
            }
            finally
            {
                if (AutoFlush) Flush(true);
            }

            // Do an auto-cleanup if required
            checkAutoClean();

            return deleteCount;
        }

        bool isEof
        {
            get
            {
                return _iteratorIndex >= _numRecords;
            }
        }

        /// <summary>
        /// move to the first index position
        /// </summary>
        /// 
        internal bool MoveFirst()
        {
            checkIsDbOpen();

            _iteratorIndex = 0;
            return !isEof;
        }

        /// <summary>
        ///  Move the current index position to the next database item.
        /// </summary>
        /// <returns>true if advanced to a new item, false if there are none left</returns>
        /// 
        internal bool MoveNext()
        {
            checkIsDbOpen();

            bool result = false;

            // No items?
            if (_numRecords == 0 || isEof)
                return result;

            _iteratorIndex++;
            result = _iteratorIndex < _numRecords;

            return result;
        }


        /// <summary>
        /// Return the current record in the database.  Note that the current iterator pointer is not moved in any way.
        /// </summary>
        /// <returns></returns>
        /// 
        internal object[] GetCurrentRecord(bool includeIndex)
        {
            checkIsDbOpen();

            // No items?
            if (_numRecords == 0)
                return null;

            object[] record = null;

            //try
            //{
            //lockRead( false );

            // No more records left?
            if (isEof)
            {
                throw new FileDbException(FileDbException.IteratorPastEndOfFile, FileDbExceptionsEnum.IteratorPastEndOfFile);
            }

            int indexOffset = _index[_iteratorIndex];
            record = readRecord(indexOffset, includeIndex);

            if (includeIndex)
            {
                // set the index into the record
                record[record.Length - 1] = _iteratorIndex;
            }
            //}
            //finally
            //{
            //}

            // Return the record
            return record;
        }

        /// <summary>
        /// retrieves a record based on the specified key
        /// </summary>
        /// <param name="key">primary key used to identify record to retrieve.  For
        /// databases without primary keys, it is the record number (zero based) in 
        /// the table.</param>
        /// <param name="fieldList"></param>
        /// <param name="includeIndex">if true, an extra field called 'IFIELD' will
        /// be added to each record returned.  It will contain an Int32 that specifies
        /// the original position in the database (zero based) that the record is 
        /// positioned.  It might be useful when an orderby is used, and a future 
        /// operation on a record is required, given it's index in the table.</param>
        /// <returns>record if found, or false otherwise</returns>
        /// 
        internal object[] GetRecordByKey(object key, string[] fieldList, bool includeIndex)
        {
            checkIsDbOpen();

            object[] record = null;

            //try
            //{
            //lockRead( false );

            // Read the index
            //Int32[] vIndex = readIndex();

            Int32 offset, idx;

            if (!string.IsNullOrEmpty(_primaryKey))
            {
                // Do a binary search to find the item
                idx = bsearch(_index, 0, _numRecords - 1, key);

                if (idx < 0)
                {
                    // Not found!
                    return null;
                }

                // bsearch always returns the real position + 1
                --idx;

                // Get the offset of the record in the database
                offset = _index[idx];
            }
            else
            {
                idx = (Int32) key;

                // Ensure the record number is an Int32 and within range
                if ((idx < 0) || (idx >= _numRecords))
                {
                    //user_error("Invalid record number (key).", E_USER_ERROR);
                    return null;
                }

                offset = _index[idx];
            }

            // Read the record
            //bool includeIndex = fieldListContainsIndex( fieldList );
            bool deleted;
            record = readRecord(offset, includeIndex, out deleted);
            Debug.Assert(!deleted);

            if (fieldList != null)
            {
                object[] tmpRecord = new object[fieldList.Length + (includeIndex ? 1 : 0)]; // one extra for the index
                int n = 0;
                foreach (string fieldName in fieldList)
                {
                    if (!_fields.ContainsKey(fieldName))
                        throw new FileDbException(string.Format(FileDbException.InvalidFieldName, fieldName), FileDbExceptionsEnum.InvalidFieldName);
                    Field fld = _fields[fieldName];
                    tmpRecord[n++] = record[fld.Ordinal];
                }
                record = tmpRecord;
            }

            if (includeIndex)
            {
                // set the index into the record
                record[record.Length - 1] = idx++;
            }
            //}
            //finally
            //{
            //}

            return record;
        }

        /// <summary>
        /// retrieves a record based on the record number in the table
        /// (zero based)
        /// </summary>
        /// <param name="idx">zero based record number to retrieve</param>
        /// <param name="fieldList"></param>
        /// <param name="includeIndex"></param>
        /// <returns></returns>
        /// 
        internal object[] GetRecordByIndex(Int32 idx, string[] fieldList, bool includeIndex)
        {
            checkIsDbOpen();

            // Ensure the record number is within range
            if ((idx < 0) || (idx >= _numRecords))
            {
                //throw new FileDbException( string.Format( FileDbException.RecordNumOutOfRange, idx ), FileDbExceptions.IndexOutOfRange );
                return null;
            }

            object[] record = null;

            //try
            //{
            //lockRead( false );

            // Read the index
            //Int32[] vIndex = readIndex();
            Int32 offset = _index[idx];

            // Read the record
            //bool includeIndex = fieldListContainsIndex( fieldList );
            bool deleted;
            record = readRecord(offset, includeIndex, out deleted);
            Debug.Assert(!deleted);

            if (fieldList != null)
            {
                object[] tmpRecord = new object[fieldList.Length + (includeIndex ? 1 : 0)]; // one extra for the index
                int n = 0;
                foreach (string fieldName in fieldList)
                {
                    if (!_fields.ContainsKey(fieldName))
                        throw new FileDbException(string.Format(FileDbException.InvalidFieldName, fieldName), FileDbExceptionsEnum.InvalidFieldName);
                    Field fld = _fields[fieldName];
                    tmpRecord[n++] = record[fld.Ordinal];
                }
                record = tmpRecord;
            }

            if (includeIndex)
            {
                // set the index into the record
                record[record.Length - 1] = idx++;
            }
            //}
            //finally
            //{
            //}

            return record;
        }

        internal object[][] GetRecordByField(FilterExpression searchExp, string[] fieldList, bool includeIndex, string[] orderByList)
        {
            checkIsDbOpen();

            string fieldName = searchExp.FieldName;
            if (fieldName[0] == '~')
            {
                fieldName = fieldName.Substring(1);
                searchExp.MatchType = MatchTypeEnum.IgnoreCase;
            }

            // Check the field name is valid
            if (!_fields.ContainsKey(fieldName))
                throw new FileDbException(string.Format(FileDbException.InvalidFieldName, searchExp.FieldName), FileDbExceptionsEnum.InvalidFieldName);

            Field field = _fields[fieldName];

            // If there are no records, return
            if (_numRecords == 0)
                return null;

            object[][] result = null;

            //try
            //{
            //lockRead( false );

            // Read the index
            //Int32[] vIndex = readIndex();

            var lstResults = new List<object[]>();

            // Read each record and add it to an array
            Int32 idx = 0;
            Regex regex = null;

            //bool includeIndex = fieldListContainsIndex( fieldList );

            foreach (Int32 offset in _index)
            {
                // Read the record
                bool deleted;
                object[] record = readRecord(offset, includeIndex, out deleted);
                Debug.Assert(!deleted);

                if (searchExp.Equality == ComparisonOperatorEnum.Regex && regex == null)
                    regex = new Regex(searchExp.SearchVal.ToString(), RegexOptions.IgnoreCase);

                bool isMatch = evaluate(field, searchExp, record, regex);

                if (isMatch)
                {
                    if (fieldList != null)
                    {
                        object[] tmpRecord = new object[fieldList.Length + (includeIndex ? 1 : 0)]; // one extra for the index
                        int n = 0;
                        foreach (string fldName in fieldList)
                        {
                            if (!_fields.ContainsKey(fldName))
                                throw new FileDbException(string.Format(FileDbException.InvalidFieldName, fldName), FileDbExceptionsEnum.InvalidFieldName);
                            Field fld = _fields[fldName];
                            tmpRecord[n++] = record[fld.Ordinal];
                        }
                        record = tmpRecord;
                    }

                    if (includeIndex)
                    {
                        // set the index into the record
                        record[record.Length - 1] = idx++;
                    }

                    lstResults.Add(record);
                }

                idx++;
            }

            result = lstResults.ToArray();
            //}
            //finally
            //{
            //}

            // Re-order as required
            if (result != null && orderByList != null)
                orderBy(result, fieldList, orderByList);

            return result;
        }

        internal object[][] GetRecordByFields(FilterExpressionGroup searchExpGrp, string[] fieldList, bool includeIndex, string[] orderByList)
        {
            checkIsDbOpen();

            // If there are no records, return
            if (_numRecords == 0)
                return null;

            object[][] result = null;

            //try
            //{
            //lockRead( false );

            // Read the index
            //Int32[] vIndex = readIndex();

            var lstResults = new List<object[]>();

            // Read each record and add it to an array
            Int32 idx = 0;
            //bool includeIndex = fieldListContainsIndex( fieldList );

            foreach (Int32 offset in _index)
            {
                // Read the record
                bool deleted;
                object[] record = readRecord(offset, includeIndex, out deleted);
                Debug.Assert(!deleted);

                bool isMatch = searchExpGrp == null || Evaluate(searchExpGrp, record, _fields);

                if (isMatch)
                {
                    if (fieldList != null)
                    {
                        object[] tmpRecord = new object[fieldList.Length + (includeIndex ? 1 : 0)]; // one extra for the index
                        int n = 0;
                        foreach (string fieldName in fieldList)
                        {
                            if (!_fields.ContainsKey(fieldName))
                                throw new FileDbException(string.Format(FileDbException.InvalidFieldName, fieldName), FileDbExceptionsEnum.InvalidFieldName);
                            Field fld = _fields[fieldName];
                            tmpRecord[n++] = record[fld.Ordinal];
                        }
                        record = tmpRecord;
                    }

                    if (includeIndex)
                    {
                        // set the index into the record
                        record[record.Length - 1] = idx++;
                    }

                    lstResults.Add(record);
                }

                idx++;
            }

            result = lstResults.ToArray();
            //}
            //finally
            //{
            //}

            // Re-order as required
            if (result != null && orderByList != null)
                orderBy(result, fieldList, orderByList);

            return result;
        }

        internal static bool Evaluate(FilterExpressionGroup searchExpGrp, object[] record, Fields fields)
        {
            if (searchExpGrp == null || searchExpGrp.Expressions.Count == 0)
                return true;

            bool isMatch = false;

            // if an express is null its automatically a match
            int ndx = 0;
            foreach (object searchExpressionOrGroup in searchExpGrp.Expressions)
            {
                bool thisMatch = false;

                if (searchExpressionOrGroup == null)
                    continue;

                BoolOpEnum boolOp;

                if (searchExpressionOrGroup is FilterExpressionGroup) // searchExpressionOrGroup.GetType() == typeof( FilterExpressionGroup ) )
                {
                    var sexg = searchExpressionOrGroup as FilterExpressionGroup;
                    thisMatch = Evaluate(sexg, record, fields);
                    boolOp = sexg.BoolOp;
                }
                else
                {
                    var searchExp = searchExpressionOrGroup as FilterExpression;
                    boolOp = searchExp.BoolOp;

                    string fieldName = searchExp.FieldName;
                    if (fieldName[0] == '~')
                    {
                        fieldName = fieldName.Substring(1);
                        searchExp.MatchType = MatchTypeEnum.IgnoreCase;
                    }

                    // Check the field name is valid
                    if (!fields.ContainsKey(fieldName))
                        throw new FileDbException(string.Format(FileDbException.InvalidFieldName, searchExp.FieldName), FileDbExceptionsEnum.InvalidFieldName);

                    Field field = fields[fieldName];
                    thisMatch = evaluate(field, searchExp, record, null);
                }

                if (ndx == 0)
                {
                    // the first time through the loop there is no boolean test
                    isMatch = thisMatch;
                }
                else
                {
                    if (boolOp == BoolOpEnum.And)
                    {
                        isMatch = isMatch && thisMatch;
                        // we can stop as soon as one doesn't match when ANDing
                        if (!isMatch)
                            break;
                    }
                    else
                    {
                        isMatch = isMatch || thisMatch;
                        // we can stop as soon as a match is found when ORing
                        if (isMatch)
                            break;
                    }
                }
                ndx++;
            }

            return isMatch;
        }

        static bool evaluate(Field field, FilterExpression searchExp, object[] record, Regex regex)
        {
            // we currently don't support array searches
            if (field.IsArray)
                return false;

            var compareResult = ComparisonOperatorEnum.NotEqual;

            // get the field value
            object val = record[field.Ordinal];

            bool isMatch = false;

            if (val == null && searchExp.SearchVal == null) // both null - maybe not possible?
                return true;

            else if (val == null || searchExp.SearchVal == null) // only 1 is null
                return false;

            // neither null

            if (searchExp.Equality == ComparisonOperatorEnum.Contains)
            {
                // hopefully searching for strings

                string str = val.ToString();

                if (!string.IsNullOrEmpty(str))
                {
                    // See if the record matches the regular expression
                    var idx = str.IndexOf(searchExp.SearchVal.ToString(), searchExp.MatchType == MatchTypeEnum.IgnoreCase ?
                        StringComparison.CurrentCultureIgnoreCase : StringComparison.CurrentCulture);
                    isMatch = idx >= 0;
                }
            }
            else if (searchExp.Equality == ComparisonOperatorEnum.Regex)
            {
                // hopefully searching for strings

                if (regex == null)
                    regex = new Regex(searchExp.SearchVal.ToString(), RegexOptions.IgnoreCase);

                string str = val.ToString();

                if (!string.IsNullOrEmpty(str))
                {
                    // See if the record matches the regular expression
                    isMatch = regex.IsMatch(str);
                }
            }
            else if (searchExp.Equality == ComparisonOperatorEnum.In) //|| searchExp.Equality == EqualityEnum.NotIn )
            {
                var hashSet = searchExp.SearchVal as HashSet<object>;
                if (hashSet == null)
                    throw new FileDbException(FileDbException.HashSetExpected, FileDbExceptionsEnum.HashSetExpected);

                // If the HashSet was created by the FilterExpression parser, the Field type wasn't
                // yet known so all of the values will be string.  We must convert them to the
                // Field type now.

                if (field.DataType != DataTypeEnum.String)
                {
                    var tempHashSet = new HashSet<object>();
                    foreach (object obj in hashSet)
                    {
                        tempHashSet.Add(convertValueToType(obj, field.DataType));
                    }
                    hashSet = tempHashSet;
                    searchExp.SearchVal = tempHashSet;
                }
                else
                {
                    if (searchExp.MatchType == MatchTypeEnum.IgnoreCase)
                        val = val.ToString().ToUpper();
                }

                isMatch = hashSet.Contains(val);
            }
            else // all others
            {
                if (field.DataType == DataTypeEnum.String)
                {
                    int ncomp = string.Compare(searchExp.SearchVal.ToString(), val.ToString(),
                        searchExp.MatchType == MatchTypeEnum.UseCase
                            ? StringComparison.CurrentCulture
                            : StringComparison.CurrentCultureIgnoreCase);

                    compareResult = ncomp == 0
                        ? ComparisonOperatorEnum.Equal
                        : (ncomp > 0 ? ComparisonOperatorEnum.GreaterThan : ComparisonOperatorEnum.LessThan);
                }
                else
                {
                    compareResult = compareVals(field, val, searchExp.SearchVal);
                }

                // compareResult should only be one of: Equal, NotEqual, GreaterThan, LessThan

                // first check for NotEqual since it would be anythying which is not Equal

                if (searchExp.Equality == ComparisonOperatorEnum.NotEqual)
                {
                    // a match is anything BUT equal
                    if (compareResult != ComparisonOperatorEnum.Equal)
                        isMatch = true;
                }
                else
                {
                    // are they the same?
                    if (compareResult == searchExp.Equality)
                    {
                        isMatch = true;
                    }
                    else
                    {
                        if (compareResult == ComparisonOperatorEnum.Equal)
                        {
                            if (searchExp.Equality == ComparisonOperatorEnum.Equal ||
                                searchExp.Equality == ComparisonOperatorEnum.LessThanOrEqual ||
                                searchExp.Equality == ComparisonOperatorEnum.GreaterThanOrEqual)
                            {
                                isMatch = true;
                            }
                        }
                        else if (compareResult == ComparisonOperatorEnum.NotEqual)
                        {
                            if (searchExp.Equality == ComparisonOperatorEnum.NotEqual ||
                                searchExp.Equality == ComparisonOperatorEnum.LessThan ||
                                searchExp.Equality == ComparisonOperatorEnum.GreaterThan)
                            {
                                isMatch = true;
                            }
                        }
                        else if (compareResult == ComparisonOperatorEnum.LessThan &&
                                 (searchExp.Equality == ComparisonOperatorEnum.LessThan ||
                                  searchExp.Equality == ComparisonOperatorEnum.LessThanOrEqual))
                        {
                            isMatch = true;
                        }
                        else if (compareResult == ComparisonOperatorEnum.GreaterThan &&
                                 (searchExp.Equality == ComparisonOperatorEnum.GreaterThan ||
                                  searchExp.Equality == ComparisonOperatorEnum.GreaterThanOrEqual))
                        {
                            isMatch = true;
                        }
                    }
                }
            }

            if (searchExp.IsNot)
                isMatch = !isMatch; // NOT the whole thing

            return isMatch;
        }

        static ComparisonOperatorEnum compareVals(Field field, object val1, object val2)
        {
            var retVal = ComparisonOperatorEnum.NotEqual;

            switch (field.DataType)
            {
                case DataTypeEnum.Byte:
                    {
                        Byte b1 = Convert.ToByte(val1),
                             b2 = Convert.ToByte(val2);

                        retVal = b1 == b2 ? ComparisonOperatorEnum.Equal : (b1 > b2 ? ComparisonOperatorEnum.GreaterThan : ComparisonOperatorEnum.LessThan);
                    }
                    break;

                case DataTypeEnum.Bool:
                    {
                        bool b1 = Convert.ToBoolean(val1),
                             b2 = Convert.ToBoolean(val2);

                        retVal = b1 == b2 ? ComparisonOperatorEnum.Equal : ComparisonOperatorEnum.NotEqual;
                    }
                    break;

                case DataTypeEnum.Float:
                    {
                        float f1 = Convert.ToSingle(val1),
                              f2 = Convert.ToSingle(val2);

                        retVal = f1 == f2 ? ComparisonOperatorEnum.Equal : (f1 > f2 ? ComparisonOperatorEnum.GreaterThan : ComparisonOperatorEnum.LessThan);
                    }
                    break;

                case DataTypeEnum.Double:
                    {
                        double d1 = Convert.ToDouble(val1),
                               d2 = Convert.ToDouble(val2);

                        retVal = d1 == d2 ? ComparisonOperatorEnum.Equal : (d1 > d2 ? ComparisonOperatorEnum.GreaterThan : ComparisonOperatorEnum.LessThan);
                    }
                    break;

                case DataTypeEnum.Int32:
                    {
                        Int32 i1 = Convert.ToInt32(val1),
                              i2 = Convert.ToInt32(val2);

                        /*if( i1 > 65 )
                        {
                            int debug = 0;
                        }*/
                        retVal = i1 == i2 ? ComparisonOperatorEnum.Equal : (i1 > i2 ? ComparisonOperatorEnum.GreaterThan : ComparisonOperatorEnum.LessThan);
                    }
                    break;

                case DataTypeEnum.UInt32:
                    {
                        UInt32 i1 = Convert.ToUInt32(val1),
                               i2 = Convert.ToUInt32(val2);

                        retVal = i1 == i2 ? ComparisonOperatorEnum.Equal : (i1 > i2 ? ComparisonOperatorEnum.GreaterThan : ComparisonOperatorEnum.LessThan);
                    }
                    break;

                case DataTypeEnum.DateTime:
                    {
                        DateTime dt1 = Convert.ToDateTime(val1),
                                 dt2 = Convert.ToDateTime(val2);

                        retVal = dt1 == dt2 ? ComparisonOperatorEnum.Equal : (dt1 > dt2 ? ComparisonOperatorEnum.GreaterThan : ComparisonOperatorEnum.LessThan);
                    }
                    break;

                case DataTypeEnum.String:
                    {
                        int ncomp = string.Compare(val1.ToString(), val2.ToString(), StringComparison.CurrentCulture);
                        retVal = ncomp == 0 ? ComparisonOperatorEnum.Equal : (ncomp > 0 ? ComparisonOperatorEnum.GreaterThan : ComparisonOperatorEnum.LessThan);
                    }
                    break;

                case DataTypeEnum.Int64:
                    {
                        Int64 i1 = Convert.ToInt64(val1),
                              i2 = Convert.ToInt64(val2);

                        retVal = i1 == i2 ? ComparisonOperatorEnum.Equal : (i1 > i2 ? ComparisonOperatorEnum.GreaterThan : ComparisonOperatorEnum.LessThan);
                    }
                    break;

                case DataTypeEnum.Decimal:
                    {
                        Decimal d1 = Convert.ToDecimal(val1),
                                d2 = Convert.ToDecimal(val2);

                        retVal = d1 == d2 ? ComparisonOperatorEnum.Equal : (d1 > d2 ? ComparisonOperatorEnum.GreaterThan : ComparisonOperatorEnum.LessThan);
                    }
                    break;

                case DataTypeEnum.Guid:
                    {
                        Guid g1 = convertToGuid(val1),
                             g2 = convertToGuid(val2);

                        retVal = g1.CompareTo(g2) == 0 ? ComparisonOperatorEnum.Equal : ComparisonOperatorEnum.NotEqual;
                    }
                    break;
            }

            return retVal;
        }

        static Guid convertToGuid(object val)
        {
            Guid guid;
            Type type = val.GetType();

            if (type == typeof(Guid))
                guid = (Guid) val;
            else if (type == typeof(byte[]))
                guid = new Guid((byte[]) val);
            else if (type == typeof(string))
                guid = new Guid((string) val);
            else
                throw new FileDbException(string.Format(FileDbException.CantConvertTypeToGuid, type.ToString()),
                    FileDbExceptionsEnum.CantConvertTypeToGuid);

            return guid;
        }

        static object convertValueToType(object value, DataTypeEnum dataType)
        {
            object retVal = null;

            switch (dataType)
            {
                case DataTypeEnum.Byte:
                    {
                        retVal = Convert.ToByte(value);
                    }
                    break;

                case DataTypeEnum.Bool:
                    {
                        retVal = Convert.ToBoolean(value);
                    }
                    break;

                case DataTypeEnum.Float:
                    {
                        retVal = Convert.ToSingle(value);
                    }
                    break;

                case DataTypeEnum.Double:
                    {
                        retVal = Convert.ToDouble(value);
                    }
                    break;

                case DataTypeEnum.Int32:
                    {
                        retVal = Convert.ToInt32(value);
                    }
                    break;

                case DataTypeEnum.UInt32:
                    {
                        retVal = Convert.ToUInt32(value);
                    }
                    break;

                case DataTypeEnum.DateTime:
                    {
                        retVal = Convert.ToDateTime(value);
                    }
                    break;

                case DataTypeEnum.String:
                    {
                        retVal = value.ToString();
                    }
                    break;

                case DataTypeEnum.Int64:
                    {
                        retVal = Convert.ToInt64(value);
                    }
                    break;

                case DataTypeEnum.Decimal:
                    {
                        retVal = Convert.ToDecimal(value);
                    }
                    break;

                case DataTypeEnum.Guid:
                    {
                        retVal = convertToGuid(value);
                    }
                    break;
            }
            return retVal;
        }

        internal object[][] GetAllRecords(string[] fieldList, bool includeIndex, string[] orderByList)
        {
            checkIsDbOpen();

            // If there are no records, return
            if (_numRecords == 0)
                return null;

            object[][] result = null;

            //try
            //{
            //lockRead( false );

            // Read the index
            //Int32[] vIndex = readIndex();

            result = new object[_numRecords][];

            //bool includeIndex = fieldListContainsIndex( fieldList );

            // Read each record and add it to an array
            Int32 idx = 0,
                  nRow = 0;

            foreach (Int32 offset in _index)
            {
#if DEBUG
                try
                {
#endif
                    // Read the record
                    bool deleted;
                    object[] record = readRecord(offset, includeIndex, out deleted);
                    Debug.Assert(!deleted);

                    if (fieldList != null)
                    {
                        object[] tmpRecord = new object[fieldList.Length + (includeIndex ? 1 : 0)]; // one extra for the index
                        int n = 0;
                        foreach (string fieldName in fieldList)
                        {
                            if (!_fields.ContainsKey(fieldName))
                                throw new FileDbException(string.Format(FileDbException.InvalidFieldName, fieldName), FileDbExceptionsEnum.InvalidFieldName);
                            Field fld = _fields[fieldName];
                            tmpRecord[n++] = record[fld.Ordinal];
                        }
                        record = tmpRecord;
                    }

                    if (includeIndex)
                    {
                        // set the index into the record
                        record[record.Length - 1] = idx++;
                    }

                    // Add it to the result
                    result[nRow++] = record;

#if DEBUG
                }
                catch //( Exception ex )
                {
                    throw;
                }
#endif
            }
            //}
            //finally
            //{
            //}

            // Re-order as required
            if (result != null && orderByList != null)
                orderBy(result, fieldList, orderByList);

            return result;
        }

#if false
        /*
         * retrieves all keys in the database, each in an array.
         * returns: all database record keys as an array, in order, or false
         * if the database does not use keys.
         */
        internal object[] GetKeys()
        {
            checkIsDbOpen();

            // If there is no key, return false
            if( string.IsNullOrEmpty( _primaryKey ) )
                return null;

            // If there are no records, return
            if( _numRecords == 0 )
                return null;

            object[] record = null;

            try
            {
                //lockRead( false );

                // Read the index
                Int32[] vIndex = readIndex();

                var lstRecords = new List<object>();

                // Read each record key and add it to an array
                foreach( Int32 offset in vIndex )
                {
                    // Read the record key and add it to the result
                    object key = readRecordKey( _dataReader, offset );
                    lstRecords.Add( key );
                }

                if( lstRecords.Count > 0 )
                    record = lstRecords.ToArray();
            }
            finally
            {
            }

            return record;
        }
#endif
        /// <summary>
        /// Searches the database for an item, and returns true if found, false otherwise.
        /// </summary>
        /// <param name="key">rimary key of record to search for, or the record
        /// number (zero based) for databases without a primary key</param>
        /// <returns>true if found, false otherwise</returns>
        /// 
        internal bool RecordExists(Int32 key)
        {
            checkIsDbOpen();

            // Assume we won't find it until proven otherwise
            bool result = false;

            //try
            //{
            //lockRead( false );

            // Read the index
            //Int32[] vIndex = readIndex();

            if (!string.IsNullOrEmpty(_primaryKey))
            {
                // Do a binary search to find the item
                Int32 pos = bsearch(_index, 0, _numRecords - 1, key);

                if (pos > 0)
                {
                    // Found!
                    result = true;
                }
            }
            else
            {
                // if there is no primary key, the record number must be Int32
                if (key.GetType() != typeof(int))
                {
                    throw new FileDbException(FileDbException.NeedIntegerKey, FileDbExceptionsEnum.NeedIntegerKey);
                }

                Int32 nkey = (Int32) key;

                // Ensure the record number is within range
                if ((nkey < 0) || (nkey >= _numRecords))
                {
                    throw new FileDbException(string.Format(FileDbException.RecordNumOutOfRange, nkey), FileDbExceptionsEnum.IndexOutOfRange);
                }

                // ... must be found!
                result = true;
            }
            //}
            //finally
            //{
            //}

            return result;
        }

        /// <summary>
        /// Returns the number of records in the database
        /// </summary>
        /// <returns>the number of records in the database</returns>
        /// 
        internal Int32 GetRecordCount()
        {
            checkIsDbOpen();
            return _numRecords;
        }

        /// <summary>
        /// Returns the number of deleted records in the database, that would be removed if cleanup() is called.
        /// </summary>
        /// <returns>the number of deleted records in the database</returns>
        /// 
        internal Int32 GetDeletedRecordCount()
        {
            checkIsDbOpen();
            return _numDeleted;
        }

        /// <summary>
        /// Returns the current database schema in the same form
        /// as that used in the parameter for the create(...) method.
        /// </summary>
        /// <returns></returns>
        /// 
        internal Field[] GetSchema()
        {
            checkIsDbOpen();
            return _fields.ToArray();
        }

        internal void AddFields(FileDb thisDb, Field[] fieldsToAdd, object[] defaultVals)
        {
            if (fieldsToAdd == null || fieldsToAdd.Length == 0)
                throw new FileDbException(FileDbException.FieldListIsEmpty, FileDbExceptionsEnum.FieldListIsEmpty);

            checkIsDbOpen();
            checkReadOnly();

            Fields newFields = new Fields(_fields);

            for (int n = 0; n < fieldsToAdd.Length; n++)
            {
                var fld = fieldsToAdd[n];
                object defaultVal = null;
                if (defaultVals != null)
                    defaultVal = defaultVals[n];

                if (fld.IsPrimaryKey && (!string.IsNullOrEmpty(_primaryKey)))
                    throw new FileDbException(string.Format(FileDbException.DatabaseAlreadyHasPrimaryKey, fld.Name),
                        FileDbExceptionsEnum.DatabaseAlreadyHasPrimaryKey);

                // Only allow keys if the database has no records
                if (fld.IsPrimaryKey && (_numRecords > 0))
                    throw new FileDbException(string.Format(FileDbException.PrimaryKeyCannotBeAdded, fld.Name),
                            FileDbExceptionsEnum.PrimaryKeyCannotBeAdded);

                // ensure no deleted records either
                //if( _numDeleted > 0 )
                //throw new FileDbException( FileDbException.CantAddOrRemoveFieldWithDeletedRecords, FileDbExceptionsEnum.CantAddOrRemoveFieldWithDeletedRecords );

                // Make sure the name of the field is unique
                if (_fields.Any(f => string.Compare(fld.Name, f.Name, StringComparison.CurrentCultureIgnoreCase) == 0))
                    throw new FileDbException(string.Format(FileDbException.FieldNameAlreadyExists, fld.Name),
                        FileDbExceptionsEnum.FieldNameAlreadyExists);

                // Make sure that the array or boolean value is NOT the key
                if (fld.IsPrimaryKey &&
                        (fld.IsArray ||
                            !(fld.DataType == DataTypeEnum.Int32 || fld.DataType == DataTypeEnum.String)))
                    throw new FileDbException(string.Format(FileDbException.InvalidPrimaryKeyType, fld.Name),
                        FileDbExceptionsEnum.InvalidPrimaryKeyType);

                if (defaultVal != null)
                    verifyFieldSchema(fld, defaultVal);

                newFields.Add(fld);
            }

            // Create a new FileDb
            copyNewDB(thisDb, newFields, fieldsToAdd, null, null);
        }

        void assignPublicProperties(FileDb tempDb, FileDb db)
        {
            tempDb.AutoCleanThreshold = db.AutoCleanThreshold;
            tempDb.AutoFlush = db.AutoFlush;
            tempDb.UserData = db.UserData;
            tempDb.UserVersion = db.UserVersion;
        }

        internal void DeleteFields(FileDb thisDb, string[] fieldsToRemove)
        {
            if (fieldsToRemove == null || fieldsToRemove.Length == 0)
                throw new FileDbException(FileDbException.FieldListIsEmpty, FileDbExceptionsEnum.FieldListIsEmpty);

            checkIsDbOpen();
            checkReadOnly();

            Fields newFields = new Fields(_fields.Count);

            foreach (var fldName in fieldsToRemove)
            {
                if (_primaryKey != null && string.Compare(fldName, _primaryKey) == 0)
                    throw new FileDbException(string.Format(FileDbException.CannotDeletePrimaryKeyField, fldName),
                        FileDbExceptionsEnum.CannotDeletePrimaryKeyField);
            }

            // add only fields not in the delete list
            foreach (var fld in _fields)
            {
                if (!fieldsToRemove.Any(f => string.Compare(f, fld.Name, StringComparison.CurrentCultureIgnoreCase) == 0))
                    newFields.Add(fld);
            }

            // Create a new FileDb
            copyNewDB(thisDb, newFields, null, null, fieldsToRemove);
        }

#if !(NETFX_CORE || PCL)
        string getTempDbFilename()
        {
            string tmpName = Path.GetTempFileName() + ".filedb";
            return tmpName;
        }
#endif

        void copyNewDB(FileDb thisDb, Fields newFields, Field[] fieldsToAdd, object[] defaultVals, string[] fieldsToRemove)
        {
            // Create a new FileDb

            FileDb tempDb = new FileDb();

            Stream newDbStream = null;

            // we'll use a memory DB for handheld devices because the databases should be small enough
#if NETFX_CORE || PCL
            tempDb.Create(null, newFields);
#else
            string tmpFullFilename = null, fullFilenameBak = null;
            if (_dbFileName != null)
            {
                tmpFullFilename = getTempDbFilename();
                fullFilenameBak = getTempDbFilename();
            }
            // if tmpFullFilename is null, a memory DB will be created
            tempDb.Create(tmpFullFilename, newFields); // FolderLocEnum.TempFolder );
#endif

            tempDb.SuspendAutoInc();

            try
            {
                assignPublicProperties(tempDb, thisDb);

                FieldValues values = new FieldValues(newFields.Count);

                if (MoveFirst())
                {
                    do
                    {
                        object[] row = GetCurrentRecord(false);
                        object val;
                        Field fld;

                        if (fieldsToRemove != null)
                        {
                            for (int n = 0; n < row.Length; n++)
                            {
                                val = row[n];
                                fld = _fields[n];

                                // are we removing any fields?
                                if (!fieldsToRemove.Any(f => string.Compare(f, fld.Name, StringComparison.CurrentCultureIgnoreCase) == 0))
                                {
                                    values.Add(fld.Name, val);
                                }
                            }
                        }
                        else // adding or renaming a field
                        {
                            for (int n = 0; n < row.Length; n++)
                            {
                                val = row[n];
                                fld = newFields[n];
                                values.Add(fld.Name, val);
                            }

                            // are we adding new fields?
                            if (fieldsToAdd != null)
                            {
                                // add the new Field default values, if any
                                if (defaultVals != null)
                                {
                                    for (int n = 0; n < fieldsToAdd.Length; n++)
                                    {
                                        val = defaultVals != null ? defaultVals[n] : null;
                                        fld = fieldsToAdd[n];
                                        values.Add(fld.Name, val);
                                    }
                                }
                            }
                        }

                        tempDb.AddRecord(values);
                        values.Clear();

                    } while (MoveNext());
                }

                // we must set the new AutoInc vals into the fields else they will still be at their initial values
                bool hasAutoIncFields = false;
                for (int n = 0; n < _fields.Count; n++)
                {
                    var field1 = _fields[n];
                    if (field1.IsAutoInc)
                    {
                        hasAutoIncFields = true;
                        var field2 = tempDb.Fields[field1.Name];
                        field2.CurAutoIncVal = field1.CurAutoIncVal;
                    }
                }
                if (hasAutoIncFields) // update schema only if needed
                    tempDb.WriteSchema();

                tempDb.ResumeAutoInc();

#if NETFX_CORE || PCL
                newDbStream = tempDb._dbEngine.detachDataStreamAndClose();
#else
                if (tmpFullFilename == null)
                    newDbStream = tempDb._dbEngine.detachDataStreamAndClose();
                tempDb.Close();
#endif
            }
            catch
            {
                // cleanup
                if (tempDb.IsOpen)
                    tempDb.Close();

#if !(NETFX_CORE || PCL)
                if (tmpFullFilename != null)
                    File.Delete(tmpFullFilename); // FolderLocEnum.TempFolder );
#endif

                throw;
            }

            // must close DB, rename files and reopen
            string origDbFilename = _dbFileName;
            //FolderLocEnum folderLoc = _folderLoc;
            IEncryptor encryptor = _encryptor;
            thisDb.Close();

#if NETFX_CORE || PCL
            // open using the new Stream
            Open(newDbStream, encryptor);
#else
            if (tmpFullFilename != null)
            {
                // Move the temporary file over the original database file
                File.Move(origDbFilename, fullFilenameBak);
                File.Move(tmpFullFilename, origDbFilename);
                File.Delete(fullFilenameBak);
                // reopen the DB
                Open(origDbFilename, null, encryptor, false); // folderLoc );
            }
            else
            {
                Open(null, newDbStream, encryptor, false); // folderLoc );
            }
#endif
        }

#if false // NETFX_CORE && !PCL
        static void deleteFile( string filename, FolderLocEnum folderLoc )
        {
#if NETFX_CORE
            var storageFile = RunSynchronously( getStorageFolder( folderLoc ).GetFileAsync( filename ) );
            if( storageFile != null )
                deleteStorageFile( storageFile );
#else
            File.Delete( filename );
#endif
        }

        static void deleteStorageFile( StorageFile storageFile )
        {
            RunSynchronously( storageFile.DeleteAsync() );
        }

        static T RunSynchronously<T>( IAsyncOperation<T> asyncOp )
        {
            var evt = new AutoResetEvent( false );
            asyncOp.Completed = delegate
            {
                evt.Set();
            };
            if( !evt.WaitOne( AsyncWaitTimeout ) )
                throw new FileDbException( FileDbException.AsyncOperationTimeout, FileDbExceptionsEnum.AsyncOperationTimeout );
            T results = asyncOp.GetResults();
            return results;
        }

        static void RunSynchronously( IAsyncAction asyncOp )
        {
            var evt = new AutoResetEvent( false );
            asyncOp.Completed = delegate
            {
                evt.Set();
            };
            if( !evt.WaitOne( AsyncWaitTimeout ) )
                throw new FileDbException( FileDbException.AsyncOperationTimeout, FileDbExceptionsEnum.AsyncOperationTimeout );
            asyncOp.GetResults();
        }

        static void RunTaskSynchronously( Task task )
        {
            var evt = new AutoResetEvent( false );
            task.ContinueWith( delegate 
            {
                evt.Set();
            } );
            if( !evt.WaitOne( AsyncWaitTimeout ) )
                throw new FileDbException( FileDbException.AsyncOperationTimeout, FileDbExceptionsEnum.AsyncOperationTimeout );
        }

        static T RunSynchronously<T>( Task<T> task )
        {
            var evt = new AutoResetEvent( false );
            task.ContinueWith( delegate
            {
                evt.Set();
            } );
            if( !evt.WaitOne( AsyncWaitTimeout ) )
                throw new FileDbException( FileDbException.AsyncOperationTimeout, FileDbExceptionsEnum.AsyncOperationTimeout );

            return task.Result;

            //return RunSynchronously( asyncOp.AsAsyncOperation() );
        }
#endif

        internal void RenameField(FileDb thisDb, string fieldName, string newFieldName)
        {
            if (fieldName == null || fieldName.Length == 0 ||
                    newFieldName == null || newFieldName.Length == 0)
                throw new FileDbException(FileDbException.FieldNameIsEmpty, FileDbExceptionsEnum.FieldNameIsEmpty);

            checkIsDbOpen();
            checkReadOnly();

            if (!_fields.ContainsKey(fieldName))
                throw new FileDbException(string.Format(FileDbException.InvalidFieldName, fieldName), FileDbExceptionsEnum.InvalidFieldName);

            if (fieldName.Length == newFieldName.Length)
            {
                // easy - since they are the same length we can just change the name
                var field = _fields[fieldName];
                field.Name = newFieldName;
                writeSchema(_dataWriter);
                _dataWriter.Flush();
                _dbStream.Flush();
                readSchema();
            }
            else
            {
                // harder - must recreate the DB file
                Fields newFields = new Fields(_fields.Count);

                foreach (var fld in _fields)
                {
                    if (string.Compare(fld.Name, fieldName, StringComparison.OrdinalIgnoreCase) == 0)
                    {
                        var newField = fld.Clone();
                        newField.Name = newFieldName;
                        newFields.Add(newField);
                    }
                    else
                        newFields.Add(fld);
                }

                copyNewDB(thisDb, newFields, null, null, null);
            }
        }

        /// <summary>
        /// Flush the in-memory buffers to disk
        /// </summary>
        /// 
        internal void Flush(bool saveIndex)
        {
            if (!_isReadOnly)
            {
                if (saveIndex)
                    writeIndex(_dbStream, _dataWriter, _index);

                _dataWriter.Flush();
                _dbStream.Flush();
            }
        }

        internal void BeginTrans()
        {
            checkIsDbOpen();

            // just make a backup copy

#if NETFX_CORE || PCL
            _transDbStream = new MemoryStream(_dbStream.Length > int.MaxValue ? int.MaxValue : (int) _dbStream.Length);
            var pos = _dbStream.Position;
            _dbStream.Seek(0, SeekOrigin.Begin);
            _dbStream.CopyTo(_transDbStream);
            // set the position back to where it was
            _dbStream.Seek(pos, SeekOrigin.Begin);
            _transDbStream.Seek(pos, SeekOrigin.Begin);
#else
            _transFilename = getTempDbFilename();
            if (File.Exists(_transFilename))
                File.Delete(_transFilename);
            File.Copy(_dbFileName, _transFilename);
#endif
        }

        internal void CommitTrans()
        {
            checkIsDbOpen();

            if (_transDbStream == null
#if !(NETFX_CORE || PCL)
                && _transFilename == null
#endif
                ) throw new FileDbException(FileDbException.NoCurrentTransaction, FileDbExceptionsEnum.NoCurrentTransaction);

            // just delete the backup copy

#if NETFX_CORE || PCL
            _transDbStream.Dispose();
            _transDbStream = null;
#else
            File.Delete(_transFilename);
#endif
        }

        internal void RollbackTrans()
        {
            checkIsDbOpen();

            if (_transDbStream == null
#if !(NETFX_CORE || PCL)
                && _transFilename == null
#endif
                ) throw new FileDbException(FileDbException.NoCurrentTransaction, FileDbExceptionsEnum.NoCurrentTransaction);

            // close the db and copy the backup over the db

            // get the dbFileName, etc. before we close
            IEncryptor encryptor = _encryptor;
            var transDbStream = _transDbStream;

#if !(NETFX_CORE || PCL)
            string dbFileName = _dbFileName;
            //FolderLocEnum folderLoc = _folderLoc;
            bool isReadOnly = _isReadOnly;
            var transFilename = _transFilename;
#endif

            Close();

            // Re-open the database
#if NETFX_CORE || PCL
            Open(transDbStream, encryptor);
#else
            if (dbFileName == null) // memory DB
            {
                // reopen with the backup datastream
                Open(null, transDbStream, encryptor, isReadOnly);
            }
            else
            {
                string backupFilename = getTempDbFilename();
                string tmpFilename = getTempDbFilename();
                // rename the current DB - don't delete yet in case something goes wrong
                File.Move(dbFileName, tmpFilename);
                File.Move(backupFilename, dbFileName);
                File.Delete(tmpFilename);

                Open(dbFileName, null, encryptor, isReadOnly); // folderLoc );
            }
#endif
        }

        internal void SuspendAutoInc()
        {
            _isAutoIncSuspended = true;
        }

        internal void ResumeAutoInc()
        {
            _isAutoIncSuspended = false;
        }

        internal void WriteSchema()
        {
            writeSchema(_dataWriter);
        }

        #endregion internal

        ///////////////////////////////////////////////////////////////////////
        #region private methods

        /// <summary>
        /// Helper
        /// </summary>
        /// <param name="record"></param>
        /// <returns></returns>
        ///
        void verifyRecordSchema(FieldValues record)
        {
            // Verify record as compared to the schema

            foreach (string fieldName in record.Keys)
            {
                if (string.Compare(fieldName, StrIndex, StringComparison.OrdinalIgnoreCase) == 0)
                    continue;

                if (!_fields.ContainsKey(fieldName))
                    throw new FileDbException(string.Format(FileDbException.InvalidFieldName, fieldName), FileDbExceptionsEnum.InvalidFieldName);

                Field field = _fields[fieldName];
                verifyFieldSchema(field, record[fieldName]);
            }
        }

        /// <summary>
        /// Helper
        /// </summary>
        /// <returns></returns>
        ///
        void verifyFieldSchema(Field field, object value)
        {
            // We don't mind if they include a AUTOINC field,
            // as we determine its value in any case

            if (field.IsAutoInc)
                return;  // no value should be passed for autoinc fields, but we'll allow it rather than throwing error

            // Ensure they have included an entry for each record field
            /* brettg: I'm gonna allow sparse records and just write 0 or null for the value of missing fields
            if( !record.ContainsKey( field.Name ) )
            {
                throw("Missing field during add: key");
            }*/

            if (value != null)
            {
                if (field.IsArray && !value.GetType().IsArray)
                    throw new FileDbException(string.Format(FileDbException.NonArrayValue, field.Name), FileDbExceptionsEnum.NonArrayValue);

                // Verify the type
                try
                {
                    switch (field.DataType)
                    {
                        // hopefully these will throw err if wrong type

                        case DataTypeEnum.Byte:
                            if (field.IsArray)
                            {
                                value = (Byte[]) value;
                            }
                            else
                            {
                                Byte b = (value == null ? (Byte) 0 : Convert.ToByte(value));
                            }
                            break;

                        case DataTypeEnum.Int32:
                            if (field.IsArray)
                            {
                                value = (Int32[]) value;
                            }
                            else
                            {
                                Int32 n = (value == null ? 0 : Convert.ToInt32(value));
                            }
                            break;

                        case DataTypeEnum.UInt32:
                            if (field.IsArray)
                            {
                                value = (UInt32[]) value;
                            }
                            else
                            {
                                UInt32 n = (value == null ? (uint) 0 : Convert.ToUInt32(value));
                            }
                            break;

                        case DataTypeEnum.String:
                            if (field.IsArray)
                            {
                                value = (string[]) value;
                            }
                            else
                            {
                                // any object can be converted to string
                                string s = (value == null ? string.Empty : value.ToString());
                            }
                            break;

                        case DataTypeEnum.Float:
                            if (field.IsArray)
                            {
                                value = (float[]) value;
                            }
                            else
                            {
                                float f = (value == null ? (float) 0 : Convert.ToSingle(value));
                            }
                            break;

                        case DataTypeEnum.Double:
                            if (field.IsArray)
                            {
                                value = (double[]) value;
                            }
                            else
                            {
                                double f = (value == null ? 0d : Convert.ToDouble(value));
                            }
                            break;

                        case DataTypeEnum.Bool:
                            if (field.IsArray)
                            {
                                // can be Byte[] or bool[]
                                if (value.GetType() == typeof(Byte[]))
                                    value = (Byte[]) value;
                                else
                                    value = (bool[]) value;
                            }
                            else
                            {
                                bool b = (value == null ? false : Convert.ToBoolean(value)); // brettg: should be 1 or 0
                            }
                            break;

                        case DataTypeEnum.DateTime:
                            if (field.IsArray)
                            {
                                // can be string[] or DateTime[]
                                if (value.GetType() == typeof(String[]))
                                    value = (String[]) value;
                                else
                                    value = (DateTime[]) value;
                            }
                            else
                            {
                                DateTime d = (value == null ? DateTime.MinValue : Convert.ToDateTime(value));
                            }
                            break;

                        case DataTypeEnum.Int64:
                            if (field.IsArray)
                            {
                                value = (Int64[]) value;
                            }
                            else
                            {
                                Int64 f = (value == null ? (Int64) 0 : Convert.ToInt64(value));
                            }
                            break;

                        case DataTypeEnum.Decimal:
                            if (field.IsArray)
                            {
                                value = (Decimal[]) value;
                            }
                            else
                            {
                                Decimal f = (value == null ? (Decimal) 0 : Convert.ToDecimal(value));
                            }
                            break;

                        case DataTypeEnum.Guid:
                            if (field.IsArray)
                            {
                                value = (Guid[]) value;
                            }
                            else
                            {
                                Guid g = value == null ? Guid.NewGuid() : convertToGuid(value);
                            }
                            break;

                        default:
                            // Unknown type...!
                            throw new FileDbException(string.Format(FileDbException.StrInvalidDataType2, field.Name, field.DataType.ToString(), value.GetType().Name),
                                FileDbExceptionsEnum.InvalidDataType);
                    }
                }
                catch//( Exception ex )
                {
                    throw new FileDbException(string.Format(FileDbException.ErrorConvertingValueForField, field.Name, value != null ? value.ToString() : "null"),
                        FileDbExceptionsEnum.ErrorConvertingValueForField);
                }
            }
        }

        /*
        /// <summary>
        /// Function to return the index values.  We assume the
        /// database has been locked before calling this function;
        /// </summary>
        /// <returns></returns>
        Int32[] readIndex()
        {
            // I think ToArray may be efficient, just returning its internal array
            return readIndex2().ToArray();
            #if false
            _dbStream.Seek( _indexStartPos, SeekOrigin.Begin );

            // Read in the index
            Int32[] vIndex = new Int32[_numRecords];

            // brettg: must wrap with try/catch because there may be one less indices than _numRecords
            // e.g. when called from addRecord
            try
            {
                for( Int32 i = 0; i < _numRecords; i++ )
                    vIndex[i] = _dataReader.ReadInt32();
            }
            catch { }

            _deletedRecords = new List<Int32>( _numDeleted );
            if( _numDeleted > 0 )
            {
                try
                {
                    for( Int32 i = 0; i < _numDeleted; i++ )
                        _deletedRecords.Add( _dataReader.ReadInt32() );
                }
                catch { }
            }

            return vIndex;
            #endif
        }*/

        /// <summary>
        /// Use this version if you will need to add/remove indices
        /// </summary>
        ///
        List<Int32> readIndex()
        {
            _dbStream.Seek(_indexStartPos, SeekOrigin.Begin);

            // Read in the index
            var vIndex = new List<Int32>(_numRecords);

            // brettg: must wrap with try/catch because there may be one less indices than _numRecords
            // e.g. when called from addRecord
            try
            {
                for (Int32 i = 0; i < _numRecords; i++)
                    vIndex.Add(_dataReader.ReadInt32());
            }
            catch //( Exception ex )
            {
                Debug.Assert(false);
            }

            _deletedRecords = new List<Int32>(_numDeleted);
            if (_numDeleted > 0)
            {
                try
                {
                    for (Int32 i = 0; i < _numDeleted; i++)
                        _deletedRecords.Add(_dataReader.ReadInt32());
                }
                catch
                {
                    // TODO: ???
                }
            }

            readUserData(_dataReader);

            return vIndex;
        }

        /*
        /// <summary>
        /// function to write the index values.  We assume the
        /// database has been locked before calling this function.
        /// </summary>
        void writeIndex( Stream fileStrm, BinaryWriter writer, Int32[] index )
        {
            fileStrm.Seek( _indexStartPos, SeekOrigin.Begin );

            // bg NOTE: we use the min of _numRecords and the index.Length
            // because with some callers the index has an extra index at the end 
            // to effectively truncate the index

            for( Int32 i = 0; i < Math.Min( _numRecords, index.Length ); i++ )
                writer.Write( index[i] );

            Debug.Assert( _numDeleted == _deletedRecords.Count );
            
            for( Int32 i = 0; i < _deletedRecords.Count; i++ )
                writer.Write( _deletedRecords[i] );

            // we'll require that readIndex be called after this so its not hanging around in memory
            _deletedRecords = null;

            writer.Flush();

            fileStrm.SetLength( fileStrm.Position );
        }*/

        /// <summary>
        /// function to write the index values.  We assume the
        /// database has been locked before calling this function.
        /// </summary>
        /// 
        void writeIndex(Stream fileStrm, BinaryWriter writer, List<Int32> index)
        {
            fileStrm.Seek(_indexStartPos, SeekOrigin.Begin);

            // bg NOTE: we use the min of _numRecords and the index.Length
            // because with some callers the index has an extra index at the end 
            // to effectively truncate the index

            for (Int32 i = 0; i < Math.Min(_numRecords, index.Count); i++)
                writer.Write(index[i]);

            Debug.Assert(_numDeleted == _deletedRecords.Count);

            for (Int32 i = 0; i < Math.Min(_numDeleted, _deletedRecords.Count); i++)
                writer.Write(_deletedRecords[i]);

            // whenever we write the index we must write the UserData since it goes after the index
            writeUserData(writer);

            fileStrm.SetLength(fileStrm.Position);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataWriter"></param>
        /// <param name="record"></param>
        /// <param name="size"></param>
        /// <param name="nullmask">to keep track of null fields in written record</param>
        /// <param name="deleted"></param>
        /// 
        void writeRecord(BinaryWriter dataWriter, FieldValues record, Int32 size, byte[] nullmask, bool deleted)
        {
            // Auto-calculate the record size
            if (size < 0 && _encryptor == null)
                size = getRecordSize(record, out nullmask);

            // gotta set the nullmask first
            int ncol = 0;
            foreach (Field field in _fields)
            {
                object data = null;

                if (record.ContainsKey(field.Name))
                    data = record[field.Name];

                if (data == null)
                    setNullMask(nullmask, ncol);
                ncol++;
            }

#if DEBUG
            int startPos, proposedSize = size;
#endif
            MemoryStream memStrm = null;
            BinaryWriter origDataWriter = dataWriter;

            if (_encryptor != null)
            {
                memStrm = new MemoryStream(size + nullmask.Length + 100);
                dataWriter = new BinaryWriter(memStrm);
#if DEBUG
                startPos = (int) memStrm.Position;
#endif
            }
            else
            {
                if (deleted)
                    size = -size; // deleted indicator

                // Write out the size of the record
                dataWriter.Write(size);

#if DEBUG
                startPos = (int) dataWriter.BaseStream.Position; // _dbStream.Position;
#endif
            }
            dataWriter.Write(nullmask);

            // Write out the entire record in field order
            ncol = 0;
            foreach (Field field in _fields)
            {
                // all fieldnames in the record should now be upper case
                object data = null;

                if (record.ContainsKey(field.Name))
                    data = record[field.Name];

                if (data != null)
                    writeItem(dataWriter, field, data);
                ncol++;
            }

            if (_encryptor != null)
            {
                memStrm.Seek(0, SeekOrigin.Begin);

                byte[] bytes = _encryptor.Encrypt(memStrm.ToArray());
                size = bytes.Length;

                if (deleted)
                    size = -size;

                origDataWriter.Write(size);
                origDataWriter.Write(bytes);
            }

#if DEBUG
            int endPos;
            if (_encryptor != null)
                endPos = (int) memStrm.Length;
            else
                endPos = (int) dataWriter.BaseStream.Position;
            int actualSize = endPos - startPos;
            Debug.Assert(actualSize == proposedSize);
#endif
        }

        /*enum BitFlags : byte
        {
            P0 = 1,
            P1 = 2,
            P2 = 4,
            P3 = 8,
            P4 = 0x10,
            P5 = 0x20,
            P6 = 0x40,
            P7 = 0x80
        }*/

        static byte[] s_bitmask = new byte[] { 1, 2, 4, 8, 0x10, 0x20, 0x40, 0x80 };

        void setNullMask(byte[] nullmask, int pos)
        {
            int bytePos = pos / 8;
            pos = pos % 8;
            byte b = nullmask[bytePos];
            b |= s_bitmask[pos];
            nullmask[bytePos] = b;
        }

        /*
        /// <summary>
        /// Use this version only if all of the fields are present and in the correct order in the array
        /// </summary>
        /// <param name="dataWriter"></param>
        /// <param name="record"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        void writeRecord( BinaryWriter dataWriter, object[] record, Int32 size, bool deleted )
        {
            // Auto-calculate the record size
            if( size < 0 )
                size = getRecordSize( record );

            MemoryStream memStrm = null;
            BinaryWriter origDataWriter = dataWriter;

            if( _encryptor != null )
            {
                memStrm = new MemoryStream( 200 );
                dataWriter = new BinaryWriter( memStrm );
            }
            else
            {
                if( deleted )
                    size = -size;
                // Write out the size of the record
                dataWriter.Write( size );
            }

            // Write out the entire record
            foreach( Field field in _fields )
            {
                object data = record[field.Ordinal];
                writeItem( dataWriter, field, data );
            }

            if( AutoFlush ) flush();

            if( _encryptor != null )
            {
                memStrm.Seek( 0, SeekOrigin.Begin );
                //using( var outStrm = new MemoryStream( (int) memStrm.Length * 2 ) )
                {
                    byte[] bytes = _encryptor.Encrypt( memStrm.ToArray() );
                    //byte[] bytes = outStrm.ToArray();
                    size = bytes.Length;
                    if( deleted )
                        size = -size;
                    // Write out the size of the record
                    origDataWriter.Write( size );
                    origDataWriter.Write( bytes );
                }
            }
        }*/

        void writeRecordRaw(BinaryWriter dataWriter, byte[] record, bool deleted)
        {
            BinaryWriter origDataWriter = dataWriter;

            int size = record.Length;

            if (deleted)
                size = -size;

            dataWriter.Write(size);
            dataWriter.Write(record);
        }

        Int32 getRecordSize(FieldValues record, out byte[] nullmask)
        {
            Int32 size = 0;

            int nBytes = _fields.Count / 8;
            if ((_fields.Count % 8) > 0)
                nBytes++;
            nullmask = new byte[nBytes];
            size += nBytes;

            // Size up each field

            foreach (Field field in _fields)
            {
                object data = null;
                if (record.ContainsKey(field.Name))
                    data = record[field.Name];

                if (data != null)
                    size += getItemSize(field, data);
            }
            return size;
        }

        /*
        /// <summary>
        /// Use this version only if all of the fields are present and in the correct order in the array
        /// </summary>
        /// <param name="record"></param>
        /// <returns></returns>
        Int32 getRecordSize( object[] record )
        {
            Int32 size = 0;

            // Size up each field

            foreach( Field field in _fields )
            {
                object data = record[field.Ordinal];
                size += getItemSize( field, data );
            }
            return size;
        }*/

        Int32 getItemSize(Field field, object data)
        {
            Int32 size = 0;

            if (data == null)
                return size;

            switch (field.DataType)
            {
                case DataTypeEnum.Byte:
                    if (field.IsArray)
                    {
                        size = sizeof(Int32);
                        Byte[] arr = (Byte[]) data;
                        if (arr != null)
                            size += arr.Length;
                    }
                    else
                        size = 1;
                    break;

                case DataTypeEnum.Int32:
                    if (field.IsArray)
                    {
                        size = sizeof(Int32);
                        Int32[] arr = (Int32[]) data;
                        if (arr != null)
                            size += arr.Length * sizeof(Int32);
                    }
                    else
                        size = sizeof(Int32);
                    break;

                case DataTypeEnum.UInt32:
                    if (field.IsArray)
                    {
                        size = sizeof(UInt32);
                        UInt32[] arr = (UInt32[]) data;
                        if (arr != null)
                            size += arr.Length * sizeof(UInt32);
                    }
                    else
                        size = sizeof(UInt32);
                    break;

                case DataTypeEnum.Float:
                    if (field.IsArray)
                    {
                        size = sizeof(Int32);
                        float[] arr = (float[]) data;
                        size += arr.Length * sizeof(float);

#if DEBUG
                        _testWriter = getTestWriter();
                        foreach (float d in arr)
                            _testWriter.Write(d);
                        _testWriter.Flush();
                        int testSize = (Int32) _testStrm.Position;
                        Debug.Assert(testSize == size - sizeof(Int32));
#endif
                    }
                    else
                        size = sizeof(float);
                    break;

                case DataTypeEnum.Double:
                    if (field.IsArray)
                    {
                        size = sizeof(Int32);
                        double[] arr = (double[]) data;
                        size += arr.Length * sizeof(double);

#if DEBUG
                        _testWriter = getTestWriter();
                        foreach (double d in arr)
                            _testWriter.Write(d);
                        _testWriter.Flush();
                        int testSize = (Int32) _testStrm.Position;
                        Debug.Assert(testSize == size - sizeof(Int32));
#endif
                    }
                    else
                        size = sizeof(double);
                    break;

                case DataTypeEnum.Bool:
                    if (field.IsArray)
                    {
                        size = sizeof(Int32);
                        if (data.GetType() == typeof(bool[]))
                        {
                            bool[] arr = (bool[]) data;
                            size += arr.Length;
                        }
                        else if (data.GetType() == typeof(Byte[]))
                        {
                            Byte[] arr = (Byte[]) data;
                            size += arr.Length;
                        }
                    }
                    else
                        size = 1;
                    break;

                case DataTypeEnum.DateTime:
                    {
                        // DateTimes were stored as string prior to 2.2
#if false
                    _testWriter = getTestWriter();

                    if( field.IsArray )
                    {
                        size = sizeof( Int32 );

                        if( data != null )
                        {
                            if( data.GetType() == typeof( DateTime[] ) )
                            {
                                DateTime[] arr = (DateTime[]) data;
                                if( arr != null )
                                {
                                    foreach( DateTime dt in arr )
                                        _testWriter.Write( dt.ToString( DateTimeFmt ) );
                                }
                            }
                            else // must be string
                            {
                                string[] arr = (string[]) data;
                                if( arr != null )
                                {
                                    foreach( string s in arr )
                                    {
                                        DateTime dt = DateTime.Parse( s );
                                        _testWriter.Write( dt.ToString( DateTimeFmt ) );
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        if( data == null )
                            _testWriter.Write( string.Empty );
                        else
                        {
                            DateTime dt;
                            if( data.GetType() == typeof( DateTime ) )
                            {
                                dt = (DateTime) data;
                            }
                            else // must be string
                            {
                                dt = Convert.ToDateTime( data );
                            }
                            _testWriter.Write( dt.ToString( DateTimeFmt ) );
                        }
                    }

                    _testWriter.Flush();
                    size += (Int32) _testStrm.Position;
#endif

                        if (field.IsArray)
                        {
                            size = sizeof(Int32);
                            if (data.GetType() == typeof(DateTime[]))
                            {
                                DateTime[] arr = (DateTime[]) data;
                                size += arr.Length * DateTimeByteLen;
                            }
                            else // must be string
                            {
                                string[] arr = (string[]) data;
                                size += arr.Length * DateTimeByteLen;
                            }
                        }
                        else
                        {
                            size += DateTimeByteLen;
                        }
                    }
                    break;

                case DataTypeEnum.String:
                    {
                        _testWriter = getTestWriter();

                        if (field.IsArray)
                        {
                            size = sizeof(Int32);
                            string[] arr = (string[]) data;
                            foreach (string s in arr)
                                _testWriter.Write(s == null ? string.Empty : s); // can't write null strings
                        }
                        else
                        {
                            _testWriter.Write(data.ToString());
                        }

                        if (_testStrm.Position > 0)
                        {
                            _testWriter.Flush(); // is this necessary?
                            size += (Int32) _testStrm.Position;
                        }
                    }
                    break;

                case DataTypeEnum.Int64:
                    if (field.IsArray)
                    {
                        size = sizeof(Int32);
                        Int64[] arr = (Int64[]) data;
                        size += arr.Length * sizeof(Int64);

#if DEBUG
                        _testWriter = getTestWriter();
                        foreach (Int64 i in arr)
                            _testWriter.Write(i);
                        _testWriter.Flush(); // is this necessary?
                        int testSize = (Int32) _testStrm.Position;
                        Debug.Assert(testSize == size - sizeof(Int32));
#endif
                    }
                    else
                        size = sizeof(Int64);
                    break;

                case DataTypeEnum.Decimal:
                    if (field.IsArray)
                    {
                        size = sizeof(Int32);
                        Decimal[] arr = (Decimal[]) data;
                        size += arr.Length * sizeof(Decimal);

#if DEBUG
                        _testWriter = getTestWriter();
                        foreach (Decimal d in arr)
                            writeDecimal(_testWriter, d);
                        _testWriter.Flush(); // is this necessary?
                        int testSize = (Int32) _testStrm.Position;
                        Debug.Assert(testSize == size - sizeof(Int32));
#endif
                    }
                    else
                        size = sizeof(Decimal);
                    break;

                case DataTypeEnum.Guid:
                    // Guids are stored as byte[]
                    _testWriter = getTestWriter();

                    if (field.IsArray)
                    {
                        size = sizeof(Int32);

                        if (data.GetType() == typeof(Guid[]))
                        {
                            Guid[] arr = (Guid[]) data;
                            if (arr != null)
                            {
                                foreach (Guid g in arr)
                                    _testWriter.Write(g.ToByteArray());
                            }
                        }
                        /*else // must be string
                        {
                            string[] arr = (string[]) data;
                            if( arr != null )
                            {
                                foreach( string s in arr )
                                {
                                    DateTime dt = DateTime.Parse( s );
                                    _testWriter.Write( dt.ToString( DateTimeFmt ) );
                                }
                            }
                        }*/
                    }
                    else
                    {
                        Guid guid = (Guid) data;
                        _testWriter.Write(guid.ToByteArray());
                    }
                    _testWriter.Flush(); // is this necessary?
                    size += (Int32) _testStrm.Position;
                    break;

                default:
                    throw new FileDbException(string.Format(FileDbException.StrInvalidDataType, (Int32) field.DataType), FileDbExceptionsEnum.InvalidDataType);

            }

            return size;
        }

        BinaryWriter getTestWriter()
        {
            if (_testStrm == null)
            {
                _testStrm = new MemoryStream();
                _testWriter = new BinaryWriter(_testStrm);
            }
            else
                _testStrm.Seek(0, SeekOrigin.Begin);

            return _testWriter;
        }

        /// <summary>
        /// Write a single field to the file
        /// </summary>
        /// <param name="dataWriter"></param>
        /// <param name="field"></param>
        /// <param name="data"></param>
        /// 
        void writeItem(BinaryWriter dataWriter, Field field, object data)
        {
            if (data == null)
                return;

            switch (field.DataType)
            {
                case DataTypeEnum.Byte:
                    if (field.IsArray)
                    {
                        Byte[] arr = (Byte[]) data;
                        dataWriter.Write(arr.Length);

                        foreach (Byte b in arr)
                            dataWriter.Write(b);
                    }
                    else
                    {
                        Byte val;
                        if (data.GetType() != typeof(Byte))
                            val = Convert.ToByte(data);
                        else
                            val = (Byte) data;
                        dataWriter.Write(val);
                    }
                    break;

                case DataTypeEnum.Int32:
                    if (field.IsArray)
                    {
                        Int32[] arr = (Int32[]) data;
                        dataWriter.Write(arr.Length);

                        foreach (Int32 i in arr)
                            dataWriter.Write(i);
                    }
                    else
                    {
                        Int32 val;
                        if (data.GetType() != typeof(Int32))
                            val = Convert.ToInt32(data);
                        else
                            val = (Int32) data;
                        dataWriter.Write(val);
                    }
                    break;

                case DataTypeEnum.UInt32:
                    if (field.IsArray)
                    {
                        UInt32[] arr = (UInt32[]) data;
                        dataWriter.Write(arr.Length);

                        foreach (UInt32 i in arr)
                            dataWriter.Write(i);
                    }
                    else
                    {
                        UInt32 val;
                        if (data.GetType() != typeof(UInt32))
                            val = Convert.ToUInt32(data);
                        else
                            val = (UInt32) data;
                        dataWriter.Write(val);
                    }
                    break;

                case DataTypeEnum.Float:
                    if (field.IsArray)
                    {
                        float[] arr = (float[]) data;
                        dataWriter.Write(arr.Length);

                        foreach (float d in arr)
                            dataWriter.Write(d);
                    }
                    else
                    {
                        float val;
                        if (data.GetType() != typeof(float))
                            val = Convert.ToSingle(data);
                        else
                            val = (float) data;
                        dataWriter.Write(val);
                    }
                    break;

                case DataTypeEnum.Double:
                    if (field.IsArray)
                    {
                        double[] arr = (double[]) data;
                        dataWriter.Write(arr.Length);

                        foreach (double d in arr)
                            dataWriter.Write(d);
                    }
                    else
                    {
                        double val;
                        if (data.GetType() != typeof(double))
                            val = Convert.ToDouble(data);
                        else
                            val = (double) data;
                        dataWriter.Write(val);
                    }
                    break;

                case DataTypeEnum.Bool:
                    {
                        if (field.IsArray)
                        {
                            if (data.GetType() == typeof(bool[]))
                            {
                                bool[] arr = (bool[]) data;
                                dataWriter.Write(arr.Length);

                                foreach (bool b in arr)
                                    dataWriter.Write(b ? (Byte) 1 : (Byte) 0);
                            }
                            else if (data.GetType() == typeof(Byte[]))
                            {
                                Byte[] arr = (Byte[]) data;
                                dataWriter.Write(arr.Length);

                                foreach (Byte b in arr)
                                    dataWriter.Write(b);
                            }
                            else
                                throw new FileDbException(FileDbException.InValidBoolType, FileDbExceptionsEnum.InvalidDataType);
                        }
                        else
                        {
                            bool val = false;
                            if (data is bool)
                            {
                                val = (bool) data;
                            }
                            else if (data is string)
                            {
                                var s = (string) data;
                                if (string.Compare(s, "false", StringComparison.OrdinalIgnoreCase) == 0)
                                    val = false;
                                else if (string.Compare(s, "true", StringComparison.OrdinalIgnoreCase) == 0)
                                    val = true;
                            }
                            else
                            {
                                try
                                {
                                    Int32 i = Convert.ToInt32(data);
                                    val = i != 0;
                                }
                                catch //( Exception )
                                {
                                    throw new FileDbException(FileDbException.InValidBoolType, FileDbExceptionsEnum.InvalidDataType);
                                }
                            }

                            dataWriter.Write(val ? (Byte) 1 : (Byte) 0);
                        }
                    }
                    break;

                case DataTypeEnum.DateTime:
                    {
                        if (field.IsArray)
                        {
                            if (data.GetType() == typeof(DateTime[]))
                            {
                                DateTime[] arr = (DateTime[]) data;
                                dataWriter.Write(arr.Length);

                                foreach (DateTime dt in arr)
                                    writeDate(dt, dataWriter);
                            }
                            else if (data.GetType() == typeof(string[]))
                            {
                                string[] arr = (string[]) data;
                                dataWriter.Write(arr.Length);

                                // convert each string to DateTime then write it in our format
                                foreach (string s in arr)
                                {
                                    DateTime dt = DateTime.Parse(s);
                                    writeDate(dt, dataWriter);
                                }
                            }
                            else
                                throw new FileDbException(FileDbException.InvalidDateTimeType, FileDbExceptionsEnum.InvalidDataType);
                        }
                        else
                        {
                            if (data.GetType() == typeof(DateTime))
                            {
                                DateTime dt = (DateTime) data;
                                writeDate(dt, dataWriter);
                            }
                            else if (data.GetType() == typeof(String))
                            {
                                DateTime dt = DateTime.Parse(data.ToString());
                                writeDate(dt, dataWriter);
                            }
                            else
                                throw new FileDbException(FileDbException.InvalidDateTimeType, FileDbExceptionsEnum.InvalidDataType);
                        }
                    }
                    break;

                case DataTypeEnum.String:
                    if (field.IsArray)
                    {
                        string[] arr = (string[]) data;
                        writeStringArray(dataWriter, arr);
                    }
                    else
                    {
                        dataWriter.Write(data.ToString());
                    }
                    break;

                case DataTypeEnum.Int64:
                    if (field.IsArray)
                    {
                        Int64[] arr = (Int64[]) data;
                        dataWriter.Write(arr.Length);

                        foreach (Int64 i in arr)
                            dataWriter.Write(i);
                    }
                    else
                    {
                        Int64 val;
                        if (data.GetType() != typeof(Int64))
                            val = Convert.ToInt64(data);
                        else
                            val = (Int64) data;
                        dataWriter.Write(val);
                    }
                    break;

                case DataTypeEnum.Decimal:
                    if (field.IsArray)
                    {
                        Decimal[] arr = (Decimal[]) data;
                        dataWriter.Write(arr.Length);

                        foreach (Decimal d in arr)
                            writeDecimal(dataWriter, d);
                    }
                    else
                    {
                        Decimal d;
                        if (data.GetType() != typeof(Decimal))
                            d = Convert.ToDecimal(data);
                        else
                            d = (Decimal) data;
                        writeDecimal(dataWriter, d);
                    }
                    break;

                case DataTypeEnum.Guid:
                    if (field.IsArray)
                    {
                        if (data.GetType() == typeof(Guid[]))
                        {
                            Guid[] arr = (Guid[]) data;
                            dataWriter.Write(arr.Length);

                            foreach (Guid g in arr)
                                dataWriter.Write(g.ToByteArray());
                        }
                        else if (data.GetType() == typeof(string[]))
                        {
                            string[] arr = (string[]) data;
                            dataWriter.Write(arr.Length);

                            // convert each string to Guid
                            foreach (string s in arr)
                            {
                                Guid g = new Guid(s);
                                dataWriter.Write(g.ToByteArray());
                            }
                        }
                        else
                            throw new FileDbException(FileDbException.InvalidDateTimeType, FileDbExceptionsEnum.InvalidDataType);
                    }
                    else
                    {
                        if (data.GetType() == typeof(Guid))
                        {
                            Guid g = (Guid) data;
                            dataWriter.Write(g.ToByteArray());
                        }
                        else if (data.GetType() == typeof(String))
                        {
                            Guid g = new Guid(data.ToString());
                            dataWriter.Write(g.ToByteArray());
                        }
                        else
                            throw new FileDbException(FileDbException.InvalidDateTimeType, FileDbExceptionsEnum.InvalidDataType);
                    }
                    break;

                default:
                    // Unknown type
                    throw new FileDbException(string.Format(FileDbException.StrInvalidDataType2,
                        field.Name, field.DataType.ToString(), data.GetType().Name), FileDbExceptionsEnum.InvalidDataType);
            }
        }

        void writeDecimal(BinaryWriter writer, Decimal dec)
        {
            Int32[] arr = decimal.GetBits(dec);
            writer.Write(arr[0]);
            writer.Write(arr[1]);
            writer.Write(arr[2]);
            writer.Write(arr[3]);
        }

        Decimal readDecimal(BinaryReader reader)
        {
            Int32[] arr = new Int32[4];
            arr[0] = reader.ReadInt32();
            arr[1] = reader.ReadInt32();
            arr[2] = reader.ReadInt32();
            arr[3] = reader.ReadInt32();

            return new decimal(arr);
        }

        void writeStringArray(BinaryWriter dataWriter, string[] arr)
        {
            // write the length
            dataWriter.Write(arr.Length);

            foreach (string s in arr)
            {
                dataWriter.Write(s == null ? String.Empty : s);
            }
        }

        void writeDate(DateTime dt, BinaryWriter writer)
        {
            writer.Write((Int16) dt.Year);
            writer.Write((Byte) dt.Month);
            writer.Write((Byte) dt.Day);
            writer.Write((Byte) dt.Hour);
            writer.Write((Byte) dt.Minute);
            writer.Write((Byte) dt.Second);
            writer.Write((UInt16) dt.Millisecond);
            writer.Write((Byte) dt.Kind);

#if false
            Int16 year;
            Byte month, day, hour, minute, second;
            UInt16 milliseconds;

            year = (Int16) dt.Year;
            month = (Byte) dt.Month;
            day = (Byte) dt.Day;
            hour = (Byte) dt.Hour;
            minute = (Byte) dt.Minute;
            second = (Byte) dt.Second;
            milliseconds = (UInt16) dt.Millisecond;
            MemoryStream memstrm = new MemoryStream();
            BinaryWriter writer = new BinaryWriter( memstrm );

            memstrm.Seek( 0, SeekOrigin.Begin );
            BinaryReader reader = new BinaryReader( memstrm );

            year = reader.ReadInt16();
            month = reader.ReadByte();
            day = reader.ReadByte();
            hour = reader.ReadByte();
            minute = reader.ReadByte();
            second = reader.ReadByte();
            milliseconds = reader.ReadUInt16();
            dt = new DateTime( year, month, day, hour, minute, second, milliseconds );
#endif
        }

        DateTime readDate(BinaryReader reader)
        {
            Int16 year;
            Byte month, day, hour, minute, second;
            UInt16 milliseconds;
            DateTimeKind kind;

            year = reader.ReadInt16();
            month = reader.ReadByte();
            day = reader.ReadByte();
            hour = reader.ReadByte();
            minute = reader.ReadByte();
            second = reader.ReadByte();
            milliseconds = reader.ReadUInt16();
            kind = (DateTimeKind) reader.ReadByte();

            DateTime dt = new DateTime(year, month, day, hour, minute, second, milliseconds, kind);
            return dt;
        }

        ///------------------------------------------------------------------------------
        /// <summary>
        /// Private function to perform a binary search
        /// </summary>
        /// <param name="lstIndex">file offsets into the .dat file, it must be ordered 
        /// by primary key.</param>
        /// <param name="left">the left most index to start searching from</param>
        /// <param name="right">the right most index to start searching from</param>
        /// <param name="target">the search target we're looking for</param>
        /// <returns>-[insert pos+1] when not found, or the array index+1 
        /// when found. Note that we don't return the normal position, because we 
        /// can't differentiate between -0 and +0.</returns>
        /// 
        Int32 bsearch(List<Int32> lstIndex, Int32 left, Int32 right, object target)
        {
            Int32 middle = 0; // todo: What should this default to?

            while (left <= right)
            {
                middle = (Int32) ((left + right) / 2);

                // Read in the record key at the given offset
                object key = readRecordKey(_dataReader, lstIndex[middle]);

                Int32 nComp = compareKeys(target, key);

                if (left == right && nComp != 0) //( key != target ) )
                {
                    if (nComp < 0)
                        return -(left + 1);
                    else
                        return -(left + 1 + 1);
                }
                else if (nComp == 0)
                {
                    // Found!
                    return middle + 1;
                }
                else if (nComp < 0)
                {
                    // Try the left side
                    right = middle - 1;
                }
                else // target > key
                {
                    // Try the right side
                    left = middle + 1;
                }
            }

            // Not found: return the insert position (as negative)
            return -(middle + 1);
        }

        /// <summary>
        /// Helper
        /// </summary>
        /// <returns></returns>
        ///
        Int32 compareKeys(object left, object right)
        {
            Type tleft = left.GetType(),
                 tright = right.GetType();

            if (tleft != tright)
                throw new FileDbException(FileDbException.MismatchedKeyFieldTypes, FileDbExceptionsEnum.MismatchedKeyFieldTypes);

            if (tleft == typeof(string))
                return string.Compare(left as string, right as string);

            else if (tleft == typeof(Int32))
            {
                Int32 nleft = (Int32) left,
                    nright = (Int32) right;
                return nleft < nright ? -1 : (nleft > nright ? 1 : 0); // todo: check if this is correct
            }
            else
                throw new FileDbException(FileDbException.InvalidKeyFieldType, FileDbExceptionsEnum.InvalidKeyFieldType);
        }

        /// <summary>
        /// Private function to read a record from the database
        /// </summary>
        ///
        object[] readRecord(Int32 offset, bool includeIndex)
        {
            Int32 size;
            bool deleted;
            return readRecord(_dataReader, offset, includeIndex, out size, out deleted);
        }

        object[] readRecord(Int32 offset, bool includeIndex, out bool deleted)
        {
            Int32 size;
            return readRecord(_dataReader, offset, includeIndex, out size, out deleted);
        }

        /// <summary>
        /// size does not include the 4 byte record length, but only the total size of the fields
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="includeIndex"></param>
        /// <param name="size"></param>
        /// <param name="deleted"></param>
        /// <returns></returns>
        /// 
        object[] readRecord(Int32 offset, bool includeIndex, out Int32 size, out bool deleted)
        {
            return readRecord(_dataReader, offset, includeIndex, out size, out deleted);
        }

        object[] readRecord(BinaryReader dataReader, Int32 offset, bool includeIndex, out Int32 size, out bool deleted)
        {
            // Read in the record at the given offset.
            _dbStream.Seek(offset, SeekOrigin.Begin);

            // Read in the size of the block allocated for the record
            size = dataReader.ReadInt32();

            if (size < 0)
            {
                deleted = true;
                size = -size;
            }
            else
                deleted = false;

            int numFields = _fields.Count;
            if (includeIndex)
                numFields++;

            object[] record = new object[numFields]; // one extra for the index

            Debug.Assert(size > 0);

            MemoryStream memStrm = null;

            if (_encryptor != null)
            {
                byte[] bytes = dataReader.ReadBytes(size);
                bytes = _encryptor.Decrypt(bytes);
                memStrm = new MemoryStream(bytes);
                dataReader = new BinaryReader(memStrm);
            }

            byte[] nullmask = null;
            if (_ver >= VerNullValueSupport)
                nullmask = readNullmask(dataReader);

            // Read in the entire record
            for (Int32 n = 0; n < _fields.Count; n++)
            {
                if (nullmask != null)
                {
                    // get the correct byte
                    int pos = n / 8;
                    byte b = nullmask[pos];
                    // get the bit pos in the byte
                    pos = n % 8;
                    if ((b & s_bitmask[pos]) != 0)
                        continue;
                }
                Field field = _fields[n];
                record[n] = readItem(dataReader, field);
            }

            return record;
        }

        byte[] readRecordRaw(BinaryReader dataReader, Int32 offset, out bool deleted)
        {
            // Read in the record at the given offset.
            _dbStream.Seek(offset, SeekOrigin.Begin);

            // Read in the size of the block allocated for the record
            int size = dataReader.ReadInt32();

            if (size < 0)
            {
                deleted = true;
                size = -size;
            }
            else
                deleted = false;

            byte[] bytes = null;

            if (size > 0)
                bytes = dataReader.ReadBytes(size);

            return bytes;
        }

        /// <summary>
        /// function to read a record KEY from the database.  Note
        /// that this function relies on the fact that they key is ALWAYS the first
        /// item in the database record as stored on disk.
        /// </summary>
        /// <param name="dataReader"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        /// 
        object readRecordKey(BinaryReader dataReader, Int32 offset)
        {
            // Read in the record KEY only

            byte[] nullmask = null;

            if (_encryptor == null)
            {
                _dbStream.Seek(offset + INDEX_RBLOCK_SIZE, SeekOrigin.Begin);
            }
            else // must first decrypt the record
            {
                // Read in the size of the block allocated for the record
                _dbStream.Seek(offset, SeekOrigin.Begin);
                int size = dataReader.ReadInt32();

                if (size < 0) // deleted record
                    size = -size;

                MemoryStream memStrm = null;

                byte[] bytes = dataReader.ReadBytes(size);
                bytes = _encryptor.Decrypt(bytes);
                memStrm = new MemoryStream(bytes);
                dataReader = new BinaryReader(memStrm);
            }

            if (_ver >= VerNullValueSupport)
            {
                // read past the nullmask
                nullmask = readNullmask(dataReader);
            }

            // NOTE: Key field must not be null or empty

            return readItem(dataReader, _primaryKeyField);
        }

        byte[] readNullmask(BinaryReader dataReader)
        {
            // read the nullmask
            int nBytes = _fields.Count / 8;
            if ((_fields.Count % 8) > 0)
                nBytes++;
            return dataReader.ReadBytes(nBytes);
        }

        /// <summary>
        /// Reads a data type from a file.  Note that arrays can only 
        /// consist of other arrays, ints, and strings.
        /// </summary>
        /// <param name="dataReader"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        /// 
        object readItem(BinaryReader dataReader, Field field)
        {
            switch (field.DataType)
            {
                case DataTypeEnum.Byte:
                    if (field.IsArray)
                    {
                        Int32 elements = dataReader.ReadInt32();
                        Byte[] arr = null;
                        if (elements >= 0)
                        {
                            arr = dataReader.ReadBytes(elements);
                        }
                        return arr;
                    }
                    else
                        return dataReader.ReadByte();

                case DataTypeEnum.Int32:
                    if (field.IsArray)
                    {
                        Int32 elements = dataReader.ReadInt32();
                        Int32[] arr = null;
                        if (elements >= 0)
                        {
                            arr = new Int32[elements];
                            for (Int32 i = 0; i < elements; i++)
                                arr[i] = dataReader.ReadInt32();
                        }
                        return arr;
                    }
                    else
                        return dataReader.ReadInt32();

                case DataTypeEnum.UInt32:
                    if (field.IsArray)
                    {
                        Int32 elements = dataReader.ReadInt32();
                        UInt32[] arr = null;
                        if (elements >= 0)
                        {
                            arr = new UInt32[elements];
                            for (UInt32 i = 0; i < elements; i++)
                                arr[i] = dataReader.ReadUInt32();
                        }
                        return arr;
                    }
                    else
                        return dataReader.ReadUInt32();

                case DataTypeEnum.Float:
                    if (field.IsArray)
                    {
                        Int32 elements = dataReader.ReadInt32();
                        float[] arr = null;
                        if (elements >= 0)
                        {
                            arr = new float[elements];
                            for (Int32 i = 0; i < elements; i++)
                                arr[i] = dataReader.ReadSingle();
                        }
                        return arr;
                    }
                    else
                        return dataReader.ReadSingle();

                case DataTypeEnum.Double:
                    if (field.IsArray)
                    {
                        Int32 elements = dataReader.ReadInt32();
                        double[] arr = null;
                        if (elements >= 0)
                        {
                            arr = new double[elements];
                            for (Int32 i = 0; i < elements; i++)
                                arr[i] = dataReader.ReadDouble();
                        }
                        return arr;
                    }
                    else
                        return dataReader.ReadDouble();

                case DataTypeEnum.Bool:
                    if (field.IsArray)
                    {
                        Int32 elements = dataReader.ReadInt32();
                        bool[] arr = null;
                        if (elements >= 0)
                        {
                            arr = new bool[elements];
                            for (Int32 i = 0; i < elements; i++)
                                arr[i] = dataReader.ReadByte() == 1;
                        }
                        return arr;
                    }
                    else
                        return (dataReader.ReadByte() == 1);

                case DataTypeEnum.DateTime:
                    if (field.IsArray)
                    {
                        Int32 elements = dataReader.ReadInt32();
                        DateTime[] arr = null;
                        if (elements >= 0)
                        {
                            arr = new DateTime[elements];
                            for (Int32 i = 0; i < elements; i++)
                            {
                                if (_ver < VerNullValueSupport)
                                {
                                    string s = dataReader.ReadString();
                                    arr[i] = DateTime.Parse(s);
                                }
                                else
                                {
                                    arr[i] = readDate(dataReader);
                                }
                            }
                        }
                        return arr;
                    }
                    else
                    {
                        if (_ver < VerNullValueSupport)
                        {
                            string s = dataReader.ReadString();
                            if (s.Length == 0)
                                return DateTime.MinValue;
                            else
                                return DateTime.Parse(s);
                        }
                        else
                            return readDate(dataReader);
                    }

                case DataTypeEnum.String:
                    if (field.IsArray)
                    {
                        Int32 elements = dataReader.ReadInt32();
                        string[] arr = null;
                        if (elements >= 0)
                        {
                            arr = new string[elements];
                            for (Int32 i = 0; i < elements; i++)
                                arr[i] = dataReader.ReadString();
                        }
                        return arr;
                    }
                    else
                        return dataReader.ReadString();

                case DataTypeEnum.Int64:
                    if (field.IsArray)
                    {
                        Int32 elements = dataReader.ReadInt32();
                        Int64[] arr = null;
                        if (elements >= 0)
                        {
                            arr = new Int64[elements];
                            for (Int32 i = 0; i < elements; i++)
                                arr[i] = dataReader.ReadInt64();
                        }
                        return arr;
                    }
                    else
                        return dataReader.ReadInt64();

                case DataTypeEnum.Decimal:
                    if (field.IsArray)
                    {
                        Int32 elements = dataReader.ReadInt32();
                        Decimal[] arr = null;
                        if (elements >= 0)
                        {
                            arr = new Decimal[elements];
                            for (Int32 i = 0; i < elements; i++)
                                arr[i] = readDecimal(dataReader);
                        }
                        return arr;
                    }
                    else
                        return readDecimal(dataReader);

                case DataTypeEnum.Guid:
                    if (field.IsArray)
                    {
                        Int32 elements = dataReader.ReadInt32();
                        Guid[] arr = null;
                        if (elements >= 0)
                        {
                            arr = new Guid[elements];
                            for (Int32 i = 0; i < elements; i++)
                            {
                                Byte[] bytes = dataReader.ReadBytes(GuidByteLen);
                                arr[i] = new Guid(bytes);
                            }
                        }
                        return arr;
                    }
                    else
                    {
                        Byte[] bytes = dataReader.ReadBytes(GuidByteLen);
                        return new Guid(bytes);
                    }

                default:
                    // Error in type
                    throw new FileDbException(string.Format(FileDbException.StrInvalidDataType, (Int32) field.DataType), FileDbExceptionsEnum.InvalidDataType);
            }
        }

        void writeDbHeader(BinaryWriter writer)
        {
            writer.Seek(0, SeekOrigin.Begin);

            // Write the signature
            writer.Write(SIGNATURE);

            // Write the version
            writer.Write(_ver_major);
            writer.Write(_ver_minor);

            if (_ver_major >= 6)
            {
                writer.Write((UInt32) _flags);
                writer.Write((int) 0); // reserved
                _header_end_offset = 14;
            }
            else
                _header_end_offset = 6;

            _num_recs_offset = _header_end_offset;
            _index_deleted_offset = _header_end_offset + 4;
            _index_offset = _index_deleted_offset + 4;
        }

        /// <summary>
        /// Write the database schema and other 
        /// information.
        /// </summary>
        /// <param name="writer"></param>
        ///
        void writeSchema(BinaryWriter writer)
        {
            writer.Seek(_header_end_offset, SeekOrigin.Begin);

            writer.Write(_numRecords);
            writer.Write(_numDeleted);
            writer.Write(_indexStartPos);

            // preserve previous versions
            if (_ver >= 300)
            {
                if (_ver < 400)
                    writer.Write(_userVersion.ToString());
                else
                    writer.Write(_userVersion);
            }

            // Write the schema
            //
            // Schema format:
            //   [primary key field name]
            //   [number of fields]
            //     [field 1: name]
            //     [field 1: type]
            //     [field 1: flags]
            //     <field 1: possible autoinc value>
            //     [field 1: possible comment]
            //     ...
            //     [field n: name]
            //     [field n: type]
            //     [field n: flags]
            //     <field n: possible autoinc value>
            //     [field n: possible comment]
            //
            // For auto-incrementing fields, there is an extra Int32 specifying
            // the last value used in the last record added.

            writer.Write(_primaryKey);
            writer.Write(_fields.Count);

            // always write the key entry first
            // brettg NOTE: this will complicate adding a new field later which is a primary key

            if (!string.IsNullOrEmpty(_primaryKey))
            {
                Debug.Assert(_primaryKeyField != null);
                Debug.Assert(string.Compare(_primaryKeyField.Name, _primaryKey, StringComparison.CurrentCultureIgnoreCase) == 0);

                writeField(writer, _primaryKeyField);
            }

            // Write out all of the other entries
            foreach (Field field in _fields)
            {
                if (field != _primaryKeyField)
                    writeField(writer, field);
            }

            _dataStartPos = (Int32) _dbStream.Position;
        }

        void writeUserData(BinaryWriter dataWriter)
        {
            if (_userData == null)
            {
                // don't write anything to indicate no metadata
                return;
            }

            Type metaType = _userData.GetType();

            if (metaType == typeof(String))
            {
                dataWriter.Write((Int32) DataTypeEnum.String);
                dataWriter.Write((String) _userData);
            }
            else if (metaType == typeof(Byte[]))
            {
                dataWriter.Write((Int32) DataTypeEnum.Byte);
                Byte[] arr = (Byte[]) _userData;
                dataWriter.Write((Int32) arr.Length);
                dataWriter.Write(arr);
            }
        }

        void readUserData(BinaryReader reader)
        {
            _userData = null;

            try
            {
                if (_ver_major >= 2 && reader.PeekChar() != -1)
                {
                    var dataType = (DataTypeEnum) reader.ReadInt32();

                    switch (dataType)
                    {
                        case DataTypeEnum.String:
                            _userData = reader.ReadString();
                            break;

                        case DataTypeEnum.Byte:
                            // read the length
                            Int32 len = reader.ReadInt32();
                            _userData = reader.ReadBytes(len);
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.Assert(false, ex.Message);
                // brettg TODO: we need a way to report this error without throwing an exception
                // its best to continue on since this is the last thing we read when opening a db
                // and its better to get the other data rather than none at all
            }
        }

        /// <summary>
        /// Helper
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="field"></param>
        ///
        void writeField(BinaryWriter writer, Field field)
        {
            writer.Write(field.Name);
            writer.Write((Int16) field.DataType);

            Int32 flags = 0;
            if (field.IsAutoInc)
                flags |= AutoIncField;
            if (field.IsArray)
                flags |= ArrayField;
            writer.Write(flags);

            if (field.IsAutoInc)
            {
                writer.Write(field.AutoIncStart.Value);
                // CurAutoIncVal may not have been initialized yet
                if (!field.CurAutoIncVal.HasValue)
                    field.CurAutoIncVal = field.AutoIncStart;
                writer.Write(field.CurAutoIncVal.Value);
            }
            // ver 2.0
            writer.Write(field.Comment == null ? string.Empty : field.Comment);
        }

        void orderBy(object[][] result, string[] fieldList, string[] orderByList)
        {
            List<Field> sortFields = new List<Field>(orderByList.Length);
            List<bool> sortDirLst = new List<bool>(orderByList.Length);
            List<bool> caseLst = new List<bool>(orderByList.Length);

            GetOrderByLists(_fields, fieldList, orderByList, sortFields, sortDirLst, caseLst);

            Array.Sort(result, new RowComparer(sortFields, sortDirLst, caseLst));
        }

        internal static void GetOrderByLists(Fields fields, string[] fieldList, string[] orderByList, List<Field> sortFields,
                                List<bool> sortDirLst, List<bool> caseLst)
        {
            foreach (string s in orderByList)
            {
                string orderByField = s;

                // Do we want reverse or case-insensitive sort?

                bool rev_sort = false,
                     caseInsensitive = false;

                int ndx = 0;
                if (orderByField.Length > 0)
                {
                    rev_sort = orderByField[0] == '!';
                    caseInsensitive = orderByField[0] == '~';

                    if (orderByField.Length > 1)
                    {
                        if (!rev_sort)
                            rev_sort = orderByField[1] == '!';

                        if (!caseInsensitive)
                            caseInsensitive = orderByField[1] == '~';
                    }
                }

                // Remove the control code from the order by field
                if (rev_sort) ndx++;
                if (caseInsensitive) ndx++;

                if (ndx > 0)
                    orderByField = orderByField.Substring(ndx);

                sortDirLst.Add(rev_sort);
                caseLst.Add(caseInsensitive);

                string origOrderByName = orderByField;

                // Check the orderby field name
                if (!fields.ContainsKey(orderByField))
                {
                    throw new FileDbException(string.Format(FileDbException.InvalidOrderByFieldName, origOrderByName), FileDbExceptionsEnum.InvalidOrderByFieldName);
                }

                Field sortField = fields[orderByField];
                if (sortField.IsArray)
                {
                    throw new FileDbException(FileDbException.CannotOrderByOnArrayField, FileDbExceptionsEnum.CannotOrderByOnArrayField);
                }

                if (fieldList != null)
                {
                    // if fieldList is not null, it means its not all fields (subset) and so
                    // we must adjust the field ordinals to match the requested field columns
                    sortField = new Field(sortField.Name, sortField.DataType, -1);
                    for (int n = 0; n < fieldList.Length; n++)
                    {
                        if (string.Compare(fieldList[n], sortField.Name, StringComparison.OrdinalIgnoreCase) == 0)
                        {
                            sortField.Ordinal = n;
                            break;
                        }
                    }
                    if (sortField.Ordinal == -1)
                        throw new FileDbException(string.Format(FileDbException.InvalidOrderByFieldName, sortField), FileDbExceptionsEnum.InvalidOrderByFieldName);
                }

                sortFields.Add(sortField);
            }
        }

        void readSchema()
        {
            readSchema(_dataReader);
        }

        void readSchema(BinaryReader reader)
        {
            _dbStream.Seek(_header_end_offset, SeekOrigin.Begin);

            // Read the database statistics
            //

            _numRecords = reader.ReadInt32();
            _numDeleted = reader.ReadInt32();
            _indexStartPos = reader.ReadInt32();

            // read UserVersion
            if (_ver >= 300)
            {
                if (_ver < 400)
                {
                    // used to be string, but it was causing a bug here because we were somehow overwriting part of the 1st record!
                    string userVer = reader.ReadString();
                    if (userVer.Length > 0)
                        float.TryParse(userVer, out _userVersion);
                }
                else
                {
                    _userVersion = reader.ReadSingle();
                }
            }

            // Read the schema
            //
            // Schema format:
            //   [primary key field name]
            //   [number of fields]
            //     [field 1: name]
            //     [field 1: type]
            //     [field 1: flags]
            //     [field 1: possible comment]
            //     <field 1: possible autoinc value>
            //     ...
            //     [field n: name]
            //     [field n: type]
            //     [field n: flags]
            //     [field n: possible comment]
            //     <field n: possible autoinc value>
            //
            // For auto-incrementing fields, there is an extra Int32 specifying
            // the last value used in the last record added.

            _primaryKey = reader.ReadString();
            Int32 field_count = reader.ReadInt32();

            _fields = new Fields();

            for (Int32 i = 0; i < field_count; i++)
            {
                // Read the fields in
                string name = reader.ReadString();
                DataTypeEnum dataType = (DataTypeEnum) reader.ReadInt16();

                if (_ver < 201)
                {
                    // we changed the enum values to match the .NET TypeCodes

                    if ((DataTypeEnum_old) dataType == DataTypeEnum_old.Bool)
                        dataType = DataTypeEnum.Bool;
                    else if ((DataTypeEnum_old) dataType == DataTypeEnum_old.Byte)
                        dataType = DataTypeEnum.Byte;
                    else if ((DataTypeEnum_old) dataType == DataTypeEnum_old.Int)
                        dataType = DataTypeEnum.Int32;
                    else if ((DataTypeEnum_old) dataType == DataTypeEnum_old.UInt)
                        dataType = DataTypeEnum.UInt32;
                    else if ((DataTypeEnum_old) dataType == DataTypeEnum_old.Int64)
                        dataType = DataTypeEnum.Int64;
                    else if ((DataTypeEnum_old) dataType == DataTypeEnum_old.Float)
                        dataType = DataTypeEnum.Float;
                    else if ((DataTypeEnum_old) dataType == DataTypeEnum_old.Double)
                        dataType = DataTypeEnum.Double;
                    else if ((DataTypeEnum_old) dataType == DataTypeEnum_old.Decimal)
                        dataType = DataTypeEnum.Decimal;
                    else if ((DataTypeEnum_old) dataType == DataTypeEnum_old.DateTime)
                        dataType = DataTypeEnum.DateTime;
                    else if ((DataTypeEnum_old) dataType == DataTypeEnum_old.String)
                        dataType = DataTypeEnum.String;
                    else if ((DataTypeEnum_old) dataType == DataTypeEnum_old.Guid)
                        dataType = DataTypeEnum.Guid;
                }


                Field field = new Field(name, dataType, i);
                _fields.Add(field);

                if (string.Compare(_primaryKey, name, StringComparison.CurrentCultureIgnoreCase) == 0)
                {
                    field.IsPrimaryKey = true;
                    _primaryKeyField = field;
                }

                Int32 flags = reader.ReadInt32();

                if ((flags & AutoIncField) == AutoIncField)
                {
                    field.AutoIncStart = reader.ReadInt32();
                    field.CurAutoIncVal = reader.ReadInt32();
                }

                if ((flags & ArrayField) == ArrayField)
                    field.IsArray = true;

                if (_ver_major >= 2)
                    field.Comment = reader.ReadString();
                else
                    field.Comment = String.Empty;

            }

            // Save where the index starts
            _dataStartPos = (Int32) _dbStream.Position;
        }

        void writeNumRecords(BinaryWriter writer)
        {
            writer.Seek(_num_recs_offset, SeekOrigin.Begin);
            writer.Write(_numRecords);
        }

        void writeIndexStart(BinaryWriter writer)
        {
            writer.Seek(_index_offset, SeekOrigin.Begin);
            writer.Write(_indexStartPos);
        }

        #endregion private methods

        ///////////////////////////////////////////////////////////////////////
        #region RowComparer
        //=====================================================================
        class RowComparer : IComparer
        {
            List<Field> _fieldLst;
            List<bool> _sortDirLst,
                       _caseLst;

            internal RowComparer(List<Field> fieldLst, List<bool> sortDirLst, List<bool> caseLst)
            {
                _fieldLst = fieldLst;
                _caseLst = caseLst;
                _sortDirLst = sortDirLst;
            }

            // Calls CaseInsensitiveComparer.Compare with the parameters reversed

            Int32 IComparer.Compare(Object x, Object y)
            {
                Int32 nRet = 0;
                object v1, v2;
                object[] row1 = x as object[],
                         row2 = y as object[];

                if (row1 == null || row2 == null)
                    return 0;

                for (int n = 0; n < _fieldLst.Count; n++)
                {
                    Field field = _fieldLst[n];
                    bool reverseSort = _sortDirLst[n];
                    bool caseInsensitive = _caseLst[n];

                    v1 = row1[field.Ordinal];
                    v2 = row2[field.Ordinal];

                    int compVal = CompareVals(v1, v2, field.DataType, caseInsensitive);

                    if (reverseSort)
                        compVal = -compVal;

                    // we go until we find mismatch

                    if (compVal != 0)
                    {
                        nRet = compVal;
                        break;
                    }
                }

                return nRet;
            }
        }

        internal static int CompareVals(object v1, object v2, DataTypeEnum dataType, bool caseInsensitive)
        {
            int compVal = 0;

            switch (dataType)
            {
                case DataTypeEnum.Byte:
                    {
                        Byte b1 = (Byte) v1,
                             b2 = (Byte) v2;
                        compVal = b1 < b2 ? -1 : (b1 > b2 ? 1 : 0);
                    }
                    break;

                case DataTypeEnum.Int32:
                    {
                        Int32 i1 = (Int32) v1,
                            i2 = (Int32) v2;
                        compVal = i1 < i2 ? -1 : (i1 > i2 ? 1 : 0);
                    }
                    break;

                case DataTypeEnum.UInt32:
                    {
                        UInt32 i1 = (UInt32) v1,
                               i2 = (UInt32) v2;
                        compVal = i1 < i2 ? -1 : (i1 > i2 ? 1 : 0);
                    }
                    break;

                case DataTypeEnum.Bool:
                    {
                        Byte b1 = (Byte) v1,
                             b2 = (Byte) v2;
                        compVal = b1 < b2 ? -1 : (b1 > b2 ? 1 : 0);
                    }
                    break;

                case DataTypeEnum.Float:
                    {
                        float f1 = (float) v1,
                              f2 = (float) v2;
                        compVal = f1 < f2 ? -1 : (f1 > f2 ? 1 : 0);
                    }
                    break;

                case DataTypeEnum.Double:
                    {
                        double d1 = (double) v1,
                               d2 = (double) v2;
                        compVal = d1 < d2 ? -1 : (d1 > d2 ? 1 : 0);
                    }
                    break;

                case DataTypeEnum.DateTime:
                    {
                        DateTime dt1, dt2;

                        if (v1.GetType() == typeof(String))
                        {
                            dt1 = DateTime.Parse(v1.ToString());
                            dt2 = DateTime.Parse(v2.ToString());
                        }
                        else if (v1.GetType() == typeof(DateTime))
                        {
                            Debug.Assert(v1.GetType() == typeof(DateTime));
                            dt1 = (DateTime) v1;
                            dt2 = (DateTime) v2;
                        }
                        else
                            throw new FileDbException(FileDbException.InvalidDateTimeType, FileDbExceptionsEnum.InvalidDataType);

                        compVal = dt1 < dt2 ? -1 : (dt1 > dt2 ? 1 : 0);
                    }
                    break;

                case DataTypeEnum.String:
                    {
                        string s1 = (string) v1,
                               s2 = (string) v2;

                        // TODO: allow for culture sort rules
                        if (caseInsensitive)
                            compVal = string.Compare(s1, s2, StringComparison.CurrentCultureIgnoreCase);
                        else
                            compVal = string.Compare(s1, s2, StringComparison.CurrentCulture);
                    }
                    break;

                case DataTypeEnum.Int64:
                    {
                        Int64 i1 = (Int64) v1,
                              i2 = (Int64) v2;
                        compVal = i1 < i2 ? -1 : (i1 > i2 ? 1 : 0);
                    }
                    break;

                case DataTypeEnum.Decimal:
                    {
                        Decimal d1 = (Decimal) v1,
                                d2 = (Decimal) v2;
                        compVal = d1 < d2 ? -1 : (d1 > d2 ? 1 : 0);
                    }
                    break;

                case DataTypeEnum.Guid:
                    {
                        Guid g1 = (Guid) v1,
                             g2 = (Guid) v2;
                        compVal = g1.CompareTo(g2);
                    }
                    break;

                default:
                    Debug.Assert(false);
                    break;
            }

            return compVal;
        }
        #endregion RowComparer
    }
}
