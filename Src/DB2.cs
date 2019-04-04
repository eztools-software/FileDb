/* Copyright (C) EzTools Software - All Rights Reserved
 * Released under Mozilla Public License 2.0
 * Written and maintained by Brett Goodman <eztools-software.com>
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace FileDbNs
{
    /*
    [System.AttributeUsage( System.AttributeTargets.Property )]
    public class FileDbField : System.Attribute
    {
        public FileDbField( string name )
        {
        }
    }*/

    public partial class FileDb
    {

        //----------------------------------------------------------------------------------------
        // Create a table from the raw records
        //
        private List<T> createTList<T>(object[][] records, string[] fieldList, bool includeIndex, string[] orderByList)
            where T : class, new()
        {
            // get the field list
            int nExtra = includeIndex ? 1 : 0;
            Fields fields = null;
            if (fieldList != null)
            {
                fields = new Fields(fieldList.Length + nExtra);
                foreach (string fieldName in fieldList)
                {
                    if (fields.ContainsKey(fieldName))
                        throw new FileDbException(string.Format(FileDbException.FieldSpecifiedTwice, fieldName),
                            FileDbExceptionsEnum.FieldSpecifiedTwice);
                    fields.Add(_dbEngine.Fields[fieldName]);
                }
            }
            else
            {
                fields = new Fields(_dbEngine.Fields.Count + nExtra);
                foreach (Field field in _dbEngine.Fields)
                {
                    fields.Add(field);
                }
            }

            if (includeIndex)
                fields.Add(new Field(StrIndex, DataTypeEnum.Int32, fields.Count));

            // use reflection to populate the Field properties

#if NETSTANDARD1_6 || NETFX_CORE //|| PCL
            IEnumerable<PropertyInfo> propertyInfos = typeof( T ).GetRuntimeProperties();
#else
            PropertyInfo[] propertyInfos = typeof(T).GetProperties(BindingFlags.Public | ~BindingFlags.Static);
#endif

            Dictionary<string, PropertyInfo> propsMap = new Dictionary<string, PropertyInfo>(propertyInfos.Count());

            foreach (Field field in fields)
            {
                PropertyInfo prop = propertyInfos.Where(p => string.Compare(p.Name, field.Name, StringComparison.CurrentCultureIgnoreCase) == 0).FirstOrDefault();

                if (prop != null)
                {
                    //Attribute attrib = Attribute.GetCustomAttribute( prop, typeof( FileDbField ) );
                    //if( attrib != null )
                    //{
                    // TODO: check that the datatypes match

                    Type fieldType = null;

                    switch (field.DataType)
                    {
                        case DataTypeEnum.Bool:
                            fieldType = field.IsArray ? typeof(Boolean[]) : typeof(Boolean);
                            break;
                        case DataTypeEnum.Byte:
                            fieldType = field.IsArray ? typeof(Byte[]) : typeof(Byte);
                            break;
                        case DataTypeEnum.Int32:
                            fieldType = field.IsArray ? typeof(Int32[]) : typeof(Int32);
                            break;
                        case DataTypeEnum.UInt32:
                            fieldType = field.IsArray ? typeof(UInt32[]) : typeof(UInt32);
                            break;
                        case DataTypeEnum.Int64:
                            fieldType = field.IsArray ? typeof(Int64[]) : typeof(Int64);
                            break;
                        case DataTypeEnum.Single:
                            fieldType = field.IsArray ? typeof(Single[]) : typeof(Single);
                            break;
                        case DataTypeEnum.Double:
                            fieldType = field.IsArray ? typeof(Double[]) : typeof(Double);
                            break;
                        case DataTypeEnum.Decimal:
                            fieldType = field.IsArray ? typeof(Decimal[]) : typeof(Decimal);
                            break;
                        case DataTypeEnum.DateTime:
                            fieldType = field.IsArray ? typeof(DateTime[]) : typeof(DateTime);
                            break;
                        case DataTypeEnum.String:
                            fieldType = field.IsArray ? typeof(String[]) : typeof(String);
                            break;
                        case DataTypeEnum.Guid:
                            fieldType = field.IsArray ? typeof(Guid[]) : typeof(Guid);
                            break;
                    }

                    // we use Contains rather than direct comparison because if the Property is Nullable type
                    if (prop.PropertyType.FullName.Contains(fieldType.FullName) == false)
                    {
                        throw new Exception(string.Format("The type of Property {0} doesn't match the Field DataType - expected {1} but was {2}",
                            prop.Name, fieldType, prop.PropertyType));
                    }

                    propsMap.Add(field.Name, prop);
                    //}
                }
            }

            List<T> table = new List<T>(records != null ? records.Length : 0);

            if (records != null)
            {
                foreach (object[] record in records)
                {
                    T obj = new T();

                    for (int n = 0; n < fields.Count; n++)
                    {
                        Field field = fields[n];

                        if (propsMap.ContainsKey(field.Name))
                        {
                            PropertyInfo prop = propsMap[field.Name];

                            if (prop.CanWrite)
                            {
                                object value = record[n];
                                prop.SetValue(obj, value, null);
                            }
                        }
                    }

                    table.Add(obj);
                }
            }
            return table;
        }

        #region SelecRecords

        #region SelectRecords FilterExpression

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return a List of custom objects filtered by the filter parameter.
        /// </summary>
        /// <param name="filter">A FilterExpression representing the desired filter.</param>
        /// <returns>A new List of custom objects with the requested Records</returns>
        ///
        public List<T> SelectRecords<T>(FilterExpression filter)
            where T : class, new()
        {
            return SelectRecords<T>(filter, null, null, false);
        }

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return a List of custom objects filtered by the filter parameter. Only the specified Fields
        /// will be in the Table.
        /// </summary>
        /// <param name="filter">A FilterExpression representing the desired filter.</param>
        /// <param name="fieldList">The desired fields to be in the returned Table</param>
        /// <returns>A new List of custom objects with the requested Records and Fields</returns>
        /// 
        public List<T> SelectRecords<T>(FilterExpression filter, string[] fieldList)
            where T : class, new()
        {
            return SelectRecords<T>(filter, fieldList, null, false);
        }

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return a List of custom objects filtered by the filter parameter. Only the specified Fields
        /// will be in the Table.
        /// </summary>
        /// <param name="filter">A FilterExpression representing the desired filter.</param>
        /// <param name="fieldList">The desired fields to be in the returned Table</param>
        /// <param name="orderByList">A list of one or more fields to order the returned table by, 
        /// or null for default order. If an orderByField is prefixed with "!", that field will sorted
        /// in reverse order.</param>
        /// <returns>A new List of custom objects with the requested Records and Fields ordered by the specified fields.</returns>
        /// 
        public List<T> SelectRecords<T>(FilterExpression filter, string[] fieldList, string[] orderByList)
            where T : class, new()
        {
            return SelectRecords<T>(filter, fieldList, orderByList, false);
        }

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Get all records matching the search expression in the indicated order, if any.
        /// </summary>
        /// <param name="filter">Represents a single search expression, such as ID = 3</param>
        /// <param name="fieldList">The list of fields to return or null for all fields</param>
        /// <param name="includeIndex">If true, an additional Field named "index" will be returned
        /// which is the ordinal index of the Record in the database, which can be used in
        /// GetRecordByIndex and UpdateRecordByIndex.</param>
        /// <param name="orderByList">A list of one or more fields to order the returned table by, 
        /// or null for default order. If an orderByField is prefixed with "!", that field will sorted
        /// in reverse order.</param>
        /// <returns>A new List of custom objects with the requested Records and Fields</returns>
        /// 
        public List<T> SelectRecords<T>(FilterExpression filter, string[] fieldList, string[] orderByList, bool includeIndex)
            where T : class, new()
        {
            lock (this)
            {
                object[][] records = _dbEngine.GetRecordByField(filter, fieldList, includeIndex, orderByList);
                return createTList<T>(records, fieldList, includeIndex, orderByList);
            }
        }

        #endregion SelectRecords<T> FilterExpression

        #region SelectRecords<T> FilterExpressionGroup

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return a List of custom objects filtered by the filter parameter.
        /// </summary>
        /// <param name="filter">A FilterExpressionGroup representing the desired filter.</param>
        /// <returns>A new List of custom objects with the requested Records</returns>
        /// 
        public List<T> SelectRecords<T>(FilterExpressionGroup filter)
            where T : class, new()
        {
            return SelectRecords<T>(filter, null, null, false);
        }

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return a List of custom objects filtered by the filter parameter. Only the specified Fields
        /// will be in the Table.
        /// </summary>
        /// <param name="filter">A FilterExpression representing the desired filter.</param>
        /// <param name="fieldList">The desired fields to be in the returned Table</param>
        /// <returns>A new List of custom objects with the requested Records and Fields</returns>
        /// 
        public List<T> SelectRecords<T>(FilterExpressionGroup filter, string[] fieldList)
            where T : class, new()
        {
            return SelectRecords<T>(filter, fieldList, null, false);
        }

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return a List of custom objects filtered by the filter parameter. Only the specified Fields
        /// will be in the Table.
        /// </summary>
        /// <param name="filter">A FilterExpression representing the desired filter.</param>
        /// <param name="fieldList">The desired fields to be in the returned Table</param>
        /// <param name="orderByList">A list of one or more fields to order the returned table by, 
        /// or null for default order. If an orderByField is prefixed with "!", that field will sorted
        /// in reverse order.</param>
        /// <returns>A new List of custom objects with the requested Records and Fields in the specified order</returns>
        /// 
        public List<T> SelectRecords<T>(FilterExpressionGroup filter, string[] fieldList, string[] orderByList)
            where T : class, new()
        {
            return SelectRecords<T>(filter, fieldList, orderByList, false);
        }

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Get all records matching the FilterExpressionGroup in the indicated order, if any.
        /// </summary>
        /// <param name="filter">Represents a compound search expression, such as FirstName = "John" AND LastName = "Smith"</param>
        /// <param name="fieldList">The list of fields to return or null for all fields</param>
        /// <param name="includeIndex">Specify whether to include the record index as one of the Fields</param>
        /// <param name="orderByList">A list of one or more fields to order the returned table by, 
        /// or null for default order. If an orderByField is prefixed with "!", that field will sorted
        /// in reverse order.</param>
        /// <returns>A new List of custom objects with the requested Records and Fields</returns>
        ///
        public List<T> SelectRecords<T>(FilterExpressionGroup filter, string[] fieldList, string[] orderByList, bool includeIndex)
            where T : class, new()
        {
            lock (this)
            {
                object[][] records = _dbEngine.GetRecordByFields(filter, fieldList, includeIndex, orderByList);
                return createTList<T>(records, fieldList, includeIndex, orderByList);
            }
        }

        #endregion SelectRecords FilterExpressionGroup

        #region SelectRecords string
        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return a List of custom objects filtered by the filter parameter.
        /// </summary>
        /// <param name="filter">A string representing the desired filter, eg. LastName = 'Fuller'</param>
        /// <returns>A new List of custom objects with the requested Records</returns>
        /// 
        public List<T> SelectRecords<T>(string filter)
            where T : class, new()
        {
            FilterExpressionGroup filterExpGrp = FilterExpressionGroup.Parse(filter);
            return SelectRecords<T>(filterExpGrp);
        }

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return a List of custom objects filtered by the filter parameter. Only the specified Fields
        /// will be in the Table.
        /// </summary>
        /// <param name="filter">A string representing the desired filter, eg. LastName = 'Fuller'</param>
        /// <param name="fieldList">The desired fields to be in the returned Table</param>
        /// <returns>A new List of custom objects with the requested Records and Fields</returns>
        /// 
        public List<T> SelectRecords<T>(string filter, string[] fieldList)
            where T : class, new()
        {
            FilterExpressionGroup filterExpGrp = FilterExpressionGroup.Parse(filter);
            return SelectRecords<T>(filterExpGrp, fieldList);
        }

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return a List of custom objects filtered by the filter parameter. Only the specified Fields
        /// will be in the Table.
        /// </summary>
        /// <param name="filter">A string representing the desired filter, eg. LastName = 'Fuller'</param>
        /// <param name="fieldList">The desired fields to be in the returned Table</param>
        /// <param name="orderByList">A list of one or more fields to order the returned table by, 
        /// or null for default order. If an orderByField is prefixed with "!", that field will sorted
        /// in reverse order.</param>
        /// <returns>A new List of custom objects with the requested Records and Fields ordered by the specified list</returns>
        /// 
        public List<T> SelectRecords<T>(string filter, string[] fieldList, string[] orderByList)
            where T : class, new()
        {
            FilterExpressionGroup filterExpGrp = FilterExpressionGroup.Parse(filter);
            return SelectRecords<T>(filterExpGrp, fieldList, orderByList);
        }

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return a List of custom objects filtered by the filter parameter.
        /// </summary>
        /// <param name="filter">A string representing the desired filter, eg. LastName = 'Fuller'</param>
        /// <param name="fieldList">The desired fields to be in the returned Table</param>
        /// <param name="includeIndex">If true, an additional Field named "index" will be returned
        /// which is the ordinal index of the Record in the database, which can be used in
        /// GetRecordByIndex and UpdateRecordByIndex</param>
        /// <param name="orderByList">A list of one or more fields to order the returned table by, 
        /// or null for default order. If an orderByField is prefixed with "!", that field will sorted
        /// in reverse order</param>
        /// <returns>A new List of custom objects with the requested Records and Fields</returns>
        /// 
        public List<T> SelectRecords<T>(string filter, string[] fieldList, string[] orderByList, bool includeIndex)
            where T : class, new()
        {
            FilterExpressionGroup filterExpGrp = FilterExpressionGroup.Parse(filter);
            return SelectRecords<T>(filterExpGrp, fieldList, orderByList, includeIndex);
        }
        #endregion SelectRecords string

        #region SelectAllRecords
        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return all records in the database (table).
        /// </summary>
        /// <returns>A table containing all Records and Fields.</returns>
        /// 
        public List<T> SelectAllRecords<T>()
            where T : class, new()
        {
            return SelectAllRecords<T>(null, null, false);
        }

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return all records in the database (table).
        /// </summary>
        /// <param name="fieldList">The list of Fields to return or null for all Fields</param>
        /// <returns>A table containing all rows.</returns>
        /// 
        public List<T> SelectAllRecords<T>(string[] fieldList)
            where T : class, new()
        {
            return SelectAllRecords<T>(fieldList, null, false);
        }

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return all records in the database (table).
        /// </summary>
        /// <param name="fieldList">The list of fields to return or null for all Fields</param>
        /// <param name="orderByList">A list of one or more fields to order the returned table by, 
        /// or null for default order</param>
        /// <returns>A table containing all rows.</returns>
        /// 
        public List<T> SelectAllRecords<T>(string[] fieldList, string[] orderByList)
            where T : class, new()
        {
            return SelectAllRecords<T>(fieldList, orderByList, false);
        }

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return all records in the database (table).
        /// </summary>
        /// <param name="includeIndex">Specify whether to include the Record index as one of the Fields</param>
        /// <returns>A table containing all rows.</returns>
        /// 
        public List<T> SelectAllRecords<T>(bool includeIndex)
            where T : class, new()
        {
            return SelectAllRecords<T>(null, null, includeIndex);
        }

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Return all records in the database (table).
        /// </summary>
        /// <param name="fieldList">The list of fields to return or null for all fields</param>
        /// <param name="includeIndex">Specify whether to include the record index as one of the Fields</param>
        /// <param name="orderByList">A list of one or more fields to order the returned table by, 
        /// or null for default order</param>
        /// <returns>A table containing all Records and the specified Fields.</returns>
        /// 
        public List<T> SelectAllRecords<T>(string[] fieldList, string[] orderByList, bool includeIndex)
            where T : class, new()
        {
            lock (this)
            {
                object[][] records = _dbEngine.GetAllRecords(fieldList, includeIndex, orderByList);
                return createTList<T>(records, fieldList, includeIndex, orderByList);
            }
        }
        #endregion SelectAllRecords

        #endregion SelecRecords

        #region GetRecord

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Returns a single custom object at the current location.  Meant to be used ONLY in conjunction
        /// with the MoveFirst/MoveNext methods.
        /// </summary>
        /// <param name="fieldList">The list of fields to return or null for all fields</param>
        /// <param name="includeIndex">Specify whether to include the record index as one of the Fields</param>
        /// <returns>A new object or null</returns>
        /// 
        public T GetCurrentRecord<T>(string[] fieldList, bool includeIndex)
            where T : class, new()
        {
            lock (this)
            {
                object[] record = _dbEngine.GetCurrentRecord(includeIndex);
                return createT<T>(record, fieldList, false);
            }
        }

        //----------------------------------------------------------------------------------------
        /// 
        /// <summary>
        /// Returns a single custom object specified by the index.
        /// </summary>
        /// <param name="index">The index of the record to return. This value can be obtained from
        /// Record returning queries by specifying true for the includeIndex parameter.</param>
        /// <param name="fieldList">The list of fields to return or null for all fields</param>
        /// <returns>A new object or null</returns>
        /// 
        public T GetRecordByIndex<T>(Int32 index, string[] fieldList)
            where T : class, new()
        {
            lock (this)
            {
                object[] record = _dbEngine.GetRecordByIndex(index, fieldList, false);
                return createT<T>(record, fieldList, false);
            }
        }

        //----------------------------------------------------------------------------------------
        /// <summary>
        /// Returns a single custom object specified by the primary key value or record number.
        /// </summary>
        /// <param name="key">The primary key value.  For databases without a primary key, 
        /// 'key' is the zero-based record number in the table.</param>
        /// <param name="fieldList">The list of fields to return or null for all fields</param>
        /// <param name="includeIndex">Specify whether to include the record index as one of the Fields</param>
        /// <returns>A new object or null</returns>
        /// 
        public T GetRecordByKey<T>(object key, string[] fieldList, bool includeIndex)
            where T : class, new()
        {
            lock (this)
            {
                object[] record = _dbEngine.GetRecordByKey(key, fieldList, includeIndex);
                return createT<T>(record, fieldList, includeIndex);
            }
        }

        T createT<T>(object[] record, string[] fieldList, bool includeIndex)
            where T : class, new()
        {
            T obj = null;

            // TODO: try to make this more efficient by not creating a new Fields list

            if (record != null)
            {
                int nExtra = includeIndex ? 1 : 0;
                Fields fields = null;
                if (fieldList != null)
                {
                    fields = new Fields(fieldList.Length + nExtra);
                    foreach (string fieldName in fieldList)
                    {
                        if (fields.ContainsKey(fieldName))
                            throw new FileDbException(string.Format(FileDbException.FieldSpecifiedTwice, fieldName), FileDbExceptionsEnum.FieldSpecifiedTwice);
                        fields.Add(_dbEngine.Fields[fieldName]);
                    }
                }
                else
                {
                    fields = new Fields(_dbEngine.Fields.Count + nExtra);
                    foreach (Field field in _dbEngine.Fields)
                    {
                        fields.Add(field);
                    }
                }

                if (includeIndex)
                    fields.Add(new Field(StrIndex, DataTypeEnum.Int32, fields.Count));

                obj = new T();
#if NETSTANDARD1_6 || NETFX_CORE //|| PCL
                IEnumerable<PropertyInfo> propertyInfos = typeof( T ).GetRuntimeProperties();
#else
                PropertyInfo[] propertyInfos = typeof(T).GetProperties(BindingFlags.Public | ~BindingFlags.Static);
#endif
                Dictionary<string, PropertyInfo> propsMap = new Dictionary<string, PropertyInfo>(propertyInfos.Count());

                for (int n = 0; n < fields.Count; n++)
                {
                    Field field = fields[n];
                    PropertyInfo prop = propertyInfos.Where(p => string.Compare(p.Name, field.Name, StringComparison.CurrentCultureIgnoreCase) == 0).FirstOrDefault();

                    if (prop != null)
                    {
                        if (prop.CanWrite)
                        {
                            object value = record[n];
                            prop.SetValue(obj, value, null);
                        }
                    }
                }
            }

            return obj;
        }

        #endregion GetRecord
    }
}
