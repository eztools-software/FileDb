/* Copyright (C) EzTools Software - All Rights Reserved
 * Proprietary and confidential source code.
 * This is not free software.  Any copying of this file 
 * via any medium is strictly prohibited except as allowed
 * by the FileDb license agreement.
 * Written by Brett Goodman <eztools-software.com>, October 2014
 */
using System;

namespace FileDbNs
{
    //=====================================================================
    /// <summary>
    /// Specifies the data type for database Fields
    /// </summary>
    /// 
    internal enum DataTypeEnum_old : short
    {
        String = 0, Byte = 1, Int = 2, UInt = 3, Float = 4, Double = 5, Bool = 6, DateTime = 7,
        Int64 = 8, Decimal = 9, Guid = 10, Undefined = 0x7FFF
    }

#if NETFX_CORE || PCL
    public enum TypeCode
  {
    Empty = 0,
    Object = 1,
    DBNull = 2,
    Boolean = 3,
    Char = 4,
    SByte = 5,
    Byte = 6,
    Int16 = 7,
    UInt16 = 8,
    Int32 = 9,
    UInt32 = 10,
    Int64 = 11,
    UInt64 = 12,
    Single = 13,
    Double = 14,
    Decimal = 15,
    DateTime = 16,
    String = 18,
  }
#endif

    public enum DataTypeEnum : short
    {
        Bool = TypeCode.Boolean,
        Byte = TypeCode.Byte,
        Int = TypeCode.Int32,
        Int32 = TypeCode.Int32,
        UInt = TypeCode.UInt32,
        UInt32 = TypeCode.UInt32,
        Long = TypeCode.Int64,
        Int64 = TypeCode.Int64,
        Float = TypeCode.Single,
        Single = TypeCode.Single,
        Double = TypeCode.Double,
        Decimal = TypeCode.Decimal,
        DateTime = TypeCode.DateTime,
        String = TypeCode.String,
        Guid = 100,
        Undefined = 0x7FFF
    }

#if false
    public enum FolderLocEnum
    {
#if !WINDOWS_PHONE_APP
        Default,
#else
        Default = 0,
        LocalFolder = 0,
        RoamingFolder,
        TempFolder
#endif
    }
#endif

    //=====================================================================
    /// <summary>
    /// Specifies the type of match for FilterExpressions with String data types
    /// </summary>
    public enum MatchTypeEnum { UseCase, IgnoreCase }

    //=====================================================================
    /// <summary>
    /// Specifies the comparison operator to use for FilterExpressions
    /// </summary>
    public enum ComparisonOperatorEnum { Equal, NotEqual, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, In, Regex, Contains }

    //=====================================================================
    /// <summary>
    /// Boolean operands to use to join FilterExpressions 
    /// </summary>
    public enum BoolOpEnum { And, Or }

}
