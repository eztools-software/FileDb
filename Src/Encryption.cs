/* Copyright (C) EzTools Software - All Rights Reserved
 * Proprietary and confidential source code.
 * This is not free software.  Any copying of this file 
 * via any medium is strictly prohibited except as allowed
 * by the FileDb license agreement.
 * Written by Brett Goodman <eztools-software.com>, October 2014
 */
using System.IO;
using System.Text;
#if !(NETFX_CORE || PCL)
using System.Security.Cryptography;
#endif

using System.Reflection;

namespace FileDbNs
{
    public interface IEncryptor
    {
        byte[] Encrypt(byte[] dataToEncrypt);
        byte[] Decrypt(byte[] encryptedData);
    }

    /// <summary>
    /// Class uses the .NET AesManaged class for data encryption, but ONLY on the Windows platform
    /// build...the PCL build just returns the same data without doing anything.  This is because
    /// encryption namespace isn't available for PCLs.  In this case, create your own Encryptor
    /// class using the IEncryptor interface and set it into the FileDb object via SetEncryptor.
    /// </summary>
    /// 
    public class Encryptor : IEncryptor
    {
#if NETSTANDARD1_6 || NETFX_CORE || PCL
        public Encryptor( string hashKey, string productKey )
        {
        }

        public byte[] Encrypt( byte[] dataToEncrypt )
        {
            return dataToEncrypt;
        }

        public byte[] Decrypt( byte[] encryptedData )
        {
            return encryptedData;
        }
#else
        byte[] _key;
        AesManaged _encryptor;

        /// <summary>
        /// Constructor taking a key (password) and salt as a string
        /// </summary>
        /// <param name="encryptionKey"></param>
        /// <param name="salt"></param>
        /// 
        public Encryptor(string encryptionKey, string salt)
        {
            _key = GetHashKey(encryptionKey, salt);
            _encryptor = new AesManaged();

            // Set the key
            _encryptor.Key = _key;
            _encryptor.IV = _key;
        }

        private static byte[] GetHashKey(string hashKey, string salt)
        {
            // Initialise
            UTF8Encoding encoder = new UTF8Encoding();

            // Get the salt
            byte[] saltBytes = encoder.GetBytes(salt);

            // Setup the hasher
            Rfc2898DeriveBytes rfc = new Rfc2898DeriveBytes(hashKey, saltBytes);

            // Return the key
            return rfc.GetBytes(16);
        }

        /// <summary>
        /// Encrypt the passed byte array
        /// </summary>
        /// <param name="dataToEncrypt">The data to encrypt</param>
        /// <returns>The encrypted data</returns>
        /// 
        public byte[] Encrypt(byte[] dataToEncrypt)
        {
            byte[] bytes = null;
            MemoryStream outStrm = new MemoryStream((int) (dataToEncrypt.Length * 1.5));

            // Create the crypto stream
            using (CryptoStream encrypt = new CryptoStream(outStrm, _encryptor.CreateEncryptor(), CryptoStreamMode.Write))
            {
                // Encrypt
                encrypt.Write(dataToEncrypt, 0, dataToEncrypt.Length);
                encrypt.FlushFinalBlock();
                bytes = outStrm.ToArray();
                encrypt.Close();
            }
            return bytes;
        }

        /// <summary>
        /// Decrypt the passed byte array
        /// </summary>
        /// <param name="encryptedData">The data to decrypt</param>
        /// <returns>The decrypted data</returns>
        /// 
        public byte[] Decrypt(byte[] encryptedData)
        {
            byte[] bytes = null;
            MemoryStream outStrm = new MemoryStream((int) (encryptedData.Length * 1.5));

            // Create the crypto stream
            using (CryptoStream decrypt = new CryptoStream(outStrm, _encryptor.CreateDecryptor(), CryptoStreamMode.Write))
            {
                // Encrypt
                decrypt.Write(encryptedData, 0, encryptedData.Length);
                decrypt.FlushFinalBlock();
                bytes = outStrm.ToArray();
                decrypt.Close();
            }
            return bytes;
        }

#endif
    }
}
