/* Copyright (C) EzTools Software - All Rights Reserved
 * Released under Mozilla Public License 2.0
 * Written and maintained by Brett Goodman <eztools-software.com>
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
    public class AesEncryptor : IEncryptor
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
        //byte[] _key;
        AesManaged _encryptor;

        /// <summary>
        /// Constructor taking a key (password) and salt as a string
        /// </summary>
        /// <param name="encryptionKey">The password</param>
        /// <param name="salt">Salt</param>
        /// 
        public AesEncryptor(string encryptionKey, string salt)
        {
            init(encryptionKey, salt, null);
        }

        /// <summary>
        /// Constructor taking a key (password) and salt as a byte[]
        /// </summary>
        /// <param name="encryptionKey">The password</param>
        /// <param name="salt">Salt</param>
        /// 
        public AesEncryptor(string encryptionKey, byte[] salt)
        {
            init(encryptionKey, salt, null);
        }

        /// <summary>
        /// Constructor taking a key (password) and salt as a string
        /// </summary>
        /// <param name="encryptionKey">The password</param>
        /// <param name="salt">Salt</param>
        /// <param name="iv">Initialization Vector</param>
        /// 
        public AesEncryptor(string encryptionKey, string salt, byte[] iv)
        {
            init(encryptionKey, salt, iv);
        }

        /// <summary>
        /// Constructor taking a key (password) and salt as a byte[]
        /// </summary>
        /// <param name="encryptionKey">The password</param>
        /// <param name="salt">Salt</param>
        /// <param name="iv">Initialization Vector</param>
        /// 
        public AesEncryptor(string encryptionKey, byte[] salt, byte[] iv)
        {
            init(encryptionKey, salt, iv);
        }

        void init(string encryptionKey, string salt, byte[] iv)
        {
            // Get the salt
            byte[] saltBytes;
            if (string.IsNullOrEmpty(salt))
                saltBytes = GetDefaultSalt(encryptionKey);
            else
                saltBytes = Encoding.UTF8.GetBytes(salt);

            init(encryptionKey, saltBytes, iv);
        }

        void init(string encryptionKey, byte[] saltBytes, byte[] iv)
        {
            var key = GetHashKey(encryptionKey, saltBytes);

            createEncryptor(encryptionKey, key, iv);
        }

        void createEncryptor(string encryptionKey, byte[] key, byte[] iv)
        {
            _encryptor = new AesManaged();

            // Set the key
            _encryptor.Key = key;
            if(iv != null)
                _encryptor.IV = iv;
            else
                _encryptor.IV = key; // must use the same IV
        }

        static byte[] GetDefaultSalt(string hashKey)
        {
            // gen for no salt
            var saltBytes = new byte[8];
            for (int n = 0; n < 8; n++)
                saltBytes[n] = 0xff;
            return saltBytes;
        }

        static byte[] GetHashKey(string hashKey, byte[] saltBytes)
        {
            // Setup the hasher
            var rfc = new Rfc2898DeriveBytes(hashKey, saltBytes);
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
