package com.igniteplus.data.pipeline.service

import com.google.common.io.BaseEncoding
import com.igniteplus.data.pipeline.constants.ApplicationConstants._
import org.apache.spark.sql.DataFrame
import java.io.FileInputStream
import java.security.{Key, KeyStore}
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

object DbService
{
  def decryptUsingAESKey(encryptedData: String, key: Array[Byte]) : String = {
    val secKey : SecretKeySpec = new SecretKeySpec(key,CRYPTOGRAPHY_ALGORITHM)
    val cipher : Cipher = Cipher.getInstance(CRYPTOGRAPHY_ALGORITHM)
    cipher.init(Cipher.DECRYPT_MODE,secKey)
    val newData : Array[Byte] =cipher.doFinal(BaseEncoding.base64().decode(encryptedData))
    val message : String = new String(newData)
    message
  }
  def securityEncryptionDecryption(): String = {
    val keyStore : KeyStore = KeyStore.getInstance(KEY_TYPE);
    val stream : FileInputStream = new FileInputStream(KEY_LOCATION)
    keyStore.load(stream,KEY_PASSWORD.toCharArray)
    val key : Key = keyStore.getKey(KEY_ALIAS,KEY_PASSWORD.toCharArray)
    val encryptedData : String = scala.io.Source.fromFile(LOCATION_ENCRYPTED_PASSWORD).mkString
    val decryptedData = decryptUsingAESKey(encryptedData,key.getEncoded)
    decryptedData
  }


  def sqlWrite(df : DataFrame, tableName : String) : Unit = {
    val prop = new java.util.Properties
    prop.setProperty("driver", JDBC_DRIVER)
    prop.setProperty("user", USER_NAME)
    prop.setProperty("password", securityEncryptionDecryption())
    val url = SQL_URL
    df.write.mode("overwrite").jdbc(url, tableName, prop)
  }
}
