package org.apache.accumulo.core.security.crypto;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.NullCipher;

import org.apache.log4j.Logger;

public class DefaultCryptoModuleUtils {

  private static final Logger log = Logger.getLogger(DefaultCryptoModuleUtils.class);
  
  public static SecureRandom getSecureRandom(String secureRNG, String secureRNGProvider) {
    SecureRandom secureRandom = null;
    try {
      secureRandom = SecureRandom.getInstance(secureRNG, secureRNGProvider);
      
      // Immediately seed the generator
      byte[] throwAway = new byte[16];
      secureRandom.nextBytes(throwAway);
      
    } catch (NoSuchAlgorithmException e) {
      log.error(String.format("Accumulo configuration file specified a secure random generator \"%s\" that was not found by any provider.", secureRNG));
      throw new RuntimeException(e);
    } catch (NoSuchProviderException e) {
      log.error(String.format("Accumulo configuration file specified a secure random provider \"%s\" that does not exist", secureRNGProvider));
      throw new RuntimeException(e);
    }
    return secureRandom;
  }

  public static Cipher getCipher(String cipherSuite) {
    Cipher cipher = null;
    
    if (cipherSuite.equals("NullCipher")) {
      cipher = new NullCipher();
    } else {
      try {
        cipher = Cipher.getInstance(cipherSuite);
      } catch (NoSuchAlgorithmException e) {
        log.error(String.format("Accumulo configuration file contained a cipher suite \"%s\" that was not recognized by any providers", cipherSuite));
        throw new RuntimeException(e);
      } catch (NoSuchPaddingException e) {
        log.error(String.format("Accumulo configuration file contained a cipher, \"%s\" with a padding that was not recognized by any providers"));
        throw new RuntimeException(e);
      }
    }
    return cipher;
  }
  
  
}
