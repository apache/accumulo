---
title: "Generating Keystores for configuring Accumulo with SSL"
date: 2014-09-02 17:00:00 +0000
author: Josh Elser
---

Originally posted at [https://blogs.apache.org/accumulo/entry/generating_keystores_for_configuring_accumulo](https://blogs.apache.org/accumulo/entry/generating_keystores_for_configuring_accumulo)

One of the major features added in Accumulo 1.6.0 was the ability to configure Accumulo so that the Thrift communications will run over SSL. [Apache Thrift][thrift] is the remote procedure call library that is leverage for both intra-server communication and client communication with Accumulo. Issuing these calls over a secure socket ensures that unwanted actors cannot inspect the traffic sent across the wire. Given the sometimes sensitive nature of data stored in Accumulo and the authentication details for users, ensuring that no prying eyes have access to these communications is critical.

Due to the complex and deployment specific nature of the security model for some system, Accumulo expects users to provide their own certificates, guaranteeing that they are, in fact, secure. However, for those who want to get security who do not already operate within the confines of an established security infrastructure, OpenSSL and the Java keytool command can be used to generate the necessary components to enable wire encryption.

To enable SSL with Accumulo, it is necessary to generate a certificate authority and certificates which are signed by that authority. Typically, each client and server has its own certificate which provides the finest level of control over a secure cluster when the certificates are properly secured.

## Generate a Certificate Authority

The certificate authority (CA) is what controls what certificates can be used to authenticate with each other. To create a secure connection with two certificates, each certificate must be signed by a certificate authority in the "truststore" (A Java KeyStore which contains at least one Certificate Authority's public key). When creating your own certificate authority, a single CA is typically sufficient (and would result in a single public key in the truststore). Alternatively, a third party can also act as a certificate authority (to add an additional layer of security); however, these are typically not a free service.

The below is an example of creating a certificate authority and adding its public key to a Java KeyStore to provide to Accumulo.

```bash
# Create a private key
openssl genrsa -des3 -out root.key 4096

# Create a certificate request using the private key
openssl req -x509 -new -key root.key -days 365 -out root.pem

# Generate a Base64-encoded version of the PEM just created
openssl x509 -outform der -in root.pem -out root.der

# Import the key into a Java KeyStore
keytool -import -alias root-key -keystore truststore.jks -file root.der

# Remove the DER formatted key file (as we don't need it anymore)
rm root.der
```

Remember to protect root.key and never distribute it as the private key is the basis for your circle of trust. The keytool command will prompt you about whether or not the certificate should be trusted: enter "yes". The truststore.jks file, a "truststore", is meant to be shared with all parties communicating with one another. The password provided to the truststore verifies that the contents of the truststore have not been tampered with.

## Generate a certificate/keystore per host

For each host in the system, it's desirable to generate a certificate. Typically, this corresponds to a certificate per host. Additionally, each client connecting to the Accumulo instance running with SSL should be issued their own certificate. By issuing individual certificates to each entity, it gives proper control to revoke/reissue certificates to clients as necessary, without widespread interruption.

```bash
# Create the private key for our server
openssl genrsa -out server.key 4096

# Generate a certificate signing request (CSR) with our private key
openssl req -new -key server.key -out server.csr

# Use the CSR and the CA to create a certificate for the server (a reply to the CSR)
openssl x509 -req -in server.csr -CA root.pem -CAkey root.key -CAcreateserial -out server.crt -days 365

# Use the certificate and the private key for our server to create PKCS12 file
openssl pkcs12 -export -in server.crt -inkey server.key -certfile server.crt -name 'server-key' -out server.p12

# Create a Java KeyStore for the server using the PKCS12 file (private key)
keytool -importkeystore -srckeystore server.p12 -srcstoretype pkcs12 -destkeystore server.jks -deststoretype JKS

# Remove the PKCS12 file as we don't need it
rm server.p12

# Import the CA-signed certificate to the keystore
keytool -import -trustcacerts -alias server-crt -file server.crt -keystore server.jks
```

These commands create a private key for the server, generated a certificate signing request created from that private key, used the certificate authority to generate the certificate using the signing request and then created a Java KeyStore with the certificate and the private key for our server. This, paired with the truststore, provide what is needed to configure Accumulo servers to run over SSL. Both the private key (server.key), the certificate signed by the CA (server.pem), and the keystore (server.jks) should be restricted to only be accessed by the user running Accumulo on the host it was generated for. Use chown and chmod to protect the files and do not distribute them over insecure networks.

## Configure Accumulo Servers

Now that the Java KeyStores have been created with the necessary information, the Accumulo configuration must be updated so that Accumulo creates the Thrift server over SSL instead of a normal socket. In accumulo-site.xml, configure the following:

```xml
<property>
  <name>rpc.javax.net.ssl.keyStore</name>
  <value>/path/to/server.jks</value>
</property>
<property>
  <name>rpc.javax.net.ssl.keyStorePassword</name>
  <value>server_password</value>
</property>
<property>
  <name>rpc.javax.net.ssl.trustStore</name>
  <value>/path/to/truststore.jks</value>
</property>
<property>
  <name>rpc.javax.net.ssl.trustStorePassword</name>
  <value>truststore_password</value>
</property>
<property>
  <name>instance.rpc.ssl.enabled</name>
  <value>true</value>
</property>
```

The keystore and truststore paths are both absolute paths on the local filesystem (not HDFS). Remember that the server keystore should only be readable by the user running Accumulo and, if you place plaintext passwords in accumulo-site.xml, make sure that accumulo-site.xml is also not globally readable. To keep these passwords out of accumulo-site.xml, consider configuring your system with the new Hadoop CredentialProvider class, see [ACCUMULO-2464][accumulo-2464] for more information which will be available in Accumulo-1.6.1.

Also, be aware that if unique passwords are used for each server when generating the certificate, this will result in different accumulo-site.xml files for each host. Unique configuration files per host will add much complexity to the configuration management of your instance. The use of a CredentialProvider, a feature from Hadoop which allows for acquisitions of passwords from alternate systems) can be used to help alleviate the unique accumulo-site.xml files on each host. A Java KeyStore can be created using the CredentialProvider tools which removes the necessity of passwords to be stored in accumulo-site.xml and can instead point to the CredentialProvider URI which is consistent across hosts.

## Configure Accumulo Clients

To configure Accumulo clients, use $HOME/.accumulo/config. This is a simple [Java properties file][props]: each line is a configuration, key and value can be separated by a space, and lines beginning with a # symbol are ignored. For example, if we generated a certificate and placed it in a keystore (as described above), we would generate the following file for the Accumulo client.

```
instance.rpc.ssl.enabled true
rpc.javax.net.ssl.keyStore  /path/to/client-keystore.jks
rpc.javax.net.ssl.keyStorePassword  client-password
rpc.javax.net.ssl.trustStore  /path/to/truststore.jks
rpc.javax.net.ssl.trustStorePassword  truststore-password
```
When creating a ZooKeeperInstance, the implementation will automatically look for this file and set up a connection with the methods defined in this configuration file. The ClientConfiguration class also contains methods that can be used instead of a configuration file on the filesystem. Again, the paths to the keystore and truststore are on the local filesystem, not HDFS.

[thrift]: http://thrift.apache.org/
[accumulo-2464]: https://issues.apache.org/jira/browse/ACCUMULO-2464
[props]: http://en.wikipedia.org/wiki/.properties
