---
title: "User Manual: Security"
---

** Next:** [Administration][2] ** Up:** [Apache Accumulo User Manual Version 1.3][4] ** Previous:** [Analytics][6]   ** [Contents][8]**   
  
<a id="CHILD_LINKS"></a>**Subsections**

* [Security Label Expressions][9]
* [Security Label Expression Syntax][10]
* [Authorization][11]
* [Secure Authorizations Handling][12]
* [Query Services Layer][13]

* * *

## <a id="Security"></a> Security

Accumulo extends the BigTable data model to implement a security mechanism known as cell-level security. Every key-value pair has its own security label, stored under the column visibility element of the key, which is used to determine whether a given user meets the security requirements to read the value. This enables data of various security levels to be stored within the same row, and users of varying degrees of access to query the same table, while preserving data confidentiality. 

## <a id="Security_Label_Expressions"></a> Security Label Expressions

When mutations are applied, users can specify a security label for each value. This is done as the Mutation is created by passing a ColumnVisibility object to the put() method: 
    
    
    Text rowID = new Text("row1");
    Text colFam = new Text("myColFam");
    Text colQual = new Text("myColQual");
    ColumnVisibility colVis = new ColumnVisibility("public");
    long timestamp = System.currentTimeMillis();
    
    Value value = new Value("myValue");
    
    Mutation mutation = new Mutation(rowID);
    mutation.put(colFam, colQual, colVis, timestamp, value);
    

## <a id="Security_Label_Expression_Syntax"></a> Security Label Expression Syntax

Security labels consist of a set of user-defined tokens that are required to read the value the label is associated with. The set of tokens required can be specified using syntax that supports logical AND and OR combinations of tokens, as well as nesting groups of tokens together. 

For example, suppose within our organization we want to label our data values with security labels defined in terms of user roles. We might have tokens such as: 
    
    
    admin
    audit
    system
    

These can be specified alone or combined using logical operators: 
    
    
    // Users must have admin privileges:
    admin
    
    // Users must have admin and audit privileges
    admin&audit
    
    // Users with either admin or audit privileges
    admin|audit
    
    // Users must have audit and one or both of admin or system
    (admin|system)&audit
    

When both `|` and `&` operators are used, parentheses must be used to specify precedence of the operators. 

## <a id="Authorization"></a> Authorization

When clients attempt to read data from Accumulo, any security labels present are examined against the set of authorizations passed by the client code when the Scanner or BatchScanner are created. If the authorizations are determined to be insufficient to satisfy the security label, the value is suppressed from the set of results sent back to the client. 

Authorizations are specified as a comma-separated list of tokens the user possesses: 
    
    
    // user possess both admin and system level access
    Authorization auths = new Authorization("admin","system");
    
    Scanner s = connector.createScanner("table", auths);
    

## <a id="Secure_Authorizations_Handling"></a> Secure Authorizations Handling

Because the client can pass any authorization tokens to Accumulo, applications must be designed to obtain users' authorization tokens from a trusted 3rd party rather than having the users specify their authorizations directly. 

Often production systems will integrate with Public-Key Infrastructure (PKI) and designate client code within the query layer to negotiate with PKI servers in order to authenticate users and retrieve their authorization tokens (credentials). This requires users to specify only the information necessary to authenticate themselves to the system. Once user identity is established, their credentials can be accessed by the client code and passed to Accumulo outside of the reach of the user. 

## <a id="Query_Services_Layer"></a> Query Services Layer

Since the primary method of interaction with Accumulo is through the Java API, production environments often call for the implementation of a Query layer. This can be done using web services in containers such as Apache Tomcat, but is not a requirement. The Query Services Layer provides a mechanism for providing a platform on which user facing applications can be built. This allows the application designers to isolate potentially complex query logic, and enables a convenient point at which to perform essential security functions. 

Several production environments choose to implement authentication at this layer, where users identifiers are used to retrieve their access credentials which are then cached within the query layer and presented to Accumulo through the Authorizations mechanism. 

Typically, the query services layer sits between Accumulo and user workstations. 

* * *

** Next:** [Administration][2] ** Up:** [Apache Accumulo User Manual Version 1.3][4] ** Previous:** [Analytics][6]   ** [Contents][8]**

[2]: Administration.html
[4]: accumulo_user_manual.html
[6]: Analytics.html
[8]: Contents.html
[9]: Security.html#Security_Label_Expressions
[10]: Security.html#Security_Label_Expression_Syntax
[11]: Security.html#Authorization
[12]: Security.html#Secure_Authorizations_Handling
[13]: Security.html#Query_Services_Layer

