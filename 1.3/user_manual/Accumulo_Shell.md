---
title: "User Manual: Accumulo Shell"
---

** Next:** [Writing Accumulo Clients][2] ** Up:** [Apache Accumulo User Manual Version 1.3][4] ** Previous:** [Accumulo Design][6]   ** [Contents][8]**   
  
<a id="CHILD_LINKS"></a>**Subsections**

* [Basic Administration][9]
* [Table Maintenance][10]
* [User Administration][11]

* * *

## <a id="Accumulo_Shell"></a> Accumulo Shell

Accumulo provides a simple shell that can be used to examine the contents and configuration settings of tables, apply individual mutations, and change configuration settings. 

The shell can be started by the following command: 
    
    
    $ACCUMULO_HOME/bin/accumulo shell -u [username]
    

The shell will prompt for the corresponding password to the username specified and then display the following prompt: 
    
    
    Shell - Apache Accumulo Interactive Shell
    -
    - version 1.3
    - instance name: myinstance
    - instance id: 00000000-0000-0000-0000-000000000000
    -
    - type 'help' for a list of available commands
    -
    

## <a id="Basic_Administration"></a> Basic Administration

The Accumulo shell can be used to create and delete tables, as well as to configure table and instance specific options. 
    
    
    root@myinstance> tables
    !METADATA
    
    root@myinstance> createtable mytable
    
    root@myinstance mytable>
    
    root@myinstance mytable> tables
    !METADATA
    mytable
    
    root@myinstance mytable> createtable testtable
    
    root@myinstance testtable>
    
    root@myinstance junk> deletetable testtable
    
    root@myinstance>
    

The Shell can also be used to insert updates and scan tables. This is useful for inspecting tables. 
    
    
    root@myinstance mytable> scan
    
    root@myinstance mytable> insert row1 colf colq value1
    insert successful
    
    root@myinstance mytable> scan
    row1 colf:colq [] value1
    

## <a id="Table_Maintenance"></a> Table Maintenance

The **compact** command instructs Accumulo to schedule a compaction of the table during which files are consolidated and deleted entries are removed. 
    
    
    root@myinstance mytable> compact -t mytable
    07 16:13:53,201 [shell.Shell] INFO : Compaction of table mytable
    scheduled for 20100707161353EDT
    

The **flush** command instructs Accumulo to write all entries currently in memory for a given table to disk. 
    
    
    root@myinstance mytable> flush -t mytable
    07 16:14:19,351 [shell.Shell] INFO : Flush of table mytable
    initiated...
    

## <a id="User_Administration"></a> User Administration

The Shell can be used to add, remove, and grant privileges to users. 
    
    
    root@myinstance mytable> createuser bob
    Enter new password for 'bob': *********
    Please confirm new password for 'bob': *********
    
    root@myinstance mytable> authenticate bob
    Enter current password for 'bob': *********
    Valid
    
    root@myinstance mytable> grant System.CREATE_TABLE -s -u bob
    
    root@myinstance mytable> user bob
    Enter current password for 'bob': *********
    
    bob@myinstance mytable> userpermissions
    System permissions: System.CREATE_TABLE
    Table permissions (!METADATA): Table.READ
    Table permissions (mytable): NONE
    
    bob@myinstance mytable> createtable bobstable
    bob@myinstance bobstable>
    
    bob@myinstance bobstable> user root
    Enter current password for 'root': *********
    
    root@myinstance bobstable> revoke System.CREATE_TABLE -s -u bob
    

* * *

** Next:** [Writing Accumulo Clients][2] ** Up:** [Apache Accumulo User Manual Version 1.3][4] ** Previous:** [Accumulo Design][6]   ** [Contents][8]**

[2]: Writing_Accumulo_Clients.html
[4]: accumulo_user_manual.html
[6]: Accumulo_Design.html
[8]: Contents.html
[9]: Accumulo_Shell.html#Basic_Administration
[10]: Accumulo_Shell.html#Table_Maintenance
[11]: Accumulo_Shell.html#User_Administration

