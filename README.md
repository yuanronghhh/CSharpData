#CSharpDataLib

Simple Data Tool Libaray for Operate Data in C#
this is a useful tool if you want copy table in database;

## QuickView

1. Connect Database
```C#
// Connect
using (SQLServerClientService  = SQLServerClientService.GetInstance(conn2)) 
{
  // Do Something
}

// Nested Connect
using (SQLServerClientService s1 = SQLServerClientService.GetInstance(conn1))
{
    s1.ChangeDataBase("Demo1");
    using (SQLServerClientService s2 = SQLServerClientService.GetInstance(conn1))
    {
      // Connect Success
    }
}
```

1. Insert
```C#
Data d = new Data(){ Age = 1 };
s1.InsertItem<Data>("Data", d);

// or just a Dictionry.

Dictionary<string, object> d = new Dictionary<string, object>();
s1.InsertItemDict("Data", d);
```

2. SELECT && Update && Delete
```C#
s1.GetItemDict("Data", filter, new List<string>() { "Age", "Name" });

FilterCondition filter = new FilterCondition("Age", TableCompareType.EQ, 20);
s1.UpdateItemDict("Data", filter, "ID", Guid.NewGuid());

s1.RemoveItem<Data>("Data", filter);
```

3. Batch Insert 
```C#
s1.BulkInsertItemList<Data>("Data", List<Data> data);
s1.BulkInsertItemListDict("Data", List<Dictionary<string, object>> data);

s1.InsertItemList("Data", List<Dictionary<string, object>> data);
```

## Support
|    DataBase              | CRUD | Paging   | Batch Insert   | Bulk Insert | 
|:-------------------------|:-----|:---------|:---------------|:------------|
|`SQLite`                  |√     |√         |√               |✗            |
|`MySQL`                   |√     |√         |√               |✗            |
|`SQLServer`               |√     |√         |√               |√            |
|`Redis`                   |√     |√         |√               |✗            |
|`MongoDB`                 |√     |√         |√               |✗            |

## Paging
```C#
FilterCondition filter = new FilterCondition("Age", TableCompareType.GTE, 20);
PageCondition page = new PageCondition(1, 20);

Dictionary<string, object> d = s1.GetItemList<Data>("Data", filter, ref page);
```

## Transaction

Transaction automatic begin when use `GetInstance()` Except `Redis`.
`Redis` not support transaction, libaray will push operation to a Queue first, 
and popup execute command after you call `Commit()`.

```C#
Dictionary<string, object> d = new Dictionary<string, object>();
s1.SetItem("S1", d["ID"].ToString(), d);
s1.Commit();
```

## MySQL LoadCsv
```C#
s1.BulkLoadFromFile("Data", "D:/tmp.csv", "`");
```

## Create Script
in SQLServer, use `GetCreateScript(name, true)` to export a table with index.

## Notice
1. use `[TableFields(true)]` in Entity, then libaray will set and find value automatic.

2. for `Mongodb`, add `public ObjectId _id { get; set; }` in property. and add 
`[BsonIgnoreExtraElements]` in class is needed.

3. You should configure `replication` before use transaction.
