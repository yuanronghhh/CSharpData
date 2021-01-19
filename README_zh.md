#CSharpDataLib

一个简单的`C#`数据操作工具库, 对于简化数据操作非常有用。

## 介绍

1. 连接数据库
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

## 支持
|    DataBase              | CRUD | Paging   | Batch Insert   | Bulk Insert | 
|:-------------------------|:-----|:---------|:---------------|:------------|
|`SQLite`                  |√     |√         |√               |✗            |
|`MySQL`                   |√     |√         |√               |✗            |
|`SQLServer`               |√     |√         |√               |√            |
|`Redis`                   |√     |√         |√               |✗            |
|`MongoDB`                 |√     |√         |√               |✗            |

## 分页
```C#
FilterCondition filter = new FilterCondition("Age", TableCompareType.GTE, 20);
PageCondition page = new PageCondition(1, 20);

Dictionary<string, object> d = s1.GetItemList<Data>("Data", filter, ref page);
```

## 事务
除了`Redis`外， 事务在调用`GetInstance`时已经开启，调用`Commit()`和`RollBack()`即可
`Redis`不支持事务，所以先将数据操作放入队列，当`Commit()`的时候才一起执行。

```C#
Dictionary<string, object> d = new Dictionary<string, object>();
s1.SetItem("S1", d["ID"].ToString(), d);
s1.Commit();
```

## MySQL 导入Cvs文件
```C#
s1.BulkLoadFromFile("Data", "D:/tmp.csv", "`");
```

## SQLServer表创建脚本
在`SQLServer`中, 使用`GetCreateScript(name, true)`即可导出带索引的脚本。

## 注意
1. 在实体上使用 `[TableFields(true)]`, 库才能自动匹配数据库字段。

2. 对于`Mongodb`, 需要在实体上添加`public ObjectId _id { get; set; }`然后在类上
添加`[BsonIgnoreExtraElements]`以避免报错。

3. `Mongodb` 需要先配置`复制集`才能使用事务。
