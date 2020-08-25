#CSharpDataLib

简化数据库逻辑处理过程，抽象几个基本操作和数据操作。
如果你c#的工具需要操作数据库，可以试试这个程序。

## 快速预览

```C#
using (SQLServerClientService sqlServer = new SQLServerClientService())
{
  PageCondition page = new PageCondition(1, 100);
  List<FilterCondition> filter = new List<FilterCondition>() {
    new FilterCondition("Age", TableCompareType.LT, 200, orderType: TableOrderType.DESCENDING)
  };

  Guid ID = Guid.NewGuid();
  Data d2 = new Data() { Age = 1, ID = ID, Name = "Name1", Remark = "Remark1" };
  sqlServer.InsertItem<Data>("Data", d2);
  sqlServer.InsertItemList<Data>("Data", new List<Data>() { d2 });
  sqlServer.BulkInsertItemList<Data>("Data", new List<Data>() { d2 });

  sqlServer.RemoveItem<Data>("Data", "Age", "1");
  sqlServer.RemoveItemList<Data>("Data", filter);

  sqlServer.GetItemList<Data>("Data", filter);
  sqlServer.GetItemList<Data>("Data", filter, page);
  sqlServer.GetItem<Data>("Data", "Age", "1");

  sqlServer.UpdateItem<Data>("Data", "ID", ID.ToString(), d2, new string[] { "Age" });

  sqlServer.Commit();
}
```

## 支持

|    数据库                | CRUD | 分页   | 批量插入    | 大批量插入 | 
|:-------------------------|:-----|:-------|:------------|:-----------|
|`SQLite`                  |√     |√       |√            |✗           |
|`MySQL`                   |√     |√       |√            |✗           |
|`SQLServer`               |√     |√       |√            |√           |
|`Redis`                   |√     |√       |√            |✗           |
|`MongoDB`                 |√     |√       |√            |✗           |

## Redis事务

Redis本身不支持事务，有`Multi`指令，也是批处理类似的语句，所以这里使用队列实现事务，
当Redis插入数据时，添加一个`Action`到队列里面，当其他操作执行完成后，可以使用Commit(),
一次性执行整个队列的插入删除操作。

所以，需要注意，当没有`commit`时，操作不会执行，`GetItemList`得到的结果不是最新的数据。
`RollBack`的时候将会清空队列，或自动在释放的时候清空。

## MySQL批量导入

如果已有`csv`文件导入，可以使用`BulkLoadFromFile`, 也可以使用`BulkDataToFile`导出数据到文件。
`MySQL`有一个`max_allowed_packet`参数，默认为`4194304`, 在使用`InsertItemList`请注意调整大小。

## 分页

`SQLite`, `MySQL`, `SQLServer` 均使用`SQL`实现, 数据库均支持，查询时注意索引。
`Redis`数据库由于`ZSet`本身插入得输入`score`，本身支持度很有限，所以这里实现分页使用的是内存分页,
查询时请注意内存使用。

## 注意事项

1. 开发使用了一些反射，通过在实体上面添加`[TableFields(true)]`这样的语句找到对应的字段，
所以需要修改实体才可以实现，请参考`App.Entity`下面的内容。

2. 关于`MongoDB`, 如果出现获取数据出错，需要添加`public ObjectId _id { get; set; }`字段，
并在类上添加`[BsonIgnoreExtraElements]`属性，以防止字段和数据库不完全对应而报错。

3. 如果使用`MongoDB`，请先配置"复制集"，否则无法使用`MongoDB`的事务。
