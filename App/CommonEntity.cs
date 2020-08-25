using CommonLib.TableData;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace App.Entity
{
    [BsonIgnoreExtraElements]
    public class BaseData
    {
        public ObjectId _id { get; set; }
    }

    [TableDecorator("Data")]
    public class Data : BaseData
    {
        [TableFields(true)]
        public Guid ID { get; set; }

        [TableFields(true)]
        public string Name { get; set; }

        [TableFields(true)]
        public string Remark { get; set; }

        [TableFields(true)]
        public int Age { get; set; }

        public string Extern { get; set; }
    }

    [BsonIgnoreExtraElements]
    public class WebData : BaseData
    {
        [TableFields(true)]
        public Guid ID { get; set; }

        [TableFields(true)]
        public string Remark { get; set; }
    }

    [TableDecorator("Data")]
    public class Data2 : Data
    {
        public int DataID2 { get; set; }
        public string Alias { get; set; }
    }
}
