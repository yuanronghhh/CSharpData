using System;
using CommonLib.TableData;

namespace CommonLibTest.Entity
{
    [TableName("Data")]
    public class Data
    {
        [DatabaseFields(true)]
        public Guid ID { get; set; }

        [DatabaseFields(true)]
        public string Name { get; set; }

        [DatabaseFields(true)]
        public int Age { get; set; }

        public string Remark { get; set; }
        [DatabaseFields(true)]
        public string Aliase { get; set; }
    }
}
