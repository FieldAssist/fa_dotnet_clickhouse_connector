using System.Data;

namespace ClickHouseConnector.Models
{
    public class QueryParameter
    {
        public string Name { get; set; }
        public object Value { get; set; }
        public DbType DataType { get; set; }

        public QueryParameter(string name, object value, DbType dataType)
        {
            Name = name;
            Value = value;
            DataType = dataType;
        }
    }
}
