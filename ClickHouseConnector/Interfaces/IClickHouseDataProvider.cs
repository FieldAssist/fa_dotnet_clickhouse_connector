using ClickHouseConnector.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClickHouseConnector.Interfaces
{
    public interface IClickHouseDataProvider
    {
        Task<DataTable> FetchDataAsync(string queryString,
            IEnumerable<QueryParameter> parameters = null);
    }
}
