// Copyright (c) FieldAssist. All Rights Reserved.

using System.Data;
using ClickHouseConnector.Models;

namespace ClickHouseConnector.Interfaces
{
    public interface IClickHouseDataProvider
    {
        Task<DataTable> FetchDataAsync(string queryString,
            IEnumerable<QueryParameter> parameters = null);
    }
}
