using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CommonLib.TableBasePackage;
using CommonLib.Utils;
using Elasticsearch.Net;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CommonLib.DatabaseClient
{
    public class ELSearchResponse<T>
    {
        public ELHit<T> hits { get; set; }
    }

    public class ELTotalHits
    {
        public string relation { get; set; }
        public int value { get; set; }
    }

    public class ELHit<T>
    {
        public ELTotalHits total { get; set; }
        public List<ELSource<T>> hits { get; set; }
    }

    public class ELSource<T>
    {
        public T _source { get; set; }
    }

    public abstract class ABSElasticsearchTableBase
    {
    }

    public interface IElasticsearchBase
    {
    }

    public abstract class ElasticsearchBase : ABSElasticsearchTableBase, IDisposable
    {
        public IElasticClient conn = null;

        public ElasticsearchBase(List<Uri> connStrings)
        {
            conn = GetConnection(connStrings);
        }

        public IElasticClient GetConnection(List<Uri> connStrings)
        {
            SniffingConnectionPool connectionPool = new SniffingConnectionPool(connStrings);
            ConnectionSettings settings = new ConnectionSettings(connectionPool); //.EnableDebugMode();
            ElasticClient client = new ElasticClient(settings);
            return client;
        }

        public virtual void Dispose()
        {
        }

        public bool InsertItem<T>(string indexName, T data)
        {
            BytesResponse res = conn.LowLevel.Index<BytesResponse>(indexName, PostData.Serializable<T>(data));
            return res.Success;
        }

        public bool InsertItemList<T>(string indexName, List<T> data)
        {
            StringBuilder body = new StringBuilder();
            foreach(T d in data)
            {
                body.Append("{\"create\":{\"_index\" : \"" + indexName + "\"}}\n");
                body.Append(JsonConvert.SerializeObject(d));
                body.Append("\n");
            }
            
            BytesResponse res = conn.LowLevel.Bulk<BytesResponse>(PostData.String(body.ToString()));
            return res.Success;
        }

        public bool RemoveItem<T>(string indexName, List<FilterCondition> filter)
        {
            throw new NotImplementedException();
        }

        public bool RemoveItem<T>(string indexName, FilterCondition filter)
        {
            throw new NotImplementedException();
        }

        public bool RemoveItemList<T>(string indexName, List<FilterCondition> filter)
        {
            throw new NotImplementedException();
        }

        public bool RemoveAllItem<T>(string indexName)
        {
            throw new NotImplementedException();
        }

        public bool UpdateItem<T>(string indexName, FilterCondition filter, T data, string[] columns)
        {
            throw new NotImplementedException();
        }

        public bool UpdateItem<T>(string indexName, FilterCondition filter, string column, object value)
        {
            throw new NotImplementedException();
        }

        public T GetItem<T>(string indexName, FilterCondition filter, List<string> columns = null)
        {
            PageCondition page = new PageCondition(1, 1);
            string body = FilterToQueryString(indexName, filter, ref page);
            if(body == null) { return default(T); }

            StringResponse res = conn.LowLevel.Search<StringResponse>(PostData.String(body));
            if (!res.Success) { return default(T); }

            ELSearchResponse<T> sr = JsonConvert.DeserializeObject<ELSearchResponse<T>>(res.Body);
            if (sr == null || sr.hits.total.value == 0) { return default(T); }
            T data = sr.hits.hits[0]._source;

            return data;
        }

        public List<T> GetItemList<T>(string indexName, FilterCondition filter, ref PageCondition page, List<string> columns = null)
        {
            string body = FilterToQueryString(indexName, filter, ref page);
            StringResponse res = conn.LowLevel.Search<StringResponse>(PostData.String(body));
            if (!res.Success) { return null; }

            ELSearchResponse<T> sr = JsonConvert.DeserializeObject<ELSearchResponse<T>>(res.Body);
            if (sr == null) { return null; }
            List<T> data = sr.hits.hits.ConvertAll(d => d._source);
            page.Total = sr.hits.total.value;

            return data;
        }

        public List<T> GetAllItem<T>(string indexName, List<string> columns = null)
        {
            throw new NotImplementedException();
        }

        public int CountItemList<T>(string indexName, List<FilterCondition> filter)
        {
            throw new NotImplementedException("You can't use filter list, use single nested filter instead.");
        }

        // Inner Use
        public List<T> Query<T>(string command, object param = null)
        {
            throw new NotImplementedException();
        }

        public StringResponse Execute(JObject body, object param)
        {
            return Execute(body, param);
        }

        public StringResponse Execute(string body, Dictionary<string, object> param)
        {
            if (param == null || !param.ContainsKey("path") || !param.ContainsKey("method"))
            {
                return null;
            }

            string path = param["path"] as string;
            HttpMethod md = (HttpMethod)param["method"];

            StringResponse res = conn.LowLevel.DoRequest<StringResponse>(md, path, PostData.String(body));
            return res;
        }

        // Convert
        public string FilterToQueryString(string indexName, FilterCondition filter, ref PageCondition page)
        {
            JObject ctx = new JObject() {
                 { "from",  JToken.FromObject((page.PageNo - 1) * page.PageSize) },
                 { "size",  JToken.FromObject(page.PageSize) },
                 { "sort", new JArray() },
                 { "query", null }
            };
            ctx["query"] = FilterToQuery(ctx, filter);
            return ctx.ToString();
        }

        public JObject FilterToQuery(JObject ctx, FilterCondition filter)
        {
            JObject jr = new JObject();
            if (filter.CompareType == TableCompareType.STREE)
            {
                if (filter.Value is IEnumerable<FilterCondition>)
                {
                    JArray js = new JArray();
                    IEnumerable<FilterCondition> ls = filter.Value as IEnumerable<FilterCondition>;
                    foreach (FilterCondition f in ls)
                    {
                        JObject jn = FilterToQuery(ctx, f);
                        if (jn == null) { continue; }

                        js.Add(jn);
                    }
                    jr.Add(filter.Key, js);
                }
                else
                {
                    if (filter.Value == null)
                    {
                        jr.Add(filter.Key, null);
                    }
                    else
                    {
                        FilterCondition nf = (FilterCondition)filter.Value;
                        JObject jn = FilterToQuery(ctx, nf);

                        jr.Add(filter.Key, jn);
                    }
                }
            }
            else
            {
                jr = FilterToCompare(ctx, filter);
            }

            return jr;
        }

        public bool FilterToSort(JObject ctx, FilterCondition s)
        {
            JArray js = ctx["sort"] as JArray;
            if(js == null) { return false; }

            if (s.OrderType == TableOrderType.ASCENDING)
            {
                js.Add(new JObject() {{ s.Key, new JObject() {{
                        "order", JToken.FromObject("asc")
                }}}});
            }
            else if(s.OrderType == TableOrderType.DESCENDING)
            {
                js.Add(new JObject() {{ s.Key, new JObject() {{
                        "order", JToken.FromObject("desc")
                }}}});
            }

            return true;
        }

        public JObject FilterToCompare(JObject ctx, FilterCondition s)
        {
            JObject ja = new JObject();

            if(s.Key == null)
            {
                return ja;
            }

            if (!FilterToSort(ctx, s)) { return ja; }

            switch (s.CompareType)
            {
                case TableCompareType.EQ:
                    {
                        ja.Add("match", new JObject(){{ s.Key, JToken.FromObject(s.Value) }});
                        break;
                    }
                case TableCompareType.GT:
                    {
                        ja.Add("range", new JObject() {{ s.Key,
                                new JObject() {{ "gt", JToken.FromObject(s.Value) }}
                        }});
                        break;
                    }
                case TableCompareType.GTE:
                    {
                        ja.Add("range", new JObject() {{ s.Key,
                                new JObject() {{ "gte", JToken.FromObject(s.Value) }}
                        }});
                        break;
                    }
                case TableCompareType.LT:
                    {
                        ja.Add("range", new JObject() {{ s.Key,
                                new JObject() {{ "lt", JToken.FromObject(s.Value) }}
                        }});
                        break;
                    }
                case TableCompareType.LTE:
                    {
                        ja.Add("range", new JObject() {{ s.Key,
                                new JObject() {{ "lte", JToken.FromObject(s.Value) }}
                        }});
                        break;
                    }
                case TableCompareType.NE:
                    {
                        ja.Add("bool", new JObject() {{ "must_not", new JObject() {{ "term",
                                    new JObject() {{ s.Key, JToken.FromObject(s.Value) }}
                                }}
                            }});
                        break;
                    }
                case TableCompareType.IN:
                    {
                        ja.Add("bool", new JObject() {{ "must", new JObject() {{ "term",
                                    new JObject() {{ s.Key, JToken.FromObject(s.Value) }}
                                }}
                            }});
                        break;
                    }
                case TableCompareType.LIKE:
                    {
                        ja.Add("bool", new JObject() {{ "must", new JObject() {{ "wildcard",
                                    new JObject() {{ s.Key, JToken.FromObject(s.Value) }}
                                }}
                            }});
                        break;
                    }
                default:
                    {
                        return null;
                    }
            }

            return ja;
        }
    }

    public abstract class ElasticsearchClientBase : ElasticsearchBase, IElasticsearchBase
    {
        public ElasticsearchClientBase(List<Uri> connStrings) : base(connStrings)
        {
        }
    }
}
