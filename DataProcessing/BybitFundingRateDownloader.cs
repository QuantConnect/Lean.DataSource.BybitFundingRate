/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using QuantConnect.Logging;
using QuantConnect.Util;

namespace QuantConnect.DataProcessing;

/// <summary>
/// BybitFundingRateDownloader implementation
/// </summary>
public class BybitFundingRateDownloader : IDisposable
{
    private const string BybitApiEndpoint = "https://api.bybit.com";

    private static readonly JsonSerializerSettings SerializerSettings = new()
    {
        ContractResolver = new CamelCasePropertyNamesContractResolver()
    };

    private readonly DateTime? _deploymentDate;
    private readonly string _destinationFolder;
    private readonly string _existingInDataFolder;

    /// <summary>
    /// Control the rate of download per unit of time.
    /// </summary>
    private readonly RateGate _indexGate = new(10, TimeSpan.FromSeconds(1));

    private readonly BybitInstrumentInfo[] _perpetualFuturesExchangeInfos;

    /// <summary>
    /// Creates a new instance of <see cref="BybitFundingRateDownloader"/>
    /// </summary>
    /// <param name="destinationFolder">The folder where the data will be saved</param>
    /// <param name="deploymentDate"></param>
    public BybitFundingRateDownloader(string destinationFolder, DateTime? deploymentDate)
    {
        _deploymentDate = deploymentDate;
        _destinationFolder = Path.Combine(destinationFolder, "cryptofuture", "bybit", "margin_interest");
        _existingInDataFolder = Path.Combine(Globals.DataFolder, "cryptofuture", "bybit", "margin_interest");

        Directory.CreateDirectory(_destinationFolder);

        _perpetualFuturesExchangeInfos = GetExchangeInfo()
            .Where(x => x.ContractType.EndsWith("Perpetual", StringComparison.InvariantCultureIgnoreCase))
            .ToArray();
    }


    /// <summary>
    /// Runs the instance of the object.
    /// </summary>
    /// <returns>True if process all downloads successfully</returns>
    public bool Run()
    {
        var ratePerSymbol = new Dictionary<string, Dictionary<DateTime, decimal>>();
        using HttpClient client = new();
        foreach (var date in GetProcessingDates())
        {
            foreach (var apiFundingRate in GetData(date, client))
            {
                var fundingTimestamp = long.Parse(apiFundingRate.FundingRateTimestamp, CultureInfo.InvariantCulture);
                var fundingTime = Time.UnixMillisecondTimeStampToDateTime(fundingTimestamp);
                if (!ratePerSymbol.TryGetValue(apiFundingRate.Symbol, out var dictionary))
                {
                    ratePerSymbol[apiFundingRate.Symbol] = dictionary = new();
                }

                var key = new DateTime(fundingTime.Year, fundingTime.Month, fundingTime.Day, fundingTime.Hour,
                    fundingTime.Minute, fundingTime.Second);
                dictionary[key] = decimal.Parse(apiFundingRate.FundingRate, CultureInfo.InvariantCulture);
            }
        }

        foreach (var kvp in ratePerSymbol)
        {
            // USDC pairs have PERP in the name instead of USDC
            SaveContentToFile(_destinationFolder, kvp.Key.Replace("PERP", "USDC"), kvp.Value);
        }

        return true;
    }

    private IEnumerable<BybitFundingRate> GetData(DateTime date, HttpClient client)
    {
        var start = (long)Time.DateTimeToUnixTimeStampMilliseconds(date.Date);
        var end = (long)Time.DateTimeToUnixTimeStampMilliseconds(date.AddDays(1).Date);

        var result = new List<BybitFundingRate>();

        Parallel.ForEach(_perpetualFuturesExchangeInfos.Where(x => x.LaunchTimestamp <= end), exchangeInfo =>
        {
            _indexGate.WaitToProceed();
            var url =
                $"{BybitApiEndpoint}/v5/market/funding/history?limit=200&symbol={exchangeInfo.Symbol}&startTime={start}&endTime={end}&category={exchangeInfo.Category}";
            var data = client.DownloadData(url);

            lock (result)
            {
                try
                {
                    var response = JsonConvert
                        .DeserializeObject<ByBitResponse<BybitListResult<BybitFundingRate>>>(data, SerializerSettings);
                    result.AddRange(response.Result.List);
                }
                catch (Exception)
                {
                    Log.Error($"GetData(): deserialization failed {data}");
                    throw;
                }
            }
        });
        return result;
    }


    private IEnumerable<DateTime> GetProcessingDates()
    {
        if (_deploymentDate.HasValue)
        {
            return new[] { _deploymentDate.Value };
        }
        else
        {
            // everything
            return Time.EachDay(new DateTime(2019, 11, 14), DateTime.UtcNow.Date);
        }
    }

    /// <summary>
    /// Saves contents to disk, deleting existing zip files
    /// </summary>
    /// <param name="destinationFolder">Final destination of the data</param>
    /// <param name="name">file name</param>
    /// <param name="contents">Contents to write</param>
    private void SaveContentToFile(string destinationFolder, string name, Dictionary<DateTime, decimal> contents)
    {
        name = name.ToLowerInvariant();
        var finalPath = Path.Combine(destinationFolder, $"{name}.csv");
        var existingPath = Path.Combine(_existingInDataFolder, $"{name}.csv");

        if (File.Exists(existingPath))
        {
            foreach (var line in File.ReadAllLines(existingPath))
            {
                if (string.IsNullOrEmpty(line))
                {
                    continue;
                }

                var parts = line.Split(',');
                if (parts.Length == 1)
                {
                    continue;
                }

                var time = DateTime.ParseExact(parts[0], "yyyyMMdd HH:mm:ss", CultureInfo.InvariantCulture,
                    DateTimeStyles.None);
                var rate = decimal.Parse(parts[1], NumberStyles.Any, CultureInfo.InvariantCulture);
                if (!contents.ContainsKey(time))
                {
                    // use existing unless we have a new value
                    contents[time] = rate;
                }
            }
        }

        var finalLines = contents.OrderBy(x => x.Key)
            .Select(x => $"{x.Key:yyyyMMdd HH:mm:ss},{x.Value.ToStringInvariant()}").ToList();

        var tempPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.tmp");
        File.WriteAllLines(tempPath, finalLines);
        var tempFilePath = new FileInfo(tempPath);
        tempFilePath.MoveTo(finalPath, true);
    }

    private IEnumerable<BybitInstrumentInfo> GetExchangeInfo()
    {
        return GetExchangeInfo("linear").Concat(GetExchangeInfo("inverse"));
    }

    private IEnumerable<BybitInstrumentInfo> GetExchangeInfo(string category)
    {
        string pageCursor, nextPageCursor = null;
        do
        {
            pageCursor = nextPageCursor;

            var url = $"{BybitApiEndpoint}/v5/market/instruments-info?limit=1000&status=Trading&category={category}";
            if (!string.IsNullOrEmpty(pageCursor))
            {
                url += $"&cursor={pageCursor}";
            }

            _indexGate.WaitToProceed();
            var data = url.DownloadData();
            var response = JsonConvert
                .DeserializeObject<ByBitResponse<BybitListResult<BybitInstrumentInfo>>>(data, SerializerSettings);

            foreach (var exchangeInfo in response.Result.List)
            {
                exchangeInfo.Category = response.Result.Category;
                exchangeInfo.LaunchTimestamp = decimal.Parse(exchangeInfo.LaunchTime, CultureInfo.InvariantCulture);

                yield return exchangeInfo;
            }

            nextPageCursor = response.Result.NextPageCursor;
        } while (!string.IsNullOrEmpty(nextPageCursor) && pageCursor != nextPageCursor);
    }

    /// <summary>
    /// Disposes of unmanaged resources
    /// </summary>
    public void Dispose()
    {
        _indexGate.Dispose();
    }


    /// <summary>
    /// Funding rate
    /// </summary>
    public class BybitFundingRate
    {
        /// <summary>
        /// Symbol name
        /// </summary>
        [JsonProperty("symbol")]
        public string Symbol { get; set; }

        /// <summary>
        /// Funding rate
        /// </summary>
        [JsonProperty("fundingRate")]
        public string FundingRate { get; set; }

        /// <summary>
        /// Funding rate timestamp
        /// </summary>
        [JsonProperty("fundingRateTimestamp")]
        public string FundingRateTimestamp { get; set; }
    }

    /// <summary>
    /// Bybits default http response message
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ByBitResponse<T>
    {
        /// <summary>
        /// Success/Error code
        /// </summary>
        [JsonProperty("retCode")]
        public int ReturnCode { get; set; }

        /// <summary>
        /// Success/Error msg. OK, success, SUCCESS indicate a successful response
        /// </summary>
        [JsonProperty("retMsg")]
        public string ReturnMessage { get; set; }


        /// <summary>
        /// Extend info. Most of the time, it is <c>{}</c>
        /// </summary>
        [JsonProperty("retExtInfo")]
        public object ExtendedInfo { get; set; }

        /// <summary>
        /// Business data result
        /// </summary>
        [JsonProperty("result")]
        public T Result { get; set; }

        /// <summary>
        /// Current time
        /// </summary>
        [JsonProperty("time")]
        public long Timestamp { get; set; }
    }

    /// <summary>
    /// Bybit business data wrapper for array results
    /// </summary>
    /// <typeparam name="T">The business object type</typeparam>
    public class BybitListResult<T>
    {
        /// <summary>
        /// Product category
        /// </summary>
        public string Category { get; set; }

        /// <summary>
        /// The result items
        /// </summary>
        public T[] List { get; set; }


        /// <summary>
        /// Cursor used for pagination
        /// </summary>
        public string NextPageCursor { get; set; }
    }

    /// <summary>
    /// Instrument info
    /// </summary>
    public class BybitInstrumentInfo
    {
        /// <summary>
        /// Symbol name
        /// </summary>
        public string Symbol { get; set; }

        /// <summary>
        /// Contract type
        /// </summary>
        public string ContractType { get; set; }

        /// <summary>
        /// Launch time
        /// </summary>
        public string LaunchTime { get; set; }

        /// <summary>
        /// Launch timestamp (ms)
        /// </summary>
        [JsonIgnore]
        public decimal LaunchTimestamp { get; set; }

        /// <summary>
        /// Product category
        /// </summary>
        [JsonIgnore]
        public string Category { get; set; }
    }
}