namespace ReportRetriever;

using System.Net.Http.Headers;
using System.Collections.Concurrent;
using System.Data.OleDb;
using System.Runtime.Versioning;
using System.Text.RegularExpressions;
using System.Globalization;
public enum ReportType
{
    Legacy, Disaggregated, TFF
}
public enum ReportStatusCode
{
    NoUpdateAvailable, Updated, Failure, AttemptingRetrieval, AttemptingUpdate, NotInitialized, CheckingDataAvailability
}

[SupportedOSPlatform("windows")]
public partial class Report
{
    /// <summary>
    /// Gets or sets an enum that represnts the current state of the instance.
    /// </summary>
    public ReportStatusCode CurrentStatus
    {
        get => _currentStatus;
        private set
        {
            if (IsLegacyCombined) s_retrievalLockingStatusCode = value;
            _currentStatus = value;
        }
    }

    /// <summary>
    /// Backing field for <see cref="CurrentStatus"/> .
    /// </summary>
    private ReportStatusCode _currentStatus;

    /// <summary>
    /// Standardized field name used for C.O.T date fields.
    /// </summary>
    private const string StandardDateFieldName = "report_date_as_yyyy_mm_dd";

    /// <summary>
    /// Standardized field name for C.O.T contract code fields.
    /// </summary>
    private const string ContractCodeColumnName = "cftc_contract_market_code";

    /// <summary>
    /// Standardized date format used to parse CFTC dates or to convert ICE dates.
    /// </summary>
    private const string StandardDateFormat = "yyyy-MM-ddTHH:mm:ss.fff";

    /// <summary>
    /// DateTime returned when an instance for Legacy_Combined data queries the database for the most recent date.
    /// </summary>
    /// <remarks>Assigned a value in <see cref="RetrieveDataAsync"/></remarks>
    private static DateTime s_ceilingDateForRetrievalPermission;

    /// <summary>
    /// Dictionary used to store downloaded ICE COT data regardless of <see cref="RetrieveCombinedData"/>'s value.
    /// </summary>
    private static readonly ConcurrentDictionary<string, Task<Dictionary<DateTime, List<string[]>>>?> s_iceCsvRawData = new();

    /// <summary>
    /// Dictionary used to map ICE column names to their respective indexes within the Disaggregated database.
    /// </summary>
    private static Dictionary<string, FieldInfo?>? s_iceColumnMap = null;

    /// <summary>
    /// Dictionary of connection objects used for each report type.
    /// </summary>
    private static readonly Dictionary<ReportType, OleDbConnection> s_oleDbConnectionsByReportType = new();

    /// <summary>
    /// Gets a <see cref="OleDbConnection"/> object that correlates to this instance's <see cref="QueriedReport"/>.
    /// </summary>
    private OleDbConnection DatabaseConnection => s_oleDbConnectionsByReportType[QueriedReport];

    /// <summary>
    /// Number used to count how many threads are attempting to retrieve ICE data.
    /// </summary>
    private static int s_activeIceRetrievalCount = 0;

    /// <summary>
    /// Static variable to track changes done to LEgacy_Combined instances.
    /// </summary>
    /// <remarks>Used to escape waiting loop in RetrieveDataAsync for non Legacy Combined data.</remarks>
    private static ReportStatusCode s_retrievalLockingStatusCode = ReportStatusCode.NotInitialized;

    /// <summary>
    /// Date before the inception of the Commitments of Traders Report.
    /// </summary>
    static readonly DateTime s_defaultStartDate = new(1970, 1, 1);

    /// <summary>
    /// HttpClient used to query C.O.T API data.
    /// </summary>
    static readonly HttpClient s_cftcApiClient = new() { BaseAddress = new Uri("https://publicreporting.cftc.gov/resource/") };

    /// <summary>
    /// Connection string used to interface with SQL Server.
    /// </summary> 
    private string DatabaseConnectionString => "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + DatabasePath + ';';

    /// <summary>
    ///  Gets a boolean value that represents if the current instance is Legacy Combined data.
    /// </summary>
    /// <value><see langword="true"/> if <see cref="QueriedReport"/> equals <see cref="ReportType.Legacy"/> and <see cref="RetrieveCombinedData"/> equals <see langword="true"/>; otherwise, <see langword="false"/>.</value>
    public bool IsLegacyCombined { get; }

    /// <summary>
    /// Identification code used to target the relevant dataset.
    /// </summary>
    private readonly string _cotApiCode;

    /// <summary>
    /// Gets a <see cref="ReportType"/> enum that specifies what C.O.T type to target.
    /// </summary>
    public ReportType QueriedReport { get; }

    /// <summary>
    /// Gets a table name based on instance variables found within <see cref="DatabasePath"/>.
    /// </summary>
    private string SqlTableDataName { get; }

    /// <summary>
    /// The largest date contained within <see cref="DatabasePath"/> before data is retrieved from the api.
    /// </summary>
    public DateTime DataBaseDateBeforeUpdate { get; private set; }

    /// <summary>
    /// Date used to keep track of the largest date contained retrieved from a CFTC data query.
    /// </summary>
    public DateTime DatabaseDateAfterUpdate { get; private set; }

    /// <summary>
    /// Boolean that represents whether the current instance is awaiting price updates.
    /// </summary>
    /// <value><see langword="true"/> if conditions have been met to allow for price updating; otherwise, <see langword="false"/>.</value>
    public bool AwaitingPriceUpdate => _waitingForPriceUpdate && !IsLegacyCombined && SuccessfullyUpdated;

    /// <summary>
    /// Gets a boolean that represents if the instance has successfully uploaded data to a database.
    /// </summary>
    public bool SuccessfullyUpdated => CurrentStatus == ReportStatusCode.Updated;

    /// <summary>
    /// Field used to help determine if <see cref="AwaitingPriceUpdate"/> should return <see langword="true"/>.
    /// </summary>
    private bool _waitingForPriceUpdate = false;

    /// <summary>
    /// Boolean that specifies if the current instance is tied to Futures + Options data.
    /// </summary>
    /// <value><see langword="true"/> if instance is dedicated to Futures + Options data; otherwise, <see langword="false"/> if designated for Futures only.</value>
    public bool RetrieveCombinedData { get; }

    /// <summary>
    /// Gets a path to a database C.O.T database related to this instance's <see cref="QueriedReport"/> value.
    /// </summary>
    public string DatabasePath { get; }

    /// <summary>
    /// Boolean used to toggle off or limit certain functionalities in this instance
    /// </summary>
    private bool DebugActive { get; }

    /// <summary>
    /// Timer used to time how long it takes to retrieve and upload data.
    /// </summary>
    public readonly System.Diagnostics.Stopwatch ActionTimer = new();

    /// <summary>
    /// Dictionary used to access specific APIs related to COT data and keyed to whether or not Futures + Options data is wanted.
    /// </summary>
    private static readonly Dictionary<bool, Dictionary<ReportType, string>> s_apiIdentification = new()
    {
        [true] = new Dictionary<ReportType, string>()
        {
            {ReportType.Legacy,"jun7-fc8e"},
            {ReportType.Disaggregated,"kh3c-gbw2"},
            {ReportType.TFF,"yw9f-hn96"}
        },
        [false] = new Dictionary<ReportType, string>()
        {
            {ReportType.Legacy,"6dca-aqww"},
            {ReportType.Disaggregated,"72hh-3qpy"},
            {ReportType.TFF,"gpe5-46if"}
        }
    };

    /// <summary>
    /// An array containing a white space and quote character.
    /// </summary>
    private readonly static char[] s_charactersToTrim = { ' ', '\"' };

    public readonly struct FieldInfo
    {
        /// <summary>
        /// Column Index of the given field within an array of data.
        /// </summary>
        public int ColumnIndex { get; }
        /// <summary>
        /// OleDbType assigned to the field based on the fields <see cref="EditedColumnName"/> property. 
        /// </summary>
        public OleDbType ColumnType { get; }
        /// <summary>
        /// Name of the field within the database.
        /// </summary>
        public string ColumnName { get; }
        /// <summary>
        /// Edited version of <see cref="ColumnName"/> 
        /// </summary>
        public string EditedColumnName { get; }
        public string DatabaseAdjustedName => $"[{ColumnName}]";

        public FieldInfo(int columnIndex, string columnName, string editedColumnName)
        {
            ColumnIndex = columnIndex;
            ColumnName = columnName;
            EditedColumnName = editedColumnName;
            ColumnType = CotFieldType(editedColumnName);
        }

        /// <summary>
        /// Returns a <see cref="OleDbType"/> based on text within <paramref name="editedDatabaseHeader"/>.
        /// </summary>
        /// <param name="editedDatabaseHeader">String used to determine what value should be returned.</param>
        /// <returns>An <see cref="OleDbType"/> that corresponds with the given header.</returns>
        static private OleDbType CotFieldType(string editedDatabaseHeader)
        {
            var intDesignator = new string[] { "all", "old", "other", "trader", "yymmdd" };
            return editedDatabaseHeader switch
            {
                string a when a.Contains("yyyy", StringComparison.InvariantCultureIgnoreCase) => OleDbType.Date,
                string b when b.Contains("pct", StringComparison.InvariantCultureIgnoreCase) || b.Contains("conc", StringComparison.InvariantCultureIgnoreCase) => OleDbType.Decimal,
                string c when intDesignator.Any(x => c.Contains(x, StringComparison.InvariantCultureIgnoreCase)) => OleDbType.Integer,
                string d when d.Equals("price", StringComparison.InvariantCultureIgnoreCase) => OleDbType.Currency,
                _ => OleDbType.VarChar
            };
        }
    }

    /// <summary>
    /// Initializes a new instance of the Report class with the specified properties.
    /// </summary>
    /// <param name="queriedReport">A ReportType enum used to specify what sort of data should be retrieved with this instance.</param>
    /// <param name="retrieveCombinedData"><see langword="true"/> if Futures + Options data should be filtered for; otherwise, <see langword="false"/> for Futures Only data.</param>
    /// <param name="databasePath">File path to database that data should be stored in.</param>
    /// <exception cref="FileNotFoundException">Thrown if <paramref name="databasePath"/> cannot be found.</exception>
    public Report(ReportType queriedReport, bool retrieveCombinedData, string databasePath, string tableName, bool useDebugMode = false)
    {
        if (!File.Exists(databasePath)) throw new FileNotFoundException($"{databasePath} wasn't found.");

        QueriedReport = queriedReport;
        RetrieveCombinedData = retrieveCombinedData;
        DatabasePath = databasePath;
        SqlTableDataName = tableName;
        DebugActive = useDebugMode;
        IsLegacyCombined = queriedReport == ReportType.Legacy && retrieveCombinedData;
        _cotApiCode = s_apiIdentification[retrieveCombinedData][queriedReport];

        s_oleDbConnectionsByReportType.TryAdd(queriedReport, new OleDbConnection(DatabaseConnectionString));
    }

    /// <summary>
    /// Retrieves related data from api and uploads to a local database if new data is retrieved.
    /// </summary>
    /// <param name="priceSymbolByContractCode">Dictionary of price symbols keyed to cftc contract codes.</param>
    /// <param name="testUpload">If <see langword="true"/> and <see cref="DebugActive"/> is <see langword="true"/> then data upload will be tested.</param>
    /// <returns><see langword="true"/> if no errors are generated; otherwise, <see langword="false"/>.</returns>
    /// <remarks>Price data will only be retrieved if <see cref="IsLegacyCombined"/> is <see langword="true"/>.</remarks>
    /// <exception cref="HttpRequestException">Thrown if unable to connecto to API service.</exception>
    /// <exception cref="KeyNotFoundException">Thrown if a necessary key for data upload wasn't found.</exception>
    /// <exception cref="NullReferenceException">Thrown if a null value is returned for a field necessary for data upload.</exception>
    /// <exception cref="OleDbException">Database error.</exception>
    /// <exception cref="IndexOutOfRangeException"></exception>
    /// <exception cref="ArgumentException"></exception>
    public async Task<bool> CommitmentsOfTradersRetrievalAndUploadAsync(Dictionary<string, string>? priceSymbolByContractCode, bool testUpload = false)
    {
        if (!DebugActive && testUpload) throw new ArgumentException($"Cannot test upload if {nameof(DebugActive)} is false.");

        ActionTimer.Start();
        DataBaseDateBeforeUpdate = DatabaseDateAfterUpdate = await ReturnLatestDateInTableAsync(filterForIce: false);
        // CurrentStatus is used as alocking flag for non legacy combined instances.
        CurrentStatus = ReportStatusCode.CheckingDataAvailability;
        if (!DebugActive)
        {
            if (!IsLegacyCombined)
            {
                // Loop until a change in state is detected in the running Legacy Combined instance.
                ActionTimer.Stop();
                while (s_retrievalLockingStatusCode == ReportStatusCode.CheckingDataAvailability)
                {
                    await Task.Delay(300);
                }

                var failureCodes = new ReportStatusCode[] { ReportStatusCode.NoUpdateAvailable, ReportStatusCode.Failure };
                if (failureCodes.Contains(s_retrievalLockingStatusCode) && DataBaseDateBeforeUpdate >= s_ceilingDateForRetrievalPermission)
                {
                    CurrentStatus = ReportStatusCode.NoUpdateAvailable;
                    return true;
                }
                ActionTimer.Start();
            }
            else
            {
                s_ceilingDateForRetrievalPermission = DataBaseDateBeforeUpdate;
            }
        }

        int queryReturnLimit = DebugActive ? 1 : 20_000;
        Dictionary<string, Dictionary<DateTime, string?>> priceByDateByContractCode = new();
        Dictionary<string, FieldInfo?> fieldInfoByEditedName = new();
        List<string>? databaseFieldNames = null;

        bool errorStatus = false;
        try
        {
            List<string[]> cftcData = await CftcCotRetrievalAsync(queryReturnLimit, fieldInfoByEditedName, priceByDateByContractCode, databaseFieldNames);
            List<string[]>? iceData = null;

            if (QueriedReport == ReportType.Disaggregated && cftcData.Any())
            {
                iceData = await IceCotRetrievalAsync(DatabaseDateAfterUpdate, databaseFieldNames!, queryReturnLimit);
            }

            if (cftcData.Any())
            {
                if (IsLegacyCombined && priceSymbolByContractCode != null && fieldInfoByEditedName.ContainsKey("price"))
                {
                    bool retrievePrices = true;
                    if (DebugActive)
                    {
                        Console.WriteLine("Do you want to test price retrieval(Y/N)?");
                        var keyResponse = Console.ReadKey(true);
                        if (keyResponse.Key != ConsoleKey.Y) retrievePrices = false;
                    }
                    if (retrievePrices) await RetrieveYahooPriceDataAsync(priceSymbolByContractCode, priceByDateByContractCode);
                }

                if (!DebugActive || testUpload)
                {
                    CurrentStatus = ReportStatusCode.AttemptingUpdate;

                    errorStatus = !UploadToDatabase(fieldInfoByEditedName!, cftcData, uploadingIceData: false, priceData: priceByDateByContractCode);
                    if (QueriedReport == ReportType.Disaggregated && iceData != null && iceData.Any())
                    {
                        errorStatus = !UploadToDatabase(s_iceColumnMap!, iceData, uploadingIceData: true);
                    }
                    if (!errorStatus) CurrentStatus = ReportStatusCode.Updated;
                }
            }
            if (errorStatus) CurrentStatus = ReportStatusCode.Failure;
        }
        catch (Exception)
        {
            CurrentStatus = ReportStatusCode.Failure;
            throw;
        }
        finally
        {
            ActionTimer.Stop();
        }

        return !errorStatus;
    }

    /// <summary>
    /// Retrieves CFTC C.O.T data from an API.
    /// </summary>
    /// <param name="maxRecordsPerLoop">Number used to limit the number of records retrieved from each loop attempt.</param>
    /// <param name="fieldInfoByEditedName">Empty dictionary to populate with FieldInfo instances.</param>
    /// <param name="priceByDateByContractCode">Used to store null values for wanted price data.</param>
    /// <param name="databaseFieldNames">List of headers from table in local database that data will be uploaded to.</param>
    /// <returns>An asynchronous task.</returns>
    /// <exception cref="FormatException"></exception>
    /// <exception cref="HttpRequestException"></exception>
    /// <exception cref="KeyNotFoundException"></exception>
    /// <exception cref="NullReferenceException"></exception>
    private async Task<List<string[]>> CftcCotRetrievalAsync(int maxRecordsPerLoop, Dictionary<string, FieldInfo?>? fieldInfoByEditedName, Dictionary<string, Dictionary<DateTime, string?>> priceByDateByContractCode, List<string>? databaseFieldNames)
    {
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types
        const string WantedDataFormat = ".csv", MimeType = "text/csv";
        int offsetCount = 0, totalRecordsToRetrieve = 0, recordsQueriedCount = 0;
        string[]? responseLines = null;

        try
        {
            s_cftcApiClient.DefaultRequestHeaders.Accept.Clear();
            s_cftcApiClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue(MimeType));
        }
        catch (InvalidOperationException)
        {// Properties can only be adjustted before a request has been made using s_cftcApiClient.
        }

        List<string[]> newCftcData = new();

        do
        {
            string? response;
            string comparisonOprator = DebugActive ? ">=" : ">";
            // Make api call to find out how many new records are available.
            // This section will only be run once.
            if (totalRecordsToRetrieve == 0)
            {
                var countRecordsUrl = $"{_cotApiCode}{WantedDataFormat}?$select=count(id)&$where=report_date_as_yyyy_mm_dd {comparisonOprator}'{DataBaseDateBeforeUpdate.ToString(StandardDateFormat)}'";
                response = await s_cftcApiClient.GetStringAsync(countRecordsUrl);

                totalRecordsToRetrieve = int.Parse(response.Split('\n')[1].Trim(s_charactersToTrim), NumberStyles.Number, null);

                if (totalRecordsToRetrieve == 0)
                {
                    CurrentStatus = ReportStatusCode.NoUpdateAvailable;
                    break;
                }
                else if (DebugActive)
                {
                    totalRecordsToRetrieve = maxRecordsPerLoop;
                }
                CurrentStatus = ReportStatusCode.AttemptingRetrieval;
            }
            string apiDetails = $"{_cotApiCode}{WantedDataFormat}?$where=report_date_as_yyyy_mm_dd{comparisonOprator}'{DataBaseDateBeforeUpdate.ToString(StandardDateFormat)}'&$order=id&$limit={maxRecordsPerLoop}&$offset={offsetCount++}";
            response = await s_cftcApiClient.GetStringAsync(apiDetails);

            // Parse each line of the response and ensure that its date is valid and wanted.
            if (response != null)
            {
                // Data from the API tends to have an extra line at the end so trim it.
                responseLines = response.Trim('\n').Split('\n');
                response = null;
                // Subtract 1 to account for headers.
                recordsQueriedCount += responseLines.Length - 1;

                if (fieldInfoByEditedName == null)
                {
                    databaseFieldNames = await QueryDatabaseFieldNamesAsync();
                    fieldInfoByEditedName = MapHeaderFieldsToDatabase(externalHeaders: SplitOnCommaNotWithinQuotesRegex().Split(responseLines[0]), databaseFields: databaseFieldNames, iceHeaders: false);
                }

                int cftcDateColumn = (int)fieldInfoByEditedName[StandardDateFieldName]?.ColumnIndex!;
                int cftcCodeColumn = (int)fieldInfoByEditedName[ContractCodeColumnName]?.ColumnIndex!;

                // Start index at 1 rather than 0 to skip over headers.
                for (var i = 1; i < responseLines.Length; ++i)
                {
                    if (!string.IsNullOrEmpty(responseLines[i]))
                    {
                        string[] apiRecord = SplitOnCommaNotWithinQuotesRegex().Split(responseLines[i]).Select(x => x.Trim(s_charactersToTrim)).ToArray();

                        if (DateTime.TryParseExact(apiRecord[cftcDateColumn], StandardDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime parsedDate)
                        && ((parsedDate > DataBaseDateBeforeUpdate && !DebugActive) || (parsedDate >= DataBaseDateBeforeUpdate && DebugActive)))
                        {   // Create a null entry for the current combination of contract code and date within priceByDateByContractCode.
                            if (IsLegacyCombined)
                            {
                                string currentContractCode = apiRecord[cftcCodeColumn];
                                if (!priceByDateByContractCode.TryGetValue(currentContractCode, out Dictionary<DateTime, string?>? priceByDateForContractCode))
                                {
                                    priceByDateForContractCode = priceByDateByContractCode[currentContractCode] = new();
                                }
                                priceByDateForContractCode.TryAdd(parsedDate, null);
                            }
                            newCftcData.Add(apiRecord);
                            if (parsedDate > DatabaseDateAfterUpdate) DatabaseDateAfterUpdate = parsedDate;
                        }
                    }
                }
                Array.Clear(responseLines);
            }
        } while (recordsQueriedCount < totalRecordsToRetrieve);

        return newCftcData;
    }

    /// <summary>
    /// Starts asynchronous tasks to download ICE contract data based on the value of <paramref name="mostRecentCftcDate"/>.
    /// </summary>
    /// <param name="mostRecentCftcDate">The date value of the most recent data from the CFTC</param>
    /// <param name="databaseFieldNames">Field names of the table data would be uploaded to.</param>
    /// <param name="debugReturnLimit">Number of rows to return if debugging this method.</param>
    /// <returns>An asynchronous task.</returns>
    private async Task<List<string[]>?> IceCotRetrievalAsync(DateTime mostRecentCftcDate, List<string> databaseFieldNames, int debugReturnLimit = 1)
    {
        DateTime maxIceDateInDatabase = await ReturnLatestDateInTableAsync(filterForIce: true);

        if (QueriedReport != ReportType.Disaggregated || (maxIceDateInDatabase >= mostRecentCftcDate && !DebugActive)) return null;

        const byte MaxDayDifference = 9;
        bool singleWeekRetrieval = (mostRecentCftcDate - maxIceDateInDatabase).Days <= MaxDayDifference || DebugActive;
        const string WeeklyIceKey = "Weekly_ICE";

        Interlocked.Increment(ref s_activeIceRetrievalCount);
        // nulls ae added instead of Tasks because the compiler will attempt to execute the task instead of immediately checking the key.
        if (singleWeekRetrieval && s_iceCsvRawData.TryAdd(WeeklyIceKey, null))
        {
            string iceCsvUrl = $"https://www.ice.com/publicdocs/cot_report/automated/COT_{mostRecentCftcDate:ddMMyyyy}.csv";
            s_iceCsvRawData.TryUpdate(WeeklyIceKey, QueryIceDataAsync(iceCsvUrl, databaseFieldNames, DebugActive), null);
        }
        else if (!singleWeekRetrieval)
        {
            const int IceStartYear = 2011;
            for (var csvYear = Math.Max(maxIceDateInDatabase.Year, IceStartYear); csvYear <= DatabaseDateAfterUpdate.Year; ++csvYear)
            {
                string iceCsvUrl = $"https://www.ice.com/publicdocs/futures/COTHist{csvYear}.csv";
                if (s_iceCsvRawData.TryAdd(iceCsvUrl, null))
                {
                    s_iceCsvRawData.TryUpdate(iceCsvUrl, QueryIceDataAsync(iceCsvUrl, databaseFieldNames, DebugActive), null);
                }
            }
        }
        Interlocked.Decrement(ref s_activeIceRetrievalCount);
        // Given this methods async properties, 2 instances may be attempting to download ICE data at the same time.
        while (s_activeIceRetrievalCount > 0)
        {
            await Task.Delay(300);
        }

        await Task.WhenAll(s_iceCsvRawData.Values!);
        int futOrCombinedColumn = (int)s_iceColumnMap!["futonly_or_combined"]?.ColumnIndex!;
        // Filter for data that corresponds witht he current instance and is more recent,
        var iceQuery = from kvp in s_iceCsvRawData
                       let weeklyTaskLocated = kvp.Key.Equals(WeeklyIceKey)
                       where ((singleWeekRetrieval && weeklyTaskLocated) || (!singleWeekRetrieval && !weeklyTaskLocated)) && !kvp.Value.IsFaulted
                       from recordsByDateTime in kvp.Value.Result
                       where (!DebugActive && recordsByDateTime.Key > maxIceDateInDatabase) || (DebugActive && recordsByDateTime.Key >= maxIceDateInDatabase)
                       from row in recordsByDateTime.Value
                       where row[futOrCombinedColumn].Equals(RetrieveCombinedData ? "combined" : "futonly", StringComparison.InvariantCultureIgnoreCase)
                       select row;

        return DebugActive ? iceQuery.Take(debugReturnLimit).ToList() : iceQuery.ToList();
    }

    /// <summary>
    /// Queries the International Continental Exchange website for Commitments of Traders data.
    /// </summary>
    /// <param name="iceCsvUrl">URL for the csv file to download.</param>
    /// <param name="databaseHeaders">Headers from the Disaggregated report found within the database located at <see cref="DatabasePath"/>.</param>
    /// <returns>A Date keyed dictionary containg a list of relevant rows.</returns>
    private static async Task<Dictionary<DateTime, List<string[]>>> QueryIceDataAsync(string iceCsvUrl, List<string> databaseHeaders, bool DebugActive)
    {
        var csvDataByDateTime = new Dictionary<DateTime, List<string[]>>();

        if (DebugActive)
        {
            Console.WriteLine(iceCsvUrl);
        }

        int iceShortDateColumn = 1;

        using var client = new HttpClient();
        using HttpResponseMessage response = await client.GetAsync(iceCsvUrl);
        if (response.IsSuccessStatusCode)
        {
            bool foundHeaders = false;
            using HttpContent content = response.Content;
            using var stream = (MemoryStream)await content.ReadAsStreamAsync();
            using var sr = new StreamReader(stream);
            int iceDateColumn = -1;
            const string IceShortDateFormat = "yyMMdd";
            while (!sr.EndOfStream)
            {
                string[]? iceCsvRecord;

                try
                {
                    iceCsvRecord = SplitOnCommaNotWithinQuotesRegex().Split(await sr.ReadLineAsync() ?? throw new NullReferenceException("Empty iceCsvRecord"));
                }
                catch (NullReferenceException)
                {
                    continue;
                }

                if (!foundHeaders)
                {
                    foundHeaders = true;
                    s_iceColumnMap ??= MapHeaderFieldsToDatabase(iceCsvRecord, databaseHeaders, true);
                    iceDateColumn = (int)s_iceColumnMap[StandardDateFieldName]?.ColumnIndex!;
                    iceShortDateColumn = (int)s_iceColumnMap.First(x => x.Key.Contains(IceShortDateFormat, StringComparison.InvariantCultureIgnoreCase)).Value?.ColumnIndex!;
                }
                else if (DateTime.TryParseExact(iceCsvRecord[iceShortDateColumn], IceShortDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out DateTime parsedDate))
                {
                    iceCsvRecord = iceCsvRecord.Select(x => x.Trim(s_charactersToTrim)).ToArray();
                    iceCsvRecord[iceDateColumn] = parsedDate.ToString(StandardDateFormat);

                    if (!csvDataByDateTime.TryGetValue(parsedDate, out List<string[]>? dataGroupedByDate))
                    {
                        dataGroupedByDate = csvDataByDateTime[parsedDate] = new();
                    }
                    dataGroupedByDate.Add(iceCsvRecord);
                }
            }
        }

        return csvDataByDateTime;
    }

    /// <summary>
    /// Uploads string arrays found within <paramref name="dataToUpload"/> to the database.
    /// </summary>
    /// <param name="fieldInfoPerEditedName">Dictionary that maps column names within the database to the equivalent column index within an array in <paramref name="dataToUpload"/>.</param>
    /// <param name="dataToUpload">A list of string arrays that data will be pulled from and uploaded to the database.</param>
    /// <param name="priceData">Dictionary of price information only supplied when <see cref="IsLegacyCombined"/> equals <see langword="true"/>.</param>
    /// <param name="uploadingIceData"><see langword="true"/> if uploading ICE data; otherwise, <see langword="false"/> for CFTC data.</param>  
    /// <returns><see langword="true"/> if no errors are generated; otherwise, <see langword="false"/>.</returns>
    bool UploadToDatabase(Dictionary<string, FieldInfo?> fieldInfoPerEditedName, List<string[]> dataToUpload, bool uploadingIceData, Dictionary<string, Dictionary<DateTime, string?>>? priceData = null)
    {
        // The value doesn't matter.
        const int PriceIndex = -1;

        bool updatePrices = IsLegacyCombined && !(priceData == null || uploadingIceData);
        if (updatePrices && fieldInfoPerEditedName.ContainsKey("price")) fieldInfoPerEditedName["price"] = new FieldInfo(columnIndex: PriceIndex, editedColumnName: "price", columnName: "Price");

        lock (DatabaseConnection)
        {
            try
            {
                DatabaseConnection.Open();

                using OleDbCommand cmd = DatabaseConnection.CreateCommand();
                Dictionary<string, char> paramaterizedCharByName = new();

                foreach (FieldInfo? mappedColumn in fieldInfoPerEditedName.Values)
                {
                    if (mappedColumn is not null)
                    {
                        cmd.Parameters.Add(new OleDbParameter(mappedColumn?.EditedColumnName, (OleDbType)mappedColumn?.ColumnType!) { IsNullable = true });
                        paramaterizedCharByName.Add(mappedColumn?.DatabaseAdjustedName!, '?');
                    }
                }

                if (cmd.Parameters.Count == 0)
                {
                    Console.WriteLine("No paramaters were added to the command.");
                    return false;
                }

                cmd.CommandText = $"INSERT INTO {SqlTableDataName} ({string.Join(',', paramaterizedCharByName.Keys)}) VALUES ({string.Join(',', paramaterizedCharByName.Values)});";
                paramaterizedCharByName.Clear();

                using OleDbTransaction transaction = DatabaseConnection.BeginTransaction();
                cmd.Transaction = transaction;

                int cotCodeColumn = (int)fieldInfoPerEditedName[ContractCodeColumnName]?.ColumnIndex!;
                int cotDateColumn = (int)fieldInfoPerEditedName[StandardDateFieldName]?.ColumnIndex!;
                // For each row of data, assign values to wanted parameters.         
                foreach (string[] dataRow in dataToUpload)
                {
                    foreach (OleDbParameter param in cmd.Parameters)
                    {
                        string? fieldValue;

                        if (updatePrices && param.OleDbType.Equals(OleDbType.Currency))
                        {    // Every row is guaranteed to have a corresponding DateTime.
                            fieldValue = priceData![dataRow[cotCodeColumn]][DateTime.ParseExact(dataRow[cotDateColumn], StandardDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None)];
                        }
                        else
                        {
                            int wantedIndex = (int)fieldInfoPerEditedName[param.ParameterName]?.ColumnIndex!;
                            fieldValue = dataRow[wantedIndex];
                        }

                        if (string.IsNullOrEmpty(fieldValue)) param.Value = DBNull.Value;
                        else
                        {
                            param.Value = param.OleDbType switch
                            {
                                OleDbType.Integer => int.Parse(fieldValue, NumberStyles.AllowThousands | NumberStyles.AllowLeadingSign),
                                OleDbType.Currency or OleDbType.Decimal => decimal.Parse(fieldValue),
                                OleDbType.VarChar => fieldValue,
                                OleDbType.Date => DebugActive ? s_defaultStartDate : DateTime.ParseExact(fieldValue, StandardDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None),
                                _ => throw new ArgumentOutOfRangeException($"{nameof(param.OleDbType)} is unaccounted for: {param.OleDbType}.")
                            };
                        }
                    }
                    cmd.ExecuteNonQuery();
                }
                transaction.Commit();
                if (!(IsLegacyCombined || uploadingIceData)) _waitingForPriceUpdate = true;
            }
            finally
            {
                DatabaseConnection.Close();
            }
        }
        return true;
    }

    /// <summary>
    /// Asynchronously queries yahoo finance for price data and updates dictionaries within <paramref name="priceByDateByContractCode"/>.
    /// </summary>
    /// <param name="priceSymbolByContractCode">Dictionary of price symbols keyed to cftc contract codes.</param>
    /// <param name="priceByDateByContractCode">A contract code keyed dictionary that contains a dictionary keyed to wanted dates for the given contract code.</param>
    /// <returns>An asynchronous task.</returns>
    static async Task RetrieveYahooPriceDataAsync(Dictionary<string, string> priceSymbolByContractCode, Dictionary<string, Dictionary<DateTime, string?>> priceByDateByContractCode)
    {
        const string BaseUrl = "https://query1.finance.yahoo.com/v7/finance/download/";

        using HttpClient priceRetrievalClient = new() { BaseAddress = new Uri(BaseUrl) };
        priceRetrievalClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("text/csv"));

        const byte YahooDateColumn = 0, YahooCloseColumn = 5;

        foreach (var knownSymbol in priceSymbolByContractCode)
        {
            if (priceByDateByContractCode.TryGetValue(knownSymbol.Key, out Dictionary<DateTime, string?>? wantedPrices))
            {
                var startDate = (long)wantedPrices.Keys.Min().Subtract(DateTime.UnixEpoch).TotalSeconds;
                var endDate = (long)wantedPrices.Keys.Max().AddDays(1).Subtract(DateTime.UnixEpoch).TotalSeconds;

                string urlDetails = $"{knownSymbol.Value}?period1={startDate}&period2={endDate}&interval=1d&events=history&includeAdjustedClose=true";
                string response;

                try
                {
                    response = await priceRetrievalClient.GetStringAsync(urlDetails);
                }
                catch (HttpRequestException)
                {
                    //retrievalFailures.Add($"Failed to retrieve data for {knownSymbol.Key} with Symbol:{knownSymbol.Value}");
                    continue;
                }
                string[] queriedResults = response.Trim(s_charactersToTrim).Split('\n');

                for (var i = 1; i < queriedResults.Length; ++i)
                {
                    string[] priceData = queriedResults[i].Split(',');
                    if (DateTime.TryParse(priceData[YahooDateColumn], out DateTime parsedDate) && wantedPrices.ContainsKey(parsedDate))
                    {
                        wantedPrices[parsedDate] = priceData[YahooCloseColumn];
                    }
                }
            }
        }
    }

    /// <summary>
    /// Queries the database for the latest date found within <see cref="SqlTableDataName"/> .
    /// </summary>
    /// <param name="filterForIce"><see langword="true"/> if the latest date for ICE data should be returned; otherwise, <see langword="false"/>.</param>
    /// <returns>The most recent DateTime found within the database.</returns>
    async Task<DateTime> ReturnLatestDateInTableAsync(bool filterForIce)
    {
        if (filterForIce && QueriedReport != ReportType.Disaggregated) throw new InvalidOperationException($"{nameof(QueriedReport)} must be {nameof(ReportType.Disaggregated)} while {nameof(filterForIce)} is true.");
        using OleDbConnection con = new(DatabaseConnectionString);
        using OleDbCommand cmd = con.CreateCommand();

        string iceCodes = "('B','Cocoa','G','RC','Wheat','W')";

        cmd.CommandText = $"SELECT MAX([Report_Date_as_YYYY-MM-DD]) FROM {SqlTableDataName} Where CFTC_Contract_Market_Code {(filterForIce ? string.Empty : "NOT ")}In {iceCodes};";
        DateTime storedDate = s_defaultStartDate;

        try
        {
            await con.OpenAsync();
            storedDate = ((DateTime?)await cmd.ExecuteScalarAsync()) ?? s_defaultStartDate;
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
        finally
        {
            con.Close();
        }

        return storedDate;
    }

    /// <summary>
    /// Asynchronously updates price data with stored data found in the legacy combined database.
    /// </summary>
    /// <param name="legacyCombinedInstance"><see cref="Report"/> instance that contains needed information to query prices from.</param>
    /// <returns>An asynchronous Task</returns>
    public bool UpdatePricesWithLegacyDatabase(Report legacyCombinedInstance)
    {
        if (!legacyCombinedInstance.IsLegacyCombined) throw new ArgumentOutOfRangeException(nameof(legacyCombinedInstance), "Must use Legacy Combined instance to update.");

        string sqlCommand = @$"Update {SqlTableDataName} as T
                             INNER JOIN [{legacyCombinedInstance.DatabasePath}].{legacyCombinedInstance.SqlTableDataName} as Source_TBL
                             ON Source_TBL.[Report_Date_as_YYYY-MM-DD]=T.[Report_Date_as_YYYY-MM-DD] AND Source_TBL.[CFTC_Contract_Market_Code]=T.[CFTC_Contract_Market_Code]
                             SET T.[Price] = Source_TBL.[Price] WHERE T.[Report_Date_as_YYYY-MM-DD] > ?;";// AND Source_TBL.[Price] IS NOT ?;";

        using OleDbCommand cmd = DatabaseConnection.CreateCommand();
        cmd.CommandText = sqlCommand;
        cmd.Parameters.AddWithValue("@GreaterThanDate", DataBaseDateBeforeUpdate);
        // cmd.Parameters.AddWithValue("nullPrice", DBNull.Value);

        lock (DatabaseConnection)
        {
            try
            {
                if (DatabaseConnection.State == System.Data.ConnectionState.Closed) DatabaseConnection.Open();
                cmd.ExecuteNonQuery();
                _waitingForPriceUpdate = false;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                return false;
            }
            finally
            {
                DatabaseConnection.Close();
            }
            return true;
        }
    }

    /// <summary>
    /// Queries the database for the field names found within the <see cref="SqlTableDataName"/> table.
    /// </summary>
    /// <returns>A list of field names found within the database.</returns>
    async Task<List<string>> QueryDatabaseFieldNamesAsync()
    {
        using OleDbConnection con = new(DatabaseConnectionString);
        using OleDbCommand cmd = con.CreateCommand();

        cmd.CommandText = $"SELECT TOP 1 * FROM {SqlTableDataName};";
        List<string> fieldNames = new();
        try
        {
            await con.OpenAsync();

            using var reader = await cmd.ExecuteReaderAsync();

            fieldNames = (from columnSchema in await reader.GetColumnSchemaAsync()
                          let fieldName = columnSchema.ColumnName
                          where fieldName.Equals("id", StringComparison.InvariantCultureIgnoreCase) == false
                          select fieldName).ToList();

            await reader.CloseAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
        finally
        {
            con.Close();
        }
        return fieldNames;
    }

    public void DisposeConnection()
    {
        DatabaseConnection.Dispose();
    }

    /// <summary>
    /// Maps columns from the retrieved API data to columns within the related local database.
    /// </summary>
    /// <param name="externalHeaders">Array of field names from an external source that need to be aligned with database fields.</param>
    /// <param name="databaseFields">List of field names found within the database.</param>
    /// <param name="iceHeaders"><see langword="true"/> if <paramref name="externalHeaders"/> are from an ICE C.O.T report; otherwise, <see langword="false"/> for CFTC reports.</param>
    /// <returns>Returns a dictionary of FieldInfo instances keyed to their edited names if the field exists in both <paramref name="externalHeaders"/> and <paramref name="databaseFields"/>; otherwise, a value of null.</returns> 
    static Dictionary<string, FieldInfo?> MapHeaderFieldsToDatabase(string[] externalHeaders, List<string> databaseFields, bool iceHeaders)
    {
        // return dictionary keyed to fields within databaseFields with wanted FieldInfo structs as a value 
        Dictionary<string, FieldInfo?> fieldInfoByEditedName = new();
        // Dictionary keyed to api header names with their 0 based column number.
        Dictionary<string, int> headerIndexesByEditedName = new();

        // Editing externalHeaders for alignment.
        for (var i = 0; i < externalHeaders.Length; ++i)
        {
            var header = externalHeaders[i].ToLower();
            if (!iceHeaders)
            {
                header = header.Replace("spead", "spread");
                header = header.Replace("postions", "positions");
                header = header.Replace("open_interest", "oi");
                header = header.Replace("__", "_");
                externalHeaders[i] = header.Replace("\"", string.Empty);
            }
            else
            {
                externalHeaders[i] = header.Contains("yyyy", StringComparison.InvariantCultureIgnoreCase) ? StandardDateFieldName : header;
            }
            headerIndexesByEditedName.Add(externalHeaders[i], i);
        }

        Dictionary<string, string> databaseFieldReplacements = new()
        {
            {" ","_"},{"\"",string.Empty},
            {"%", "pct"},
            {"=", "_"},
            {"(", "_"},
            {")", string.Empty},
            {"-", "_"},
            {"commercial", "comm"},
            {"reportable", "rept"},
            {"total", "tot"},
            {"concentration", "conc"},
            {"spreading", "spread"},
            {"_lt_", "_le_"},
            {"___","_"},
            {"__", "_"},
            { "open_interest_oi", "oi"},
            { "open_interest", "oi"},
            {"_in_initials",string.Empty}
        };
        // <edited name , original name in database >
        Dictionary<string, string> originalFieldByEditedName = new();

        // Edit database column names for alignment.
        for (var i = 0; i < databaseFields.Count; ++i)
        {
            var editedTableFieldName = databaseFields[i].ToLower();

            if (editedTableFieldName.Contains("yyyy", StringComparison.InvariantCultureIgnoreCase))
            {
                editedTableFieldName = StandardDateFieldName;
            }
            else
            {
                if (iceHeaders) editedTableFieldName = editedTableFieldName.Replace("__", "_");
                else
                {   // Apply known substitutions.
                    foreach (var pair in databaseFieldReplacements)
                    {
                        editedTableFieldName = editedTableFieldName.Replace(pair.Key, pair.Value);
                    }
                }
            }
            //  There is an inconsistency between cftc_region_code in the api and in the original database columns.
            if (!editedTableFieldName.Equals("cftc_region_code"))
            {
                originalFieldByEditedName.Add(editedTableFieldName, databaseFields[i]);
                fieldInfoByEditedName.Add(editedTableFieldName, null);
            }
        }

        // ICE places their contract codes under cftc_commodity_code rather than cftc_contract_market_code like the cftc,
        // so map cftc_contract_market_code to cftc_commodity_code's column index.
        if (iceHeaders && headerIndexesByEditedName.ContainsKey(ContractCodeColumnName) && headerIndexesByEditedName.Remove("cftc_commodity_code", out int columnIndex))
        {
            headerIndexesByEditedName[ContractCodeColumnName] = columnIndex;
        }
        /*
            Cycle through database fields and attempt to align fields via exact matches.
            If that fails, remove elements from the endings array until an exact match is found.
            Then attempt to match basic field with a field within the api and then cycle through numeric additions to align additional fields.
        */
        foreach ((string editedTableFieldName, string databaseFieldName) in originalFieldByEditedName)
        {
            if (fieldInfoByEditedName[editedTableFieldName] != null) continue;

            if (headerIndexesByEditedName.Remove(editedTableFieldName, out columnIndex))
            {
                fieldInfoByEditedName[editedTableFieldName] = new FieldInfo(columnIndex, databaseFieldName, editedTableFieldName);
            }
            else if (!iceHeaders)
            {
                // These endings are sorted by the orde r in which they appear within the api headers.
                string[] endings = { "_all", "_old", "_other" };
                // Remove endings from editedTableFieldName until a match is found within headerIndexesByEditedName.
                // Once found map column and then attempt to find additional substitutions.
                for (byte primaryEndingsIndex = 0; primaryEndingsIndex < endings.Length; ++primaryEndingsIndex)
                {
                    if (editedTableFieldName.EndsWith(endings[primaryEndingsIndex]))
                    {
                        string baseKey = editedTableFieldName.Replace(endings[primaryEndingsIndex], string.Empty);
                        // _1 can represent old or other so it's important that its addition be independent of primaryEndingsIndex.
                        byte digitIncrement = 0;
                        for (byte secondaryEndingsIndex = primaryEndingsIndex; secondaryEndingsIndex < endings.Length; ++secondaryEndingsIndex)
                        {
                            string? newKey = null;
                            if (secondaryEndingsIndex == primaryEndingsIndex && headerIndexesByEditedName.Remove(baseKey, out columnIndex))
                            {
                                newKey = editedTableFieldName;
                            }
                            else if (secondaryEndingsIndex > primaryEndingsIndex && headerIndexesByEditedName.Remove(baseKey + $"_{++digitIncrement}", out columnIndex))
                            {
                                newKey = baseKey + endings[secondaryEndingsIndex];
                            }

                            if (!string.IsNullOrEmpty(newKey)) fieldInfoByEditedName[newKey] = new FieldInfo(columnIndex, originalFieldByEditedName[newKey], newKey);
                        }
                    }
                }
            }
        }
        return fieldInfoByEditedName;
    }

    [GeneratedRegex("[,]{1}(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))")]
    private static partial Regex SplitOnCommaNotWithinQuotesRegex();
}



