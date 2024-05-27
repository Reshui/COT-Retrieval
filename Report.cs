namespace ReportRetriever;

using System.Net.Http.Headers;
using System.Collections.Concurrent;
using System.Data.OleDb;
using System.Runtime.Versioning;
using System.Text.RegularExpressions;
using System.Globalization;
using System.Diagnostics;

//using System.Text.Json.Nodes;

public enum ReportType
{
    Legacy, Disaggregated, TFF
}
public enum ReportStatusCode
{
    NoUpdateAvailable, Updated, Failure, AttemptingRetrieval, AttemptingUpdate, NotInitialized, CheckingDataAvailability
}

[SupportedOSPlatform("windows")]
public partial class Report : IDisposable
{
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
    /// Gets or sets an enum that represnts the current state of the instance.
    /// </summary>
    /// <value>The current value of the backing field <see cref="_currentStatus"/></value>
    /// <remarks>IF <see cref="IsLegacyCombined"/> is <see langword="true"/> then <see cref="s_retrievalLockingStatusCode"/> will also be updated.</remarks>
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
    /// File path to the database associaed with the current value of <see cref="QueriedReport"/>.
    /// </summary>
    private readonly string _microsoftAccessDatabasePath;

    /// <summary>
    /// The table name to target within the database located at <see cref="_microsoftAccessDatabasePath"/>.
    /// </summary>
    private readonly string _tableNameWithinDatabase;

    /// <summary>
    /// Gets an identification code used to target the relevant CFTC API code.
    /// </summary>
    private readonly string _cftcApiCode;

    /// <summary>
    /// Field used to help determine if <see cref="AwaitingPriceUpdate"/> should return <see langword="true"/>.
    /// </summary>
    private bool _waitingForPriceUpdate = false;

    /// <summary>
    /// Number used to count how many threads are attempting to retrieve ICE data.
    /// </summary>
    private static int s_activeIceRetrievalCount = 0;

    /// <summary>
    /// DateTime returned when an instance for Legacy_Combined data queries the database for the most recent date.
    /// </summary>
    /// <remarks>Assigned a value in <see cref="CommitmentsOfTradersRetrievalAndUploadAsync"/></remarks>
    private static DateTime s_legacyCombinedDateBeforeApiQuery;

    /// <summary>
    /// Dictionary used to store downloaded ICE COT data regardless of <see cref="RetrieveCombinedData"/>'s value.
    /// </summary>
    private static readonly ConcurrentDictionary<string, Task<Dictionary<DateTime, List<string[]>>>?> s_iceCsvRawData = new();

    /// <summary>
    /// Dictionary used to map ICE column names to their respective indexes within the Disaggregated database.
    /// </summary>
    private static Dictionary<string, FieldInfo?>? s_iceColumnMap = null;

    /// <summary>
    /// Dictionary of <see cref="OleDbConnection"/> objects used for each <see cref="ReportType"/>.
    /// </summary>
    private static readonly Dictionary<ReportType, OleDbConnection> s_oleDbConnectionsByReportType = new();

    /// <summary>
    /// Gets a <see cref="OleDbConnection"/> object that correlates to this instance's <see cref="QueriedReport"/>.
    /// </summary>
    private OleDbConnection DatabaseConnection => s_oleDbConnectionsByReportType[QueriedReport];

    /// <summary>
    /// Static variable to track changes done to Legacy_Combined instances.
    /// </summary>
    /// <remarks>Used to escape waiting loop in RetrieveDataAsync for non Legacy Combined data.</remarks>
    private static ReportStatusCode s_retrievalLockingStatusCode = ReportStatusCode.NotInitialized;

    /// <summary>
    /// Date before the inception of the Commitments of Traders Report.
    /// </summary>
    /// <value>An instance initialized to January 1, 1970.</value>
    private static readonly DateTime s_defaultStartDate = new(1970, 1, 1);

    /// <summary>
    /// HttpClient used to query C.O.T API data.
    /// </summary>
    private static readonly HttpClient s_cftcApiClient = new() { BaseAddress = new Uri("https://publicreporting.cftc.gov/resource/") };

    /// <summary>
    /// Connection string used to connect to the database located at <see cref="_microsoftAccessDatabasePath"/>.
    /// </summary> 
    private string DatabaseConnectionString => "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + _microsoftAccessDatabasePath + ';';

    /// <summary>
    ///  Gets a boolean value that represents if the current instance is Legacy Combined data.
    /// </summary>
    /// <value><see langword="true"/> if <see cref="QueriedReport"/> equals <see cref="ReportType.Legacy"/> and <see cref="RetrieveCombinedData"/> equals <see langword="true"/>; otherwise, <see langword="false"/>.</value>
    public bool IsLegacyCombined { get; }

    /// <summary>
    /// Gets the <see cref="ReportType"/> enum used to initialize the class.
    /// </summary>
    /// <value>An enum that specifies which Commitments of Traders Report this instance will target.</value>
    public ReportType QueriedReport { get; }

    /// <summary>
    /// Gets or sets a DateTime instance that represents the largest date available before data has been retrieved from the CFTC API.
    /// </summary>
    /// <value>The largest date found within <see cref="_tableNameWithinDatabase"/> the database located at <see cref="_microsoftAccessDatabasePath"/>.</value>
    public DateTime DatabaseDateBeforeUpdate { get; private set; }

    /// <summary>
    /// Gets or sets a DateTimve instance used to keep track of the largest date returned from a CFTC data query.
    /// </summary>
    /// <value>The largest date returned when querying CFTC data.</value>
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
    /// Gets a value that specifies if the current instance is tied to Futures + Options data.
    /// </summary>
    /// <value><see langword="true"/> if instance is dedicated to Futures + Options data; otherwise, <see langword="false"/> if designated for Futures only.</value>
    public bool RetrieveCombinedData { get; }

    /// <summary>
    /// Gets a value used to toggle off or limit certain functionalities in this instance
    /// </summary>
    /// <value><see langword="true"/> if currently debugging; otherwise, <see langword="false"/>.</value>
    private bool DebugActive { get; }

    /// <summary>
    /// Timer used to time how long it takes to retrieve and upload data.
    /// </summary>
    public readonly Stopwatch ActionTimer = new();
    /// <summary>
    /// Gets or sets a boolean to show if the most recent date
    /// </summary>
    private bool _databaseQueriedForMostRecentDate = false;

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

    public string Id { get => $"{QueriedReport.ToString()[0]}{(RetrieveCombinedData ? 'C' : 'F')}"; }

    /// <summary>
    /// Initializes a new instance of the Report class with the specified properties.
    /// </summary>
    /// <param name="queriedReport">A <see cref="ReportType"/> enum used to specify what sort of data should be retrieved with this instance.</param>
    /// <param name="retrieveCombinedData"><see langword="true"/> if Futures + Options data should be filtered for; otherwise, <see langword="false"/> for Futures Only data.</param>
    /// <param name="microsoftAccessDatabasePath">File path to database that data should be stored in.</param>
    /// <exception cref="FileNotFoundException">Thrown if <paramref name="microsoftAccessDatabasePath"/> cannot be found.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if an <paramref name="queriedReport"/> is out of range.</exception>
    public Report(ReportType queriedReport, bool retrieveCombinedData, string microsoftAccessDatabasePath, string tableNameWithinDatabase, bool useDebugMode = false)
    {
        if (!File.Exists(microsoftAccessDatabasePath)) throw new FileNotFoundException($"{nameof(microsoftAccessDatabasePath)} doesn't exist.", microsoftAccessDatabasePath);

        if (!new ReportType[] { ReportType.Legacy, ReportType.Disaggregated, ReportType.TFF }.Any(x => x.Equals(queriedReport)))
        {
            throw new ArgumentOutOfRangeException(nameof(queriedReport), queriedReport, $"Unsupported {nameof(ReportType)} detected.");
        }

        QueriedReport = queriedReport;
        RetrieveCombinedData = retrieveCombinedData;
        DebugActive = useDebugMode;
        IsLegacyCombined = queriedReport == ReportType.Legacy && retrieveCombinedData;

        _microsoftAccessDatabasePath = microsoftAccessDatabasePath;
        _tableNameWithinDatabase = tableNameWithinDatabase;
        _cftcApiCode = s_apiIdentification[retrieveCombinedData][queriedReport];

        var con = new OleDbConnection(DatabaseConnectionString);
        if (!s_oleDbConnectionsByReportType.TryAdd(queriedReport, con))
        {
            con.Dispose();
        }
    }

    /// <summary>
    /// Retrieves related data from api and uploads to a local database if new data is retrieved.
    /// </summary>
    /// <param name="yahooPriceSymbolByContractCode">Dictionary of price symbols keyed to cftc contract codes.</param>
    /// <param name="testUpload">If <see langword="true"/> and <see cref="DebugActive"/> is <see langword="true"/> then data upload will be tested.</param>
    /// <returns><see langword="true"/> if no errors are generated; otherwise, <see langword="false"/>.</returns>
    /// <remarks>Price data will only be retrieved if <see cref="IsLegacyCombined"/> is <see langword="true"/>.</remarks>
    /// <exception cref="HttpRequestException">Thrown if unable to connecto to API service.</exception>
    /// <exception cref="KeyNotFoundException">Thrown if a necessary key for data upload wasn't found.</exception>
    /// <exception cref="NullReferenceException">Thrown if a null value is returned for a field necessary for data upload.</exception>
    /// <exception cref="OleDbException">Database error.</exception>
    /// <exception cref="IndexOutOfRangeException">Indicates an error in record length returned from source.</exception>
    /// <exception cref="ArgumentException"></exception>
    public async Task CommitmentsOfTradersRetrievalAndUploadAsync(Dictionary<string, string>? yahooPriceSymbolByContractCode, bool testUpload = false)
    {
        try
        {
            if (!DebugActive && testUpload) throw new ArgumentException($"Cannot test upload if {nameof(DebugActive)} is false.");

            ActionTimer.Start();

            if (!_databaseQueriedForMostRecentDate)
            {
                DatabaseDateBeforeUpdate = DatabaseDateAfterUpdate = await ReturnLatestDateInTableAsync(filterForIce: false).ConfigureAwait(false);
                // DatabaseDateBeforeUpdate = DatabaseDateAfterUpdate = await Task.Run(() => ReturnLatestDateInTable(filterForIce: false)).ConfigureAwait(false);                
                _databaseQueriedForMostRecentDate = true;
            }

            CurrentStatus = ReportStatusCode.CheckingDataAvailability;

            if (!DebugActive)
            {
                // Wait until after the Legacy_Combined instance has attempted CFTC retrieval before continuing.
                if (!IsLegacyCombined)
                {
                    // Loop until a change in state is detected in the running Legacy Combined instance.
                    ActionTimer.Stop();
                    while (s_retrievalLockingStatusCode == ReportStatusCode.CheckingDataAvailability)
                    {
                        await Task.Delay(300).ConfigureAwait(false);
                    }

                    var failureCodes = new ReportStatusCode[] { ReportStatusCode.NoUpdateAvailable, ReportStatusCode.Failure };

                    if (failureCodes.Contains(s_retrievalLockingStatusCode) && DatabaseDateBeforeUpdate >= s_legacyCombinedDateBeforeApiQuery)
                    {
                        CurrentStatus = ReportStatusCode.NoUpdateAvailable;
                        return;
                    }
                    ActionTimer.Start();
                }
                else
                {
                    s_legacyCombinedDateBeforeApiQuery = DatabaseDateBeforeUpdate;
                }
            }

            int queryReturnLimit = DebugActive ? 1 : 20_000;
            // Headers from local database.
            List<string> databaseFieldNames = await QueryDatabaseFieldNamesAsync().ConfigureAwait(false);
            // Dictionary to keep track of wanted date and contract code combinations. 
            Dictionary<string, Dictionary<DateTime, string?>> priceByDateByContractCode = new();
            // (New data from API, Mapped FieldInfo instances for each column or null)
            (List<string[]> cftcData, Dictionary<string, FieldInfo?>? cftcFieldInfoByEditedName) = await CftcCotRetrievalAsync(queryReturnLimit, priceByDateByContractCode, databaseFieldNames).ConfigureAwait(false);

            List<string[]>? iceData = null;
            if (QueriedReport == ReportType.Disaggregated && cftcData.Count > 0)
            {
                iceData = await IceCotRetrievalAsync(DatabaseDateAfterUpdate, databaseFieldNames, queryReturnLimit).ConfigureAwait(false);
            }

            if (cftcData.Count > 0 && cftcFieldInfoByEditedName is not null)
            {   // Only retrieve price data for Legacy Combined instances since it encompases both Disaggregated and Traders in Financial Futures reports.
                if (IsLegacyCombined && yahooPriceSymbolByContractCode != null && cftcFieldInfoByEditedName.ContainsKey("price"))
                {
                    bool retrievePrices = true;
                    if (DebugActive)
                    {
                        Console.WriteLine("Do you want to test price retrieval(Y/N)?");
                        var keyResponse = Console.ReadKey(true);
                        if (keyResponse.Key != ConsoleKey.Y) retrievePrices = false;
                    }

                    if (retrievePrices)
                    {
                        await RetrieveYahooPriceDataAsync(yahooPriceSymbolByContractCode, priceByDateByContractCode).ConfigureAwait(false);
                    }
                }

                if (!DebugActive || testUpload)
                {
                    if (QueriedReport == ReportType.Disaggregated && iceData != null && s_iceColumnMap != null && iceData.Count > 0)
                    {
                        try
                        {   // Make an attempt to upload ICE data.
                            await Task.Run(() => UploadToDatabase(fieldInfoPerEditedName: s_iceColumnMap, dataToUpload: iceData, uploadingIceData: true));
                        }
                        catch (Exception e)
                        {   // ICE is low priority so just print the error.
                            Console.WriteLine(e);
                        }
                    }
                    // Make an attempt to upload CFTC data.
                    await Task.Run(() => UploadToDatabase(fieldInfoPerEditedName: cftcFieldInfoByEditedName, dataToUpload: cftcData, uploadingIceData: false, priceData: priceByDateByContractCode));
                }
            }
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
    }

    /// <summary>
    /// Retrieves CFTC Commitments of Traders data if any data has a date value more recent than <see cref="DatabaseDateBeforeUpdate"/>. 
    /// </summary>
    /// <param name="maxRecordsPerLoop">Number used to limit the number of records retrieved from each loop attempt.</param>
    /// <param name="priceByDateByContractCode">Used to store null values for wanted price data.</param>
    /// <param name="databaseFieldNames">List of headers from table in local database that data will be uploaded to.</param>
    /// <returns>An asynchronous task.</returns>
    /// <exception cref="FormatException"></exception>
    /// <exception cref="HttpRequestException">Thrown if an error occurs while attempting to access the CFTC API.</exception>
    /// <exception cref="KeyNotFoundException">Thrown if an unknown key is used to access the fieldInfoByEditedName dictionary.</exception>
    /// <exception cref="NullReferenceException">Thrown if an attempt to use a null value from fieldInfoByEditedName dictionary.</exception>
    private async Task<(List<string[]>, Dictionary<string, FieldInfo?>?)> CftcCotRetrievalAsync(int maxRecordsPerLoop, Dictionary<string, Dictionary<DateTime, string?>> priceByDateByContractCode, List<string> databaseFieldNames)
    {
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types
        const string WantedDataFormat = ".csv", MimeType = "text/csv";
        int offsetCount = 0, remainingRecordsToRetrieve = 0;
        string[]? responseLines = null;

        HttpHeaderValueCollection<MediaTypeWithQualityHeaderValue> acceptHeaders = s_cftcApiClient.DefaultRequestHeaders.Accept;
        if (acceptHeaders.Count == 0)
        {
            acceptHeaders.Add(new MediaTypeWithQualityHeaderValue(MimeType));
        }
        // List will contain records cleared for database upload.
        List<string[]> newCftcData = new();
        // Dictionary will hold mapped FieldInfo instances for fields within the API and local database.
        Dictionary<string, FieldInfo?>? fieldInfoByEditedName = null;

        string comparisonOperator = DebugActive ? ">=" : ">";
        // Make initial API call to find out how many new records are available. Executed only once.
        var countRecordsUrl = $"{_cftcApiCode}{WantedDataFormat}?$select=count(id)&$where=report_date_as_yyyy_mm_dd {comparisonOperator}'{DatabaseDateBeforeUpdate.ToString(StandardDateFormat)}'";

        string? response = await s_cftcApiClient.GetStringAsync(countRecordsUrl).ConfigureAwait(false);

        remainingRecordsToRetrieve = int.Parse(response.Split('\n')[1].Trim(s_charactersToTrim), NumberStyles.Number, null);

        if (remainingRecordsToRetrieve == 0)
        {
            CurrentStatus = ReportStatusCode.NoUpdateAvailable;
        }
        else if (DebugActive)
        {
            remainingRecordsToRetrieve = maxRecordsPerLoop;
        }

        while (remainingRecordsToRetrieve > 0)
        {
            CurrentStatus = ReportStatusCode.AttemptingRetrieval;
            string apiDetails = $"{_cftcApiCode}{WantedDataFormat}?$where=report_date_as_yyyy_mm_dd{comparisonOperator}'{DatabaseDateBeforeUpdate.ToString(StandardDateFormat)}'&$order=id&$limit={maxRecordsPerLoop}&$offset={offsetCount++}";
            response = await s_cftcApiClient.GetStringAsync(apiDetails).ConfigureAwait(false);

            // Data from the API tends to have an extra line at the end so trim it.
            responseLines = response.Trim('\n').Split('\n');
            response = null;
            // Subtract 1 to account for headers.
            remainingRecordsToRetrieve -= responseLines.Length - 1;

            fieldInfoByEditedName ??= MapHeaderFieldsToDatabase(externalHeaders: MyRegex().Split(responseLines[0]), databaseFields: databaseFieldNames, iceHeaders: false);

            int cftcDateColumn = (int)fieldInfoByEditedName[StandardDateFieldName]?.ColumnIndex!;
            int cftcCodeColumn = (int)fieldInfoByEditedName[ContractCodeColumnName]?.ColumnIndex!;

            // Start index at 1 rather than 0 to skip over headers.
            for (var i = 1; i < responseLines.Length; ++i)
            {
                if (!string.IsNullOrEmpty(responseLines[i]))
                {
                    string[] apiRecord = MyRegex().Split(responseLines[i]).Select(x => x.Trim(s_charactersToTrim)).ToArray();

                    if (DateTime.TryParseExact(apiRecord[cftcDateColumn], StandardDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime parsedDate)
                    && ((parsedDate > DatabaseDateBeforeUpdate && !DebugActive) || (parsedDate >= DatabaseDateBeforeUpdate && DebugActive)))
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
            responseLines = null;
        };
        return (newCftcData, fieldInfoByEditedName);
    }

    /// <summary>
    /// Starts asynchronous tasks to download ICE contract data based on the value of <paramref name="mostRecentCftcDate"/>.
    /// </summary>
    /// <param name="mostRecentCftcDate">The date value of the most recent data from the CFTC.</param>
    /// <param name="databaseFieldNames">Field names of the table data would be uploaded to.</param>
    /// <param name="debugReturnLimit">Number of rows to return if debugging this method.</param>
    /// <returns>An asynchronous task.</returns>
    private async Task<List<string[]>?> IceCotRetrievalAsync(DateTime mostRecentCftcDate, List<string> databaseFieldNames, int debugReturnLimit = 1)
    {
        //DateTime maxIceDateInDatabase = await ReturnLatestDateInTableAsync(filterForIce: true).ConfigureAwait(false);
        DateTime maxIceDateInDatabase = await Task.Run(() => ReturnLatestDateInTable(true)).ConfigureAwait(false);

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

        try
        {
            await Task.WhenAll(s_iceCsvRawData.Values!);
        }
        catch (Exception e)
        {
            Console.WriteLine("Failed to retrieve at least 1 instance of ICE data.    " + e.Message);
        }

        try
        {
            int futOrCombinedColumn = (int)s_iceColumnMap!["futonly_or_combined"]?.ColumnIndex!;
            // Filter for data relevant to the current instance and is more recent than what is stored in the database.
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
        catch (Exception e) when (e is KeyNotFoundException or NullReferenceException)
        {
            return new List<string[]>();
        }
    }

    /// <summary>
    /// Queries the International Continental Exchange website for Commitments of Traders data.
    /// </summary>
    /// <param name="iceCsvUrl">URL for the csv file to download.</param>
    /// <param name="databaseHeaders">Headers from the Disaggregated report found within the database located at <see cref="_microsoftAccessDatabasePath"/>.</param>
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
        using HttpResponseMessage response = await client.GetAsync(iceCsvUrl).ConfigureAwait(false);
        if (response.IsSuccessStatusCode)
        {
            bool foundHeaders = false;
            using HttpContent content = response.Content;
            using var stream = (MemoryStream)await content.ReadAsStreamAsync().ConfigureAwait(false);
            using var sr = new StreamReader(stream);
            int iceDateColumn = -1;
            const string IceShortDateFormat = "yyMMdd";
            while (!sr.EndOfStream)
            {
                string[]? iceCsvRecord;

                try
                {
                    iceCsvRecord = MyRegex().Split(await sr.ReadLineAsync().ConfigureAwait(false) ?? throw new NullReferenceException("Empty iceCsvRecord"));
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
    /// <remarks>Method isn't asynchronous because attempts to use the same <see cref="DatabaseConnection"/> instance from different threads will result in an error.</remarks>
    /// <exception cref="KeyNotFoundException">Thrown if an unknown key is used to access <paramref name="fieldInfoPerEditedName"/>.</exception>
    /// <exception cref="IndexOutOfRangeException">Thrown if a wanted index is out of bounds fora an array in <paramref name="dataToUpload"/>.</exception>
    /// <exception cref="OleDbException">Error related to database interaction.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if a field type is unaccounted for when assigning a value to a parameter or if <paramref name="fieldInfoPerEditedName"/> contains only null values.</exception>    
    /// <exception cref="InvalidOperationException">Thrown if an error occurs while attempting to comit a database transaction.</exception> 
    void UploadToDatabase(Dictionary<string, FieldInfo?> fieldInfoPerEditedName, List<string[]> dataToUpload, bool uploadingIceData, Dictionary<string, Dictionary<DateTime, string?>>? priceData = null)
    {
        CurrentStatus = ReportStatusCode.AttemptingUpdate;

        bool updatePrices = IsLegacyCombined && !(priceData == null || uploadingIceData);
        if (updatePrices && fieldInfoPerEditedName.ContainsKey("price"))
        {   // The value of PriceIndex doesn't matter.
            const int PriceIndex = -1;
            fieldInfoPerEditedName["price"] = new FieldInfo(columnIndex: PriceIndex, editedColumnName: "price", columnName: "Price");
        }

        lock (DatabaseConnection)
        {
            bool closeConnectionOnFinish = false;
            try
            {
                if (DatabaseConnection.State == System.Data.ConnectionState.Closed)
                {
                    DatabaseConnection.Open();
                    closeConnectionOnFinish = true;
                }
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
                    throw new ArgumentOutOfRangeException(nameof(fieldInfoPerEditedName), $"Command paramaterization failed. {nameof(fieldInfoPerEditedName)} doesn't contain non-null values.");
                }

                cmd.CommandText = $"INSERT INTO {_tableNameWithinDatabase} ({string.Join(',', paramaterizedCharByName.Keys)}) VALUES ({string.Join(',', paramaterizedCharByName.Values)});";
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
                        {
                            try
                            {   // Code is already set up to ensure that there is a value for every record. This ty block is for just in case an error happens somewhere else.
                                fieldValue = priceData![dataRow[cotCodeColumn]][DateTime.ParseExact(dataRow[cotDateColumn], StandardDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None)];
                            }
                            catch (KeyNotFoundException)
                            {
                                fieldValue = null;
                            }
                        }
                        else
                        {
                            fieldValue = dataRow[(int)fieldInfoPerEditedName[param.ParameterName]?.ColumnIndex!];
                        }

                        if (string.IsNullOrEmpty(fieldValue))
                        {
                            param.Value = DBNull.Value;
                        }
                        else
                        {
                            param.Value = param.OleDbType switch
                            {
                                OleDbType.Integer => int.Parse(fieldValue, NumberStyles.AllowThousands | NumberStyles.AllowLeadingSign),
                                OleDbType.Currency or OleDbType.Decimal => decimal.Parse(fieldValue),
                                OleDbType.VarChar => fieldValue,
                                OleDbType.Date => DebugActive ? s_defaultStartDate : DateTime.ParseExact(fieldValue, StandardDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None),
                                _ => throw new ArgumentOutOfRangeException(nameof(fieldInfoPerEditedName), param.OleDbType, $"An unaccounted for OleDbType was encountered when accessing {param.ParameterName}.")
                            };
                        }
                    }
                    cmd.ExecuteNonQuery();
                }
                transaction.Commit();

                if (!(IsLegacyCombined || uploadingIceData)) _waitingForPriceUpdate = true;
                CurrentStatus = ReportStatusCode.Updated;
            }
            finally
            {
                if (closeConnectionOnFinish) DatabaseConnection.Close();
            }
        }
    }

    /// <summary>
    /// Asynchronously queries yahoo finance for price data and updates dictionaries within <paramref name="priceByDateByContractCode"/>.
    /// </summary>
    /// <param name="yahooPriceSymbolByContractCode">Dictionary of price symbols keyed to cftc contract codes.</param>
    /// <param name="priceByDateByContractCode">A contract code keyed dictionary that contains a dictionary keyed to wanted dates for the given contract code.</param>
    /// <returns>An asynchronous task.</returns>
    static async Task RetrieveYahooPriceDataAsync(Dictionary<string, string> yahooPriceSymbolByContractCode, Dictionary<string, Dictionary<DateTime, string?>> priceByDateByContractCode)
    {
        using HttpClient priceRetrievalClient = new() { BaseAddress = new Uri("https://query1.finance.yahoo.com/v7/finance/download/") };
        priceRetrievalClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("text/csv"));

        const byte YahooDateColumn = 0, YahooCloseColumn = 5;
        // int httpRequestExceptionCount = 0, httpAttemptCount = 0;
        foreach (var knownSymbol in yahooPriceSymbolByContractCode)
        {
            if (priceByDateByContractCode.TryGetValue(knownSymbol.Key, out Dictionary<DateTime, string?>? wantedPrices))
            {
                var startDate = (long)wantedPrices.Keys.Min().Subtract(DateTime.UnixEpoch).TotalSeconds;
                var endDate = (long)wantedPrices.Keys.Max().AddDays(1).Subtract(DateTime.UnixEpoch).TotalSeconds;

                string urlDetails = $"{knownSymbol.Value}?period1={startDate}&period2={endDate}&interval=1d&events=history&includeAdjustedClose=true";
                string response;

                try
                {
                    //++httpAttemptCount;
                    response = await priceRetrievalClient.GetStringAsync(urlDetails).ConfigureAwait(false);
                }
                catch (HttpRequestException)
                {
                    // Console.WriteLine(e.StatusCode);
                    // ++httpRequestExceptionCount;
                    continue;
                }
                string[] queriedResults = response.Trim(s_charactersToTrim).Split('\n');

                for (var i = 1; i < queriedResults.Length; ++i)
                {
                    string[] priceData = queriedResults[i].Split(',');
                    if (DateTime.TryParseExact(priceData[YahooDateColumn], "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime parsedDate) && wantedPrices.ContainsKey(parsedDate))
                    {
                        wantedPrices[parsedDate] = priceData[YahooCloseColumn];
                    }
                }
            }
        }
        //if (httpRequestExceptionCount ==httpAttemptCount ) throw new HttpRequestException()
    }

    /// <summary>
    /// Queries the database for the latest date found within <see cref="_tableNameWithinDatabase"/> .
    /// </summary>
    /// <param name="filterForIce"><see langword="true"/> if the latest date for ICE data should be returned; otherwise, <see langword="false"/>.</param>
    /// <returns>The most recent DateTime found within the database.</returns>
    async Task<DateTime> ReturnLatestDateInTableAsync(bool filterForIce)
    {
        if (filterForIce && QueriedReport != ReportType.Disaggregated)
        {
            throw new InvalidOperationException($"{nameof(QueriedReport)} must be {nameof(ReportType.Disaggregated)} while {nameof(filterForIce)} is true.");
        }

        using var con = new OleDbConnection(DatabaseConnectionString);
        using OleDbCommand cmd = con.CreateCommand();

        const string IceCodes = "('B','Cocoa','G','RC','Wheat','W')";
        cmd.CommandText = $"SELECT MAX([Report_Date_as_YYYY-MM-DD]) FROM {_tableNameWithinDatabase} Where CFTC_Contract_Market_Code {(filterForIce ? string.Empty : "NOT ")}In {IceCodes};";
        DateTime storedDate = s_defaultStartDate;

        try
        {
            if (DatabaseConnection.State == System.Data.ConnectionState.Closed)
            {
                await con.OpenAsync().ConfigureAwait(false);
            }
            //var cmdResponse = await Task.Run(async () => await cmd.ExecuteScalarAsync()).ConfigureAwait(false);
            //var cmdResponse = await Task.Run(() => cmd.ExecuteScalar());
            var cmdResponse = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
            storedDate = (DateTime?)cmdResponse ?? s_defaultStartDate;
        }
        finally
        {
            await con.CloseAsync().ConfigureAwait(false);
        }
        return storedDate;
    }

    /// <summary>
    /// Queries the database for the latest date found within <see cref="_tableNameWithinDatabase"/> .
    /// </summary>
    /// <param name="filterForIce"><see langword="true"/> if the latest date for ICE data should be returned; otherwise, <see langword="false"/>.</param>
    /// <returns>The most recent DateTime found within the database.</returns>
    DateTime ReturnLatestDateInTable(bool filterForIce)
    {
        if (filterForIce && QueriedReport != ReportType.Disaggregated) throw new InvalidOperationException($"{nameof(QueriedReport)} must be {nameof(ReportType.Disaggregated)} while {nameof(filterForIce)} is true.");
        DateTime storedDate = s_defaultStartDate;

        lock (DatabaseConnection)
        {
            using var con = new OleDbConnection(DatabaseConnectionString);
            using OleDbCommand cmd = con.CreateCommand();

            const string IceCodes = "('B','Cocoa','G','RC','Wheat','W')";
            cmd.CommandText = $"SELECT MAX([Report_Date_as_YYYY-MM-DD]) FROM {_tableNameWithinDatabase} Where CFTC_Contract_Market_Code {(filterForIce ? string.Empty : "NOT ")}In {IceCodes};";

            try
            {
                if (con.State == System.Data.ConnectionState.Closed) con.Open();
                storedDate = (DateTime?)cmd.ExecuteScalar() ?? s_defaultStartDate;
            }
            finally
            {
                con.Close();
            }
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

        string sqlCommand = @$"Update {_tableNameWithinDatabase} as T
                             INNER JOIN [{legacyCombinedInstance._microsoftAccessDatabasePath}].{legacyCombinedInstance._tableNameWithinDatabase} as Source_TBL
                             ON Source_TBL.[Report_Date_as_YYYY-MM-DD]=T.[Report_Date_as_YYYY-MM-DD] AND Source_TBL.[CFTC_Contract_Market_Code]=T.[CFTC_Contract_Market_Code]
                             SET T.[Price] = Source_TBL.[Price] WHERE T.[Report_Date_as_YYYY-MM-DD] > ?;";// AND Source_TBL.[Price] IS NOT ?;";

        using var con = new OleDbConnection(DatabaseConnectionString);
        using OleDbCommand cmd = con.CreateCommand();
        cmd.CommandText = sqlCommand;
        cmd.Parameters.AddWithValue("@GreaterThanDate", DatabaseDateBeforeUpdate);
        // cmd.Parameters.AddWithValue("nullPrice", DBNull.Value);

        //lock (DatabaseConnection)
        //{
        try
        {
            if (con.State == System.Data.ConnectionState.Closed) con.Open();
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
            con.Close();
        }
        //}
        return true;
    }

    /// <summary>
    /// Queries the database for the field names found within the <see cref="_tableNameWithinDatabase"/> table.
    /// </summary>
    /// <returns>A list of field names found within the database.</returns>
    async Task<List<string>> QueryDatabaseFieldNamesAsync()
    {
        List<string> fieldNames = new();
        //lock (DatabaseConnection)
        using var con = new OleDbConnection(DatabaseConnectionString);
        try
        {
            if (con.State == System.Data.ConnectionState.Closed) await con.OpenAsync().ConfigureAwait(false);
            using OleDbCommand cmd = con.CreateCommand();
            cmd.CommandText = $"SELECT TOP 1 * FROM {_tableNameWithinDatabase};";

            using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

            fieldNames = (from columnSchema in await reader.GetColumnSchemaAsync()
                          let fieldName = columnSchema.ColumnName
                          where fieldName.Equals("id", StringComparison.InvariantCultureIgnoreCase) == false
                          select fieldName).ToList();

            await reader.CloseAsync().ConfigureAwait(false);
        }
        finally
        {
            //Console.WriteLine(fieldNames.Count);
            //DatabaseConnection.Close();
            await con.CloseAsync().ConfigureAwait(false);
        }
        //}
        return fieldNames;
    }

    public void Dispose()
    {
        try
        {
            if (DatabaseConnection.State == System.Data.ConnectionState.Open) DatabaseConnection.Close();
        }
        catch (ObjectDisposedException)
        {
        }
        DatabaseConnection.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Maps columns from <paramref name ="externalHeaders"/> to columns within <paramref name="databaseFields"/> based on their names.
    /// </summary>
    /// <param name="externalHeaders">Array of field names from an external source that need to be aligned with database fields.</param>
    /// <param name="databaseFields">List of field names found within the database.</param>
    /// <param name="iceHeaders"><see langword="true"/> if <paramref name="externalHeaders"/> are from an ICE Commitments of Traders report; otherwise, <see langword="false"/> for CFTC reports.</param>
    /// <returns>Returns a dictionary of <see cref="FieldInfo"/> instances keyed to their edited names if the field exists in both <paramref name="externalHeaders"/> and <paramref name="databaseFields"/>; otherwise, a value of null.</returns> 
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
                        string endingStrippedName = editedTableFieldName.Replace(endings[primaryEndingsIndex], string.Empty);
                        // _1 can represent old or other so it's important that its addition be independent of primaryEndingsIndex.
                        byte digitIncrement = 0;
                        for (byte secondaryEndingsIndex = primaryEndingsIndex; secondaryEndingsIndex < endings.Length; ++secondaryEndingsIndex)
                        {
                            string? newKey = null;
                            if (secondaryEndingsIndex == primaryEndingsIndex && headerIndexesByEditedName.Remove(endingStrippedName, out columnIndex))
                            {
                                newKey = editedTableFieldName;
                            }
                            else if (secondaryEndingsIndex > primaryEndingsIndex && headerIndexesByEditedName.Remove(endingStrippedName + $"_{++digitIncrement}", out columnIndex))
                            {
                                newKey = endingStrippedName + endings[secondaryEndingsIndex];
                            }

                            if (!string.IsNullOrEmpty(newKey)) fieldInfoByEditedName[newKey] = new FieldInfo(columnIndex, originalFieldByEditedName[newKey], newKey);
                        }
                    }
                }
            }
        }
        return fieldInfoByEditedName;
    }
    /// <summary>
    /// Queries the database for the most recent date and assigns the DatabaseDateBeforeUpdate
    /// </summary>
    /// <param name="availableReports"></param>    
    public static void GetAllDates(List<Report> availableReports)
    {
        var commandToSend = new List<string>();
        foreach (var report in availableReports)
        {
            commandToSend.Add($"Select '{report.Id}' as Name,MAX([Report_Date_as_YYYY-MM-DD]) From [{report._microsoftAccessDatabasePath}].{report._tableNameWithinDatabase}");
        }

        try
        {
            if (availableReports[0].DatabaseConnection.State == System.Data.ConnectionState.Closed)
            {
                availableReports[0].DatabaseConnection.Open();
            }
            using var cmd = availableReports[0].DatabaseConnection.CreateCommand();
            cmd.CommandText = string.Join(" UNION ", commandToSend) + ';';
            using var reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                var rowId = reader.GetString(0);
                Report report = (from rp in availableReports
                                 where rp.Id.Equals(rowId)
                                 select rp).First();
                report.DatabaseDateBeforeUpdate = report.DatabaseDateAfterUpdate = reader.GetDateTime(1);
                report._databaseQueriedForMostRecentDate = true;
            }
        }
        finally
        {
            availableReports[0].DatabaseConnection.Close();
        }
    }

    [GeneratedRegex("[,]{1}(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))")]
    private static partial Regex MyRegex();
    private readonly struct FieldInfo
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

        /// <summary>
        /// Adjusts <see cref="ColumnName"/> so that it is database compatible.
        /// </summary>
        /// <value>Returns <see cref="ColumnName"/> enclosed in square brackets.</value>
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
        static public OleDbType CotFieldType(string editedDatabaseHeader)
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
    /*
/// <summary>
/// Queries the CFTC API and filters for the displayed headers.
/// </summary>
/// <returns>An asynchronous task.</returns>
private async Task<List<string>> GetHeadersFromAPI()
{
var apiMetaDataUri = new Uri($"https://publicreporting.cftc.gov/api/views/");
using var client = new HttpClient() { BaseAddress = apiMetaDataUri };
string jsonResponse = await client.GetStringAsync(_cftcApiCode).ConfigureAwait(false);

var document = JsonNode.Parse(jsonResponse)!;
JsonNode root = document.Root;
JsonArray columnInfo = root["columns"]!.AsArray();

var listOfFields = (from column in columnInfo
where !column["fieldName"]!.Equals("id")
select column["name"]?.ToString().ToLower().Replace(' ','_')!.ToList();

return listOfFields;
}
private void CreateDatabase()
{
var cat = new ADOX.Catalog();
cat.Create(DatabaseConnectionString);
}
*/
    /*
    [GeneratedRegex("[,]{1}(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))")]
    private static partial Regex SplitOnCommaNotWithinQuotesRegex();
    */
}