namespace ReportRetriever;

using System.Net.Http.Headers;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using System.Globalization;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using System.Data;

public enum ReportType
{
    Legacy, Disaggregated, TFF
}
public enum ReportStatusCode
{
    NoUpdateAvailable, Updated, Failure, AttemptingRetrieval, AttemptingUpdate, NotInitialized, CheckingDataAvailability
}

public partial class Report
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
    /// The table name to target within the database located at <see cref="_microsoftAccessDatabasePath"/>.
    /// </summary>
    private readonly string _tableNameWithinDatabase;

    /// <summary>
    /// Gets an identification code used to target the relevant CFTC API code.
    /// </summary>
    private readonly string _cftcApiCode;

    /// <summary>
    /// Number used to count how many threads are attempting to retrieve ICE data.
    /// </summary>
    private static int s_activeIceRetrievalCount = 0;

    /// <summary>
    /// DateTime returned when an instance for Legacy_Combined data queries the database for the most recent date.
    /// </summary>
    /// <remarks>Assigned a value in <see cref="CommitmentsOfTradersRetrievalAndUploadAsync"/></remarks>
    private static DateTime s_retrievalLockingDate;

    /// <summary>
    /// Dictionary used to store downloaded ICE COT data regardless of <see cref="RetrieveCombinedData"/>'s value.
    /// </summary>
    private static readonly ConcurrentDictionary<string, Task<Dictionary<DateTime, List<string[]>>>?> s_iceCsvRawData = new();

    /// <summary>
    /// Dictionary used to map ICE column names to their respective indexes within the Disaggregated database.
    /// </summary>
    private static Dictionary<string, FieldInfo>? s_iceColumnMap = null;

    /// <summary>
    /// Gets a <see cref="OleDbConnection"/> object that correlates to this instance's <see cref="QueriedReport"/>.
    /// </summary>
    private static SqlConnection? s_databaseConnection;

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
    private const string DatabaseConnectionString = "Data Source=Campbell-PC\\SQLEXPRESS01;Initial Catalog=Commitments_Of_Traders_MoshiM;Trusted_Connection=True;TrustServerCertificate=True;MultipleActiveResultSets=True";

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
    /// <value>The largest date found within <see cref="_tableNameWithinDatabase"/> the table inside the database.</value>
    public DateTime DatabaseDateBeforeUpdate { get; private set; }

    /// <summary>
    /// Gets or sets a DateTimve instance used to keep track of the largest date returned from a CFTC data query.
    /// </summary>
    /// <value>The largest date returned when querying CFTC data.</value>
    public DateTime DatabaseDateAfterUpdate { get; private set; }

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
    /// Boolean that describes if lates date data has been queried from the database.
    /// </summary>
    private bool _mostRecentDateRetrieved = false;

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
    /// <summary>
    /// First letter of report type and C or F for combined or futures only.
    /// </summary>
    public string Id { get => $"{QueriedReport.ToString()[0]}{(RetrieveCombinedData ? 'C' : 'F')}"; }

    private const string IceCodes = "('B','Cocoa','G','RC','Wheat','W')";

    /// <summary>
    /// Initializes a new instance of the Report class with the specified properties.
    /// </summary>
    /// <param name="queriedReport">A <see cref="ReportType"/> enum used to specify what sort of data should be retrieved with this instance.</param>
    /// <param name="retrieveCombinedData"><see langword="true"/> if Futures + Options data should be filtered for; otherwise, <see langword="false"/> for Futures Only data.</param>
    /// <param name="microsoftAccessDatabasePath">File path to database that data should be stored in.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if an <paramref name="queriedReport"/> is out of range.</exception>
    public Report(ReportType queriedReport, bool retrieveCombinedData, bool useDebugMode = false)
    {
        if (!new ReportType[] { ReportType.Legacy, ReportType.Disaggregated, ReportType.TFF }.Any(x => x.Equals(queriedReport)))
        {
            throw new ArgumentOutOfRangeException(nameof(queriedReport), queriedReport, $"Unsupported {nameof(ReportType)} detected.");
        }

        QueriedReport = queriedReport;
        RetrieveCombinedData = retrieveCombinedData;
        DebugActive = useDebugMode;
        IsLegacyCombined = queriedReport == ReportType.Legacy && retrieveCombinedData;

        _tableNameWithinDatabase = $"{queriedReport.ToString()[0]}_{(retrieveCombinedData == true ? "Combined" : "Futures_Only")}";
        _cftcApiCode = s_apiIdentification[retrieveCombinedData][queriedReport];

        s_databaseConnection ??= new SqlConnection(DatabaseConnectionString);
    }

    /// <summary>
    /// Retrieves related data from api and uploads to a local database if new data is retrieved.
    /// </summary>
    /// <param name="yahooPriceSymbolByContractCode">Dictionary of price symbols keyed to cftc contract codes.</param>
    /// <param name="testUpload">If <see langword="true"/> and <see cref="DebugActive"/> is <see langword="true"/> then data upload will be tested.</param>
    /// <returns>An asyc Task.</returns>
    /// <remarks>Price data will only be retrieved if <see cref="IsLegacyCombined"/> is <see langword="true"/>.</remarks>
    /// <exception cref="HttpRequestException">Thrown if unable to connecto to API service.</exception>
    /// <exception cref="KeyNotFoundException">Thrown if a necessary key for data upload wasn't found.</exception>
    /// <exception cref="NullReferenceException">Thrown if a null value is returned for a field necessary for data upload.</exception>
    /// <exception cref="SqlException">Database error.</exception>
    /// <exception cref="IndexOutOfRangeException">Indicates an error in record length returned from source.</exception>
    /// <exception cref="ArgumentException"></exception>
    public async Task CommitmentsOfTradersRetrievalAndUploadAsync(Dictionary<string, string>? yahooPriceSymbolByContractCode, bool testUpload = false, bool userAllowsPriceDownload = false)
    {
        try
        {
            if (!DebugActive && testUpload) throw new ArgumentException($"Cannot test upload if {nameof(DebugActive)} is false.");
            ActionTimer.Start();

            if (!_mostRecentDateRetrieved)
            {
                DatabaseDateBeforeUpdate = DatabaseDateAfterUpdate = await ReturnLatestDateInTableAsync(filterForIce: false).ConfigureAwait(false);
                _mostRecentDateRetrieved = true;
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
                    if (failureCodes.Contains(s_retrievalLockingStatusCode) && DatabaseDateBeforeUpdate >= s_retrievalLockingDate)
                    {
                        CurrentStatus = ReportStatusCode.NoUpdateAvailable;
                        return;
                    }
                    ActionTimer.Start();
                }
                else
                {
                    s_retrievalLockingDate = DatabaseDateBeforeUpdate;
                }
            }

            int queryReturnLimit = DebugActive ? 10 : 20_000;
            // Headers from local database.
            List<string> databaseFieldNames = await QueryDatabaseFieldNamesAsync().ConfigureAwait(false);
            // Dictionary to keep track of wanted date and contract code combinations. 
            Dictionary<string, Dictionary<DateTime, string?>> priceByDateByContractCode = new();
            // (New data from API, Mapped FieldInfo instances for each column or null)
            (List<string[]> cftcData, Dictionary<string, FieldInfo>? cftcFieldInfoByEditedName) = await CftcCotRetrievalAsync(queryReturnLimit, priceByDateByContractCode, databaseFieldNames).ConfigureAwait(false);

            List<string[]>? iceData = null;

            var tasksToWaitFor = new List<Task>();

            if (cftcData.Any() && cftcFieldInfoByEditedName is not null)
            {
                if (QueriedReport == ReportType.Disaggregated)
                {
                    iceData = await IceCotRetrievalAsync(DatabaseDateAfterUpdate, databaseFieldNames, queryReturnLimit).ConfigureAwait(false);
                }

                // Only retrieve price data for Legacy Combined instances since it encompases both Disaggregated and Traders in Financial Futures reports.
                if (IsLegacyCombined && yahooPriceSymbolByContractCode != null && userAllowsPriceDownload)
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
                        tasksToWaitFor.Add(RetrieveYahooPriceDataAsync(yahooPriceSymbolByContractCode, priceByDateByContractCode).ContinueWith(x => UploadPriceDataAsync(priceByDateByContractCode)));
                    }
                }

                if (!DebugActive || testUpload)
                {
                    if (QueriedReport == ReportType.Disaggregated && iceData != null && s_iceColumnMap != null && iceData.Any())
                    {
                        try
                        {   // Make an attempt to upload ICE data.
                            tasksToWaitFor.Add(UploadToDatabaseAsync(fieldInfoPerEditedName: s_iceColumnMap, dataToUpload: iceData, true));
                        }
                        catch (Exception e)
                        {   // ICE is low priority so just print the error.
                            Console.WriteLine(e);
                        }
                    }
                    // Make an attempt to upload CFTC data.
                    tasksToWaitFor.Add(UploadToDatabaseAsync(fieldInfoPerEditedName: cftcFieldInfoByEditedName, dataToUpload: cftcData, false));
                }
                if (tasksToWaitFor.Any()) await Task.WhenAll(tasksToWaitFor).ConfigureAwait(false);
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
    private async Task<(List<string[]>, Dictionary<string, FieldInfo>?)> CftcCotRetrievalAsync(int maxRecordsPerLoop, Dictionary<string, Dictionary<DateTime, string?>> priceByDateByContractCode, List<string> databaseFieldNames)
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
        Dictionary<string, FieldInfo>? fieldInfoByEditedName = null;

        string comparisonOperator = DebugActive ? ">=" : ">";
        // Make initial API call to find out how many new records are available. Executed only once.
        var countRecordsUrl = $"{_cftcApiCode}{WantedDataFormat}?$select=count(id)&$where={StandardDateFieldName}{comparisonOperator}'{DatabaseDateBeforeUpdate.ToString(StandardDateFormat)}'";

        string? response = await s_cftcApiClient.GetStringAsync(countRecordsUrl).ConfigureAwait(false);

        remainingRecordsToRetrieve = int.Parse(response.Split('\n')[1].Trim(s_charactersToTrim), NumberStyles.Number, null);

        if (remainingRecordsToRetrieve == 0)
        {
            CurrentStatus = ReportStatusCode.NoUpdateAvailable;
        }
        else if (DebugActive)
        {
            remainingRecordsToRetrieve = Math.Min(maxRecordsPerLoop, remainingRecordsToRetrieve);
        }

        while (remainingRecordsToRetrieve > 0)
        {
            CurrentStatus = ReportStatusCode.AttemptingRetrieval;
            string apiDetails = $"{_cftcApiCode}{WantedDataFormat}?$where={StandardDateFieldName}{comparisonOperator}'{DatabaseDateBeforeUpdate.ToString(StandardDateFormat)}'&$order=id&$limit={maxRecordsPerLoop}&$offset={offsetCount++}";
            response = await s_cftcApiClient.GetStringAsync(apiDetails).ConfigureAwait(false);

            // Data from the API tends to have an extra line at the end so trim it.
            responseLines = response.Trim('\n').Split('\n');
            response = null;
            // Subtract 1 to account for headers.
            remainingRecordsToRetrieve -= responseLines.Length - 1;

            fieldInfoByEditedName ??= MapHeaderFieldsToDatabase(externalHeaders: SplitOnCommaNotWithinQuotesRegex().Split(responseLines[0]), databaseFields: databaseFieldNames, iceHeaders: false);

            int cftcDateColumn = fieldInfoByEditedName[$"@{StandardDateFieldName}"].ColumnIndex;
            int cftcCodeColumn = fieldInfoByEditedName[$"@{ContractCodeColumnName}"].ColumnIndex;

            // Start index at 1 rather than 0 to skip over headers.
            for (var i = 1; i < responseLines.Length; ++i)
            {
                if (!string.IsNullOrEmpty(responseLines[i]))
                {
                    string[] apiRecord = SplitOnCommaNotWithinQuotesRegex().Split(responseLines[i]).Select(x => x.Trim(s_charactersToTrim)).ToArray();

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
        DateTime maxIceDateInDatabase = await ReturnLatestDateInTableAsync(filterForIce: true).ConfigureAwait(false);

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
        int futOrCombinedColumn = (int)s_iceColumnMap!["futonly_or_combined"].ColumnIndex!;
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
                    iceCsvRecord = SplitOnCommaNotWithinQuotesRegex().Split(await sr.ReadLineAsync().ConfigureAwait(false) ?? throw new NullReferenceException("Empty iceCsvRecord"));
                }
                catch (NullReferenceException)
                {
                    continue;
                }

                if (!foundHeaders)
                {
                    foundHeaders = true;
                    s_iceColumnMap ??= MapHeaderFieldsToDatabase(iceCsvRecord, databaseHeaders, true);
                    iceDateColumn = s_iceColumnMap[$"@{StandardDateFieldName}"].ColumnIndex!;

                    iceShortDateColumn = s_iceColumnMap.First(x => x.Key.Contains(IceShortDateFormat, StringComparison.InvariantCultureIgnoreCase)).Value.ColumnIndex!;
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
    /// Uploads price data to the PriceData table.
    /// </summary>
    /// <param name="priceDataByDateByContractCode">Dictionary that contains data to upload.</param>
    /// <returns>Async Task.</returns>
    static async Task UploadPriceDataAsync(Dictionary<string, Dictionary<DateTime, string?>> priceDataByDateByContractCode)
    {
        SqlCommand cmd = new SqlCommandBuilder(new SqlDataAdapter("Select * From PriceData", s_databaseConnection!)).GetInsertCommand(true);

        foreach (string contractCode in priceDataByDateByContractCode.Keys)
        {
            cmd.Parameters[$"@{ContractCodeColumnName}"].Value = contractCode;
            foreach (DateTime onDate in priceDataByDateByContractCode[contractCode].Keys)
            {
                string? fieldValue = priceDataByDateByContractCode[contractCode][onDate];
                if (!string.IsNullOrEmpty(fieldValue))
                {
                    cmd.Parameters[$"@{StandardDateFieldName}"].Value = onDate;
                    cmd.Parameters["@Price"].Value = decimal.Parse(fieldValue);
                    try
                    {
                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                    catch (SqlException e)
                    {
                        // Throw if not primary key violation.
                        if (e.Number != 2627) throw;
                    }
                }
            }
        }
    }
    /// <summary>
    /// Uploads string arrays found within <paramref name="dataToUpload"/> to the database.
    /// </summary>
    /// <param name="fieldInfoPerEditedName">Dictionary that maps column names within the database to the equivalent column index within an array in <paramref name="dataToUpload"/>.</param>
    /// <param name="dataToUpload">A list of string arrays that data will be pulled from and uploaded to the database.</param>
    /// <remarks>Method isn't asynchronous because attempts to use the same <see cref="s_databaseConnection"/> instance from different threads will result in an error.</remarks>
    /// <exception cref="KeyNotFoundException">Thrown if an unknown key is used to access <paramref name="fieldInfoPerEditedName"/>.</exception>
    /// <exception cref="IndexOutOfRangeException">Thrown if a wanted index is out of bounds fora an array in <paramref name="dataToUpload"/>.</exception>
    /// <exception cref="SqlException">Error related to database interaction.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if a field type is unaccounted for when assigning a value to a parameter or if <paramref name="fieldInfoPerEditedName"/> contains only null values.</exception>    
    /// <exception cref="InvalidOperationException">Thrown if an error occurs while attempting to comit a database transaction.</exception> 
    async Task UploadToDatabaseAsync(Dictionary<string, FieldInfo> fieldInfoPerEditedName, List<string[]> dataToUpload, bool uploadingIceData)
    {
        CurrentStatus = ReportStatusCode.AttemptingUpdate;

        using var conn = new SqlConnection(s_databaseConnection!.ConnectionString);
        SqlTransaction? transaction = null;
        try
        {
            conn.Open();
            using SqlCommand cmd = new SqlCommandBuilder(new SqlDataAdapter($"Select * From {_tableNameWithinDatabase}", conn)).GetInsertCommand(true);
            transaction = conn.BeginTransaction();
            cmd.Transaction = transaction;
            bool successfullyInsertedRecords = false;
            // For each row of data, assign values to wanted parameters.
            // var year3000 = new DateOnly(3000, 1, 1);
            foreach (string[] dataRow in dataToUpload)
            {
                foreach (SqlParameter param in cmd.Parameters)
                {
                    string? fieldValue = fieldInfoPerEditedName.TryGetValue(param.ParameterName, out FieldInfo knownField) ? dataRow[knownField.ColumnIndex] : null;

                    if (string.IsNullOrEmpty(fieldValue))
                    {
                        param.Value = DBNull.Value;
                    }
                    else
                    {
                        param.Value = param.SqlDbType switch
                        {
                            SqlDbType.Int or SqlDbType.SmallInt or SqlDbType.TinyInt => int.Parse(fieldValue, NumberStyles.AllowDecimalPoint | NumberStyles.AllowThousands | NumberStyles.AllowLeadingSign),
                            SqlDbType.SmallMoney or SqlDbType.Decimal => decimal.Parse(fieldValue),
                            SqlDbType.VarChar => fieldValue,
                            SqlDbType.Date => DateTime.ParseExact(fieldValue, StandardDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None),
                            _ => throw new ArgumentOutOfRangeException(nameof(fieldInfoPerEditedName), param.SqlDbType, $"An unaccounted for SqlDbType was encountered when accessing {param.ParameterName}.")
                        };
                    }
                }

                try
                {
                    await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                    successfullyInsertedRecords = true;
                }
                catch (SqlException e)
                {
                    // If not a duplicate Primary Key error.
                    if (e.Number != 2627) throw;
                }
            }
            transaction.Commit();
            if (!uploadingIceData) CurrentStatus = successfullyInsertedRecords ? ReportStatusCode.Updated : ReportStatusCode.Failure;
        }
        catch (Exception)
        {
            if (transaction is not null)
            {
                try
                {
                    transaction.Rollback();
                }
                catch (Exception ex2)
                {
                    Console.WriteLine("Rollback Exception Type: {0}\n Message: {1}", ex2.GetType(), ex2.Message);
                }
                transaction.Dispose();
            }
            throw;
        }
        finally
        {
            if (conn.State == ConnectionState.Open) conn.Close();
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
        var headers = priceRetrievalClient.DefaultRequestHeaders;
        headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/csv"));
        headers.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0");

        const byte YahooDateColumn = 0, YahooCloseColumn = 5;

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
                    response = await priceRetrievalClient.GetStringAsync(urlDetails).ConfigureAwait(false);
                }
                catch (HttpRequestException)
                {
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

        using SqlCommand cmd = s_databaseConnection!.CreateCommand();

        cmd.CommandText = $"SELECT MAX({StandardDateFieldName}) FROM {_tableNameWithinDatabase} Where {ContractCodeColumnName} {(filterForIce ? string.Empty : "NOT ")}In {IceCodes};";
        DateTime storedDate = s_defaultStartDate;

        lock (s_databaseConnection)
        {
            if (s_databaseConnection!.State == ConnectionState.Closed)
            {
                s_databaseConnection.Open();
            }
        }
        var timer = Stopwatch.StartNew();
        var cmdResponse = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
        timer.Stop();
        storedDate = (DateTime?)cmdResponse ?? s_defaultStartDate;

        return storedDate;
    }

    /// <summary>
    /// Queries the database for the field names found within the <see cref="_tableNameWithinDatabase"/> table.
    /// </summary>
    /// <returns>A list of field names found within the database.</returns>
    async Task<List<string>> QueryDatabaseFieldNamesAsync()
    {
        List<string> fieldNames = new();

        using var con = new SqlConnection(DatabaseConnectionString);
        try
        {
            if (con.State == ConnectionState.Closed) await con.OpenAsync().ConfigureAwait(false);
            using SqlCommand cmd = con.CreateCommand();
            cmd.CommandText = $"SELECT TOP 1 * FROM {_tableNameWithinDatabase};";

            using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

            fieldNames = (from columnSchema in await reader.GetColumnSchemaAsync()
                          let fieldName = columnSchema.ColumnName
                          select fieldName).ToList();

            await reader.CloseAsync().ConfigureAwait(false);
        }
        finally
        {
            //Console.WriteLine(fieldNames.Count);
            //s_databaseConnection.Close();
            await con.CloseAsync().ConfigureAwait(false);
        }
        //}
        return fieldNames;
    }

    /// <summary>
    /// Disposes the OleDbConnection associated with this instance.
    /// </summary>
    public static void DisposeConnection()
    {
        if (s_databaseConnection != null)
        {
            if (s_databaseConnection.State == ConnectionState.Open) s_databaseConnection.Close();
            s_databaseConnection.Dispose();
            s_databaseConnection = null;
        }
    }

    /// <summary>
    /// Maps columns from <paramref name ="externalHeaders"/> to columns within <paramref name="databaseFields"/> based on their names.
    /// </summary>
    /// <param name="externalHeaders">Array of field names from an external source that need to be aligned with database fields.</param>
    /// <param name="databaseFields">List of field names found within the database.</param>
    /// <param name="iceHeaders"><see langword="true"/> if <paramref name="externalHeaders"/> are from an ICE Commitments of Traders report; otherwise, <see langword="false"/> for CFTC reports.</param>
    /// <returns>Returns a dictionary of <see cref="FieldInfo"/> instances keyed to their edited names if the field exists in both <paramref name="externalHeaders"/> and <paramref name="databaseFields"/>; otherwise, a value of null.</returns> 
    static Dictionary<string, FieldInfo> MapHeaderFieldsToDatabase(string[] externalHeaders, List<string> databaseFields, bool iceHeaders)
    {
        // return dictionary keyed to fields within databaseFields with wanted FieldInfo structs as a value 
        Dictionary<string, FieldInfo> fieldInfoByEditedName = new();
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
            if (editedTableFieldName.Equals("cftc_region_code") == false)
            {
                originalFieldByEditedName.Add(editedTableFieldName, databaseFields[i]);
                //fieldInfoByEditedName.Add(editedTableFieldName, null);
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
            FieldInfo fieldDeclaration;

            if (headerIndexesByEditedName.Remove(editedTableFieldName, out columnIndex))
            {
                fieldDeclaration = new FieldInfo(columnIndex, databaseFieldName, editedTableFieldName);
                fieldInfoByEditedName[fieldDeclaration.ParamName] = fieldDeclaration;
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

                            if (!string.IsNullOrEmpty(newKey))
                            {
                                fieldDeclaration = new FieldInfo(columnIndex, originalFieldByEditedName[newKey], editedColumnName: newKey);
                                fieldInfoByEditedName[fieldDeclaration.ParamName] = fieldDeclaration;
                                originalFieldByEditedName.Remove(newKey);
                            }
                        }
                    }
                }
            }
        }
        return fieldInfoByEditedName;
    }
    /*
    /// <summary>
    /// Queries the database for the most recent date and assigns the DatabaseDateBeforeUpdate
    /// </summary>
    /// <param name="availableReports">List of <see cref="Report"/> instances that will be queried for their latest dates.</param>    
    public static void GetAllDates(List<Report> availableReports)
    {
        var commandToSend = new List<string>();
        foreach (var report in availableReports)
        {
            commandToSend.Add($"Select '{report.Id}' as Name,MAX({StandardDateFieldName}) From {report._tableNameWithinDatabase}");
        }

        try
        {
            if (availableReports[0].s_databaseConnection.State == System.Data.ConnectionState.Closed)
            {
                availableReports[0].s_databaseConnection.Open();
            }
            using var cmd = availableReports[0].s_databaseConnection.CreateCommand();
            cmd.CommandText = string.Join(" UNION ", commandToSend) + ';';
            using var reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                var rowId = reader.GetString(0);
                Report report = (from rp in availableReports
                                 where rp.Id.Equals(rowId)
                                 select rp).First();
                report.DatabaseDateBeforeUpdate = report.DatabaseDateAfterUpdate = reader.GetDateTime(1);
                report._mostRecentDateRetrieved = true;
            }
        }
        finally
        {
            availableReports[0].s_databaseConnection.Close();
        }
    }
    */
    public Dictionary<string, object> Summarized()
    {
        return new Dictionary<string, object>(){
            {"Latest Date",$"{DatabaseDateAfterUpdate:yyyy-MM-ddTHH:mm:ssZ}"},
            {"Time Elapsed (ms)", ActionTimer.ElapsedMilliseconds},
            {"Status", (int)CurrentStatus}
        };
    }
    private readonly struct FieldInfo
    {
        /// <summary>
        /// Column Index of the given field within an array of data.
        /// </summary>
        public int ColumnIndex { get; }

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

        public string ParamName { get; }

        public FieldInfo(int columnIndex, string columnName, string editedColumnName)
        {
            ColumnIndex = columnIndex;
            ColumnName = columnName;
            EditedColumnName = editedColumnName;
            ParamName = $"@{ColumnName}";
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

    [GeneratedRegex("[,]{1}(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))")]
    private static partial Regex SplitOnCommaNotWithinQuotesRegex();
}