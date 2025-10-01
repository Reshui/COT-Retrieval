namespace ReportRetriever;

using System.Net.Http.Headers;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using System.Globalization;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Text.Json.Nodes;
using System.Text;
using System.Text.Json;

//using System.Configuration; .. use to store connection string?

public enum ReportType
{
    Legacy, Disaggregated, TFF
}
public enum ReportStatusCode
{
    NoUpdateAvailable, Updated, Failure, AttemptingRetrieval, AttemptingUpdate, NotInitialized, CheckingDataAvailability,
    OnlyDuplicateRecordsInUpdate, LockingInstanceFailure, FoundNewData
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
    /// Name of the database that the program will interact with.
    /// </summary>
    private const string DatabaseName = "Commitments_Of_Traders_MoshiM";

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
    /// The table name to target within the database.
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
    /// <see cref="SqlConnection"/> used to connect to the server.
    /// </summary>
    private static SqlConnection? s_databaseConnection;

    /// <summary>
    /// Boolean used to control whether or not an instance where <see cref="IsLegacyCombined"/> = <see langword="false"/> can be released from its waiting loop.
    /// </summary>
    public bool ReleaseLockedInstances
    {
        get => s_releaseLockedInstances;
        private set
        {
            if (IsLegacyCombined) s_releaseLockedInstances = value;
        }
    }

    private static bool s_releaseLockedInstances = false;

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
    private readonly static char[] s_charactersToTrim = [' ', '\"'];
    /// <summary>
    /// First letter of report type and C or F for combined or futures only.
    /// </summary>
    public string Id { get => $"{QueriedReport.ToString()[0]}{(RetrieveCombinedData ? 'C' : 'F')}"; }
    /// <summary>
    /// SQL query text used when checking if a contract code is from the ICE COT reports.
    /// </summary>
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

        if (s_databaseConnection == null)
        {
            var builder = new SqlConnectionStringBuilder()
            {
                DataSource = ".\\SQLEXPRESS",
                IntegratedSecurity = true,
                PersistSecurityInfo = false,
                MultipleActiveResultSets = true,
                TrustServerCertificate = true,
                ConnectTimeout = 30,
                ApplicationName = "C# Exe",
            };
            
            s_databaseConnection = new SqlConnection(builder.ConnectionString);
            using var cmd = s_databaseConnection.CreateCommand();
            
            cmd.CommandText = $"IF NOT Exists(Select name from sys.databases where name=@database) BEGIN CREATE DATABASE {DatabaseName}; END;";
            cmd.Parameters.AddWithValue("@database", DatabaseName);

            s_databaseConnection.Open();
            cmd.ExecuteNonQuery();
            s_databaseConnection.ChangeDatabase(DatabaseName);
            cmd.CommandText = $"IF NOT EXISTS (Select name FROM sys.Tables WHERE name='PriceData') BEGIN CREATE TABLE PriceData (report_date_as_yyyy_mm_dd Date NOT NULL, cftc_contract_market_code VARCHAR(10) NOT NULL, price smallmoney NOT NULL, Primary Key (report_date_as_yyyy_mm_dd, cftc_contract_market_code)); END;";
            cmd.Parameters.Clear();
            cmd.ExecuteNonQuery();
        }
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

            await CreateReportTableIfMissingAsync().ConfigureAwait(false);

            DatabaseDateBeforeUpdate = DatabaseDateAfterUpdate = await GetLatestTableDateAsync(filterForIce: false).ConfigureAwait(false);
            bool checkIceOnly = false;
            if (!DebugActive)
            {
                // Wait until after the Legacy_Combined instance has attempted CFTC retrieval before continuing.
                if (!IsLegacyCombined)
                {
                    // Loop until a change in state is detected in the running Legacy Combined instance.
                    ActionTimer.Stop();
                    while (!ReleaseLockedInstances)
                    {
                        await Task.Delay(300).ConfigureAwait(false);
                    }

                    var failureCodes = new ReportStatusCode[] { ReportStatusCode.NoUpdateAvailable, ReportStatusCode.Failure };

                    if (failureCodes.Contains(s_retrievalLockingStatusCode) && DatabaseDateBeforeUpdate >= s_retrievalLockingDate)
                    {
                        // If lockin instance failed then use the appropriate enum else assign no update available.
                        CurrentStatus = (s_retrievalLockingStatusCode == ReportStatusCode.Failure) ? ReportStatusCode.LockingInstanceFailure : ReportStatusCode.NoUpdateAvailable;
                        checkIceOnly = CurrentStatus == ReportStatusCode.NoUpdateAvailable && QueriedReport == ReportType.Disaggregated;
                        if (!checkIceOnly) return;
                    }
                    ActionTimer.Start();
                }
                else
                {
                    s_retrievalLockingDate = DatabaseDateBeforeUpdate;
                }
            }
            // Headers from local database.
            List<string> databaseFieldNames = await QueryDatabaseFieldNamesAsync().ConfigureAwait(false);

            // (New data from API, Mapped FieldInfo instances for each column or null)
            Task<(List<string[]>, Dictionary<string, FieldInfo>?, Dictionary<string, Dictionary<DateTime, decimal?>>?)>? cftcRetrievalTask = null;
            var tasksToWaitFor = new List<Task>();
            int queryReturnLimit = DebugActive ? 1_000 : 20_000;

            if (!checkIceOnly)
            {
                cftcRetrievalTask = CftcCotRetrievalAsync(queryReturnLimit, databaseFieldNames);
                tasksToWaitFor.Add(cftcRetrievalTask);
            }

            Task<List<string[]>?>? iceRetrievalTask = null;

            if (QueriedReport == ReportType.Disaggregated)
            {
                // await cftc retrieval if not just checking ICE as DatabaseDateAfterUpdate may update.
                if (!checkIceOnly && cftcRetrievalTask != null) await cftcRetrievalTask.ConfigureAwait(false);

                iceRetrievalTask = IceCotRetrievalAsync(DatabaseDateAfterUpdate, databaseFieldNames, queryReturnLimit);
                tasksToWaitFor.Add(iceRetrievalTask);
            }

            bool permitUpload = !DebugActive || testUpload;

            while (tasksToWaitFor.Count != 0)
            {
                Task completedTask = await Task.WhenAny(tasksToWaitFor).ConfigureAwait(false);

                if (completedTask == cftcRetrievalTask)
                {
                    (var cftcData, var cftcFieldInfoByEditedName, var priceByDateByContractCode) = await cftcRetrievalTask.ConfigureAwait(false);

                    if (cftcData.Count != 0 && cftcFieldInfoByEditedName is not null)
                    {
                        // Only retrieve price data for Legacy Combined instances since it encompases both Disaggregated and Traders in Financial Futures reports.

                        if (yahooPriceSymbolByContractCode != null && IsLegacyCombined && userAllowsPriceDownload && (priceByDateByContractCode?.Count ?? 0) > 0)
                        {
                            bool retrievePrices = true;
                            if (DebugActive)
                            {
                                Console.WriteLine("Do you want to test price retrieval(Y/N)?");
                                var keyResponse = Console.ReadKey(true);
                                retrievePrices = keyResponse.Key == ConsoleKey.Y;
                            }

                            if (retrievePrices)
                            {
                                tasksToWaitFor.Add(RetrieveAndUploadYahooPriceDataAsync(yahooPriceSymbolByContractCode, priceByDateByContractCode!));
                            }
                        }

                        if (permitUpload)
                        {
                            // Make an attempt to upload CFTC data.
                            tasksToWaitFor.Add(UploadToDatabaseAsync(fieldInfoPerEditedName: cftcFieldInfoByEditedName, dataToUpload: cftcData, false));
                        }
                    }
                }
                else if (completedTask == iceRetrievalTask)
                {
                    try
                    {
                        var iceData = await iceRetrievalTask.ConfigureAwait(false);
                        if (permitUpload && s_iceColumnMap != null && ((iceData?.Count ?? 0) > 0))
                        {
                            tasksToWaitFor.Add(UploadToDatabaseAsync(fieldInfoPerEditedName: s_iceColumnMap, dataToUpload: iceData!, true));
                        }
                    }
                    catch (Exception e1)
                    {
                        Console.WriteLine(e1);
                    }
                }
                else
                {   // await task to catch any errors.
                    await completedTask.ConfigureAwait(false);
                }
                tasksToWaitFor.Remove(completedTask);
            }
        }
        catch (Exception)
        {
            CurrentStatus = ReportStatusCode.Failure;
            DatabaseDateAfterUpdate = DatabaseDateBeforeUpdate;
            throw;
        }
        finally
        {
            ActionTimer.Stop();
            ReleaseLockedInstances = true;
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
    private async Task<(List<string[]>, Dictionary<string, FieldInfo>?, Dictionary<string, Dictionary<DateTime, decimal?>>?)> CftcCotRetrievalAsync(int maxRecordsPerLoop, List<string> databaseFieldNames)
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
        List<string[]> newCftcData = [];
        // Dictionary will hold mapped FieldInfo instances for fields within the API and local database.
        Dictionary<string, FieldInfo>? fieldInfoByEditedName = null;
        // This Dictionary will price dictionaries keyed to each contracts contract code.
        Dictionary<string, Dictionary<DateTime, decimal?>>? priceByDateByContractCode = IsLegacyCombined ? [] : null;

        string comparisonOperator = DebugActive ? ">=" : ">";
        // Make initial API call to find out how many new records are available. Executed only once.
        var countRecordsUrl = $"{_cftcApiCode}{WantedDataFormat}?$select=count(id)&$where={StandardDateFieldName}{comparisonOperator}'{DatabaseDateBeforeUpdate.ToString(StandardDateFormat)}'";

        CurrentStatus = ReportStatusCode.CheckingDataAvailability;

        string? response = await s_cftcApiClient.GetStringAsync(countRecordsUrl).ConfigureAwait(false);

        remainingRecordsToRetrieve = int.Parse(response.Split('\n')[1].Trim(s_charactersToTrim), NumberStyles.Number, null);

        if (remainingRecordsToRetrieve > 0)
        {
            if (DebugActive) remainingRecordsToRetrieve = Math.Min(maxRecordsPerLoop, remainingRecordsToRetrieve);
            CurrentStatus = ReportStatusCode.FoundNewData;
        }
        else
        {
            CurrentStatus = ReportStatusCode.NoUpdateAvailable;
        }

        ReleaseLockedInstances = true;
        while (remainingRecordsToRetrieve > 0)
        {
            CurrentStatus = ReportStatusCode.AttemptingRetrieval;
            string apiDetails = $"{_cftcApiCode}{WantedDataFormat}?$where={StandardDateFieldName}{comparisonOperator}'{DatabaseDateBeforeUpdate.ToString(StandardDateFormat)}'&$order=report_date_as_yyyy_mm_dd,id&$limit={maxRecordsPerLoop}&$offset={offsetCount++}";
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
                    string[] apiRecord = [.. SplitOnCommaNotWithinQuotesRegex().Split(responseLines[i]).Select(x => x.Trim(s_charactersToTrim))];

                    if (DateTime.TryParseExact(apiRecord[cftcDateColumn], StandardDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime parsedDate)
                    && ((parsedDate > DatabaseDateBeforeUpdate && !DebugActive) || (parsedDate >= DatabaseDateBeforeUpdate && DebugActive)))
                    {   // Create a null entry for the current combination of contract code and date within priceByDateByContractCode.
                        if (IsLegacyCombined)
                        {
                            string currentContractCode = apiRecord[cftcCodeColumn];
                            if (!priceByDateByContractCode!.TryGetValue(currentContractCode, out Dictionary<DateTime, decimal?>? priceByDateForContractCode))
                            {
                                priceByDateForContractCode = priceByDateByContractCode[currentContractCode] = [];
                            }
                            priceByDateForContractCode.TryAdd(parsedDate, null);
                        }
                        newCftcData.Add(apiRecord);
                        if (parsedDate > DatabaseDateAfterUpdate) DatabaseDateAfterUpdate = parsedDate;
                    }
                }
            }
            responseLines = null;
        }
        ;
        return (newCftcData, fieldInfoByEditedName, priceByDateByContractCode);
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
        DateTime maxIceDateInDatabase = await GetLatestTableDateAsync(filterForIce: true).ConfigureAwait(false);

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
            for (var csvYear = Math.Max(maxIceDateInDatabase.Year, IceStartYear); csvYear <= mostRecentCftcDate.Year; ++csvYear)
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
            await Task.Delay(300).ConfigureAwait(false);
        }

        await Task.WhenAll(s_iceCsvRawData.Values!).ConfigureAwait(false);
        // Filter for data relevant to the current instance and is more recent than what is stored in the database.
        var iceQuery = from kvp in s_iceCsvRawData
                       let weeklyTaskLocated = kvp.Key.Equals(WeeklyIceKey)
                       where ((singleWeekRetrieval && weeklyTaskLocated) || (!singleWeekRetrieval && !weeklyTaskLocated)) && !kvp.Value.IsFaulted
                       from recordsByDateTime in kvp.Value.Result
                       where (!DebugActive && recordsByDateTime.Key > maxIceDateInDatabase) || (DebugActive && recordsByDateTime.Key >= maxIceDateInDatabase)
                       from row in recordsByDateTime.Value
                       let oiTypeColumnIndex = row.Length - 1
                       where row[oiTypeColumnIndex].Equals(RetrieveCombinedData ? "combined" : "futonly", StringComparison.InvariantCultureIgnoreCase)
                       select row;

        return DebugActive ? [.. iceQuery.Take(debugReturnLimit)] : [.. iceQuery];
    }

    /// <summary>
    /// Queries the International Continental Exchange website for Commitments of Traders data.
    /// </summary>
    /// <param name="iceCsvUrl">URL for the csv file to download.</param>
    /// <param name="databaseHeaders">Headers from the Disaggregated report found within the database.</param>
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
            using var iceStreamReader = new StreamReader(stream);
            int iceDateColumn = -1;
            const string IceShortDateFormat = "yyMMdd";
            while (!iceStreamReader.EndOfStream)
            {
                string[]? iceCsvRecord;

                try
                {
                    iceCsvRecord = SplitOnCommaNotWithinQuotesRegex().Split(await iceStreamReader.ReadLineAsync().ConfigureAwait(false) ?? throw new NullReferenceException("Empty iceCsvRecord"));
                }
                catch (NullReferenceException)
                {
                    continue;
                }
                catch (HttpRequestException e)
                {
                    Console.WriteLine(e);
                    throw;
                }

                if (!foundHeaders)
                {
                    s_iceColumnMap ??= MapHeaderFieldsToDatabase(iceCsvRecord, databaseHeaders, true);
                    if (s_iceColumnMap.Count != 0)
                    {
                        iceDateColumn = s_iceColumnMap[$"@{StandardDateFieldName}"].ColumnIndex!;
                        iceShortDateColumn = iceDateColumn - 1;
                        foundHeaders = true;
                    }
                    else
                    {
                        throw new Exception($"{nameof(s_iceColumnMap)} contains no key-value pairs.");
                    }
                }
                else if (DateTime.TryParseExact(iceCsvRecord[iceShortDateColumn], IceShortDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out DateTime parsedDate))
                {
                    iceCsvRecord = [.. iceCsvRecord.Select(x => x.Trim(s_charactersToTrim))];
                    iceCsvRecord[iceDateColumn] = parsedDate.ToString(StandardDateFormat);

                    if (!csvDataByDateTime.TryGetValue(parsedDate, out List<string[]>? dataGroupedByDate))
                    {
                        dataGroupedByDate = csvDataByDateTime[parsedDate] = [];
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
    /// <param name="priceByDateByContractCode">Dictionary that contains data to upload.</param>
    /// <returns>Async Task.</returns>
    static async Task UploadPriceDataAsync(Dictionary<string, Dictionary<DateTime, decimal?>> priceByDateByContractCode)
    {
        if (s_databaseConnection != null)
        {
            SqlCommand cmd = new SqlCommandBuilder(new SqlDataAdapter($"Select * From PriceData", s_databaseConnection!)).GetInsertCommand(true);

            SqlParameter contractCodeParameter = cmd.Parameters[$"@{ContractCodeColumnName}"];
            SqlParameter dateParameter = cmd.Parameters[$"@{StandardDateFieldName}"];
            SqlParameter priceParameter = cmd.Parameters["@price"];

            foreach (var codeAndDictKvp in priceByDateByContractCode)
            {
                contractCodeParameter.Value = codeAndDictKvp.Key;
                foreach (var kvpInner in codeAndDictKvp.Value)
                {
                    if (kvpInner.Value != null)
                    {
                        dateParameter.Value = kvpInner.Key;
                        priceParameter.Value = kvpInner.Value;
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
            await conn.OpenAsync().ConfigureAwait(false);
            await conn.ChangeDatabaseAsync(DatabaseName).ConfigureAwait(false);
            using SqlCommand cmd = new SqlCommandBuilder(new SqlDataAdapter($"Select * From {_tableNameWithinDatabase}", conn)).GetInsertCommand(true);
            transaction = conn.BeginTransaction();
            cmd.Transaction = transaction;
            bool successfullyInsertedRecords = false;
            int duplicateInsertionCount = 0;
            // For each row of data, assign values to wanted parameters.
            foreach (string[] dataRow in dataToUpload)
            {
                foreach (SqlParameter param in cmd.Parameters)
                {
                    string? fieldValue = fieldInfoPerEditedName.TryGetValue(param.ParameterName, out FieldInfo knownField) ? dataRow[knownField.ColumnIndex] : null;

                    if (string.IsNullOrEmpty(fieldValue)) param.Value = DBNull.Value;
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
                    else ++duplicateInsertionCount;
                }
            }
            transaction.Commit();
            if (!uploadingIceData)
            {
                CurrentStatus = successfullyInsertedRecords ? ReportStatusCode.Updated : (duplicateInsertionCount == dataToUpload.Count) ? ReportStatusCode.OnlyDuplicateRecordsInUpdate : ReportStatusCode.Failure;
            }
        }
        catch (Exception)
        {
            try
            {
                transaction?.Rollback();
            }
            catch (Exception ex2)
            {
                Console.WriteLine("Rollback Exception Type: {0}\n Message: {1}", ex2.GetType(), ex2.Message);
            }
            throw;
        }
        finally
        {
            transaction?.Dispose();
            if (conn.State == ConnectionState.Open) conn.Close();
        }
    }

    /// <summary>
    /// Asynchronously queries yahoo finance for price data and updates dictionaries within <paramref name="priceByDateByContractCode"/>.
    /// </summary>
    /// <param name="yahooPriceSymbolByContractCode">Dictionary of price symbols keyed to cftc contract codes.</param>
    /// <param name="priceByDateByContractCode">A contract code keyed dictionary that contains a dictionary keyed to wanted dates for the given contract code.</param>
    /// <returns>An asynchronous task.</returns>
    static async Task RetrieveAndUploadYahooPriceDataAsync(Dictionary<string, string> yahooPriceSymbolByContractCode, Dictionary<string, Dictionary<DateTime, decimal?>> priceByDateByContractCode)
    {
        using HttpClient priceRetrievalClient = new() { BaseAddress = new Uri("https://query1.finance.yahoo.com/v8/finance/chart/") };
        var headers = priceRetrievalClient.DefaultRequestHeaders;
        //headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/csv"));
        headers.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0");

        // const byte YahooDateColumn = 0, YahooCloseColumn = 5;
        bool dataFound = false;
        foreach (var knownSymbol in yahooPriceSymbolByContractCode)
        {
            if (priceByDateByContractCode.TryGetValue(knownSymbol.Key, out Dictionary<DateTime, decimal?>? priceByDate))
            {
                var startDate = (long)priceByDate.Keys.Min().Subtract(DateTime.UnixEpoch).TotalSeconds;
                var endDate = (long)priceByDate.Keys.Max().AddDays(1).Subtract(DateTime.UnixEpoch).TotalSeconds;

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

                if (response.Contains("timestamp"))
                {
                    try
                    {
                        uint?[]? unixDates = JsonSerializer.Deserialize<uint?[]?>(response.Split("\"timestamp\":")[1].Split(']')[0] + ']')!;
                        decimal?[]? adjustedClose = JsonSerializer.Deserialize<decimal?[]?>(response.Split("\"adjclose\":")[2].Split(']')[0] + ']')!;

                        if (unixDates != null && adjustedClose != null)
                        {
                            for (var i = 0; i < unixDates.Length; ++i)
                            {
                                if (unixDates[i] != null)
                                {
                                    var queryDate = DateTime.UnixEpoch.AddSeconds((double)unixDates[i]!).Date;
                                    if (priceByDate.ContainsKey(queryDate) && adjustedClose[i] != null)
                                    {
                                        priceByDate[queryDate] = adjustedClose[i];
                                        dataFound = true;
                                    }
                                }
                            }
                        }
                    }
                    catch (IndexOutOfRangeException)
                    {
                        continue;
                    }
                }
            }
        }
        if (dataFound) await UploadPriceDataAsync(priceByDateByContractCode).ConfigureAwait(false);
    }

    /// <summary>
    /// Queries the database for the latest date found within <see cref="_tableNameWithinDatabase"/> .
    /// </summary>
    /// <param name="filterForIce"><see langword="true"/> if the latest date for ICE data should be returned; otherwise, <see langword="false"/>.</param>
    /// <returns>The most recent DateTime found in the <see cref="StandardDateFieldName"/> column within the database.</returns>
    /// <exception cref="InvalidOperationException">Thrown if filtering for ICE contracts but instance isn't for the Disaggregated report.</exception>
    async Task<DateTime> GetLatestTableDateAsync(bool filterForIce)
    {
        if (filterForIce && QueriedReport != ReportType.Disaggregated)
        {
            throw new InvalidOperationException($"{nameof(QueriedReport)} must be {nameof(ReportType.Disaggregated)} while {nameof(filterForIce)} is true.");
        }

        using SqlCommand cmd = s_databaseConnection!.CreateCommand();

        cmd.CommandText = $"SELECT MAX({StandardDateFieldName}) FROM {_tableNameWithinDatabase} Where {ContractCodeColumnName} {(filterForIce ? string.Empty : "NOT ")}In {IceCodes};";
        DateTime storedDate = s_defaultStartDate;

        if (s_databaseConnection!.State == ConnectionState.Closed)
        {
            try
            {
                s_databaseConnection.Open();
            }
            catch (InvalidOperationException e)
            {
                Console.WriteLine(e);
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
        using SqlCommand cmd = s_databaseConnection!.CreateCommand();
        cmd.CommandText = $"SELECT TOP 1 * FROM {_tableNameWithinDatabase};";
        if (s_databaseConnection.State == ConnectionState.Closed) await s_databaseConnection.OpenAsync().ConfigureAwait(false);

        using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

        List<string> fieldNames = [.. from columnSchema in await reader.GetColumnSchemaAsync().ConfigureAwait(false)
                                   let fieldName = columnSchema.ColumnName
                                   select fieldName];

        await reader.CloseAsync().ConfigureAwait(false);
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
            s_cftcApiClient.Dispose();
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
        Dictionary<string, FieldInfo> fieldInfoByEditedName = [];
        // Dictionary keyed to api header names with their 0 based column number.
        Dictionary<string, int> headerIndexesByEditedName = [];

        // Editing externalHeaders for alignment.
        for (var i = 0; i < externalHeaders.Length; ++i)
        {
            var header = externalHeaders[i].ToLower();
            if (!iceHeaders)
            {
                if (header.Contains("spead")) header = header.Replace("spead", "spread");
                if (header.Contains("postions")) header = header.Replace("postions", "positions");
                if (header.Contains("open_interest")) header = header.Replace("open_interest", "oi");
                if (header.Contains("__")) header = header.Replace("__", "_");
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
            {"%", "pct"},{"=", "_"},
            {"(", "_"},{")", string.Empty},
            {"-", "_"}, {"commercial", "comm"},
            {"reportable", "rept"}, {"total", "tot"},
            {"concentration", "conc"},{"spreading", "spread"},
            {"_lt_", "_le_"},{"___","_"},
            {"__", "_"},{ "open_interest_oi", "oi"},
            { "open_interest", "oi"},{"_in_initials",string.Empty}
        };
        // <edited name , original name in database >
        Dictionary<string, string> originalFieldByEditedName = [];

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
                        if (editedTableFieldName.Contains(pair.Key)) editedTableFieldName = editedTableFieldName.Replace(pair.Key, pair.Value, StringComparison.InvariantCultureIgnoreCase);
                    }
                }
            }
            //  There is an inconsistency between cftc_region_code in the api and in the original database columns.
            if (editedTableFieldName.Equals("cftc_region_code") == false)
            {
                originalFieldByEditedName.Add(editedTableFieldName, databaseFields[i]);
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

    /// <summary>
    /// Creates a dictionary of relevant values used to summarize the current state of the instance.
    /// </summary>
    /// <returns>A dictionary of values using strings as a key.</returns>
    public Dictionary<string, object> Summarized()
    {
        return new Dictionary<string, object>(){
            {"Latest Date",$"{DatabaseDateAfterUpdate:yyyy-MM-ddTHH:mm:ssZ}"},
            {"Time Elapsed (ms)", ActionTimer.ElapsedMilliseconds},
            {"Status", (int)CurrentStatus}
        };
    }
    private async Task CreateReportTableIfMissingAsync()
    {
        using var tableCMD = s_databaseConnection!.CreateCommand();
        tableCMD.CommandText = $"SELECT COUNT(name) FROM sys.Tables WHERE name =@name";
        tableCMD.Parameters.AddWithValue("@name", _tableNameWithinDatabase);

        bool? reportTableExists = (await tableCMD.ExecuteScalarAsync().ConfigureAwait(false))?.Equals(1);

        if (!(reportTableExists ?? false))
        {
            tableCMD.Parameters.Clear();
            tableCMD.CommandText = await CreateReportTableSQLAsync().ConfigureAwait(false);
            await tableCMD.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }
    /// <summary>
    /// Queries the CFTC API and filters for the displayed headers.
    /// </summary>
    /// <returns>An asynchronous task.</returns>
    private async Task<string[]> GetHeadersFromAPIAsync()
    {
        var apiMetaDataUri = new Uri($"https://publicreporting.cftc.gov/api/views/");
        using var client = new HttpClient() { BaseAddress = apiMetaDataUri };
        string jsonResponse = await client.GetStringAsync(_cftcApiCode).ConfigureAwait(false);

        var document = JsonNode.Parse(jsonResponse)!;
        JsonNode root = document.Root;
        JsonArray columnInfo = root["columns"]!.AsArray();
        var columnsToDrop = "id,futonly_or_combined,commodity_name".Split(',');
        // The name field is selected for because it hasn't been renamed improperly.
        return [.. (from column in columnInfo
                let columnName = column["fieldName"]!.GetValue<string>()
                where !columnsToDrop.Any(columnName.Equals)
                select column["name"]!.GetValue<string>().ToLower().Replace(' ', '_'))!];
    }
    /// <summary>
    /// Builds an SQL command for the creation of a SQL Table for current instance.
    /// </summary>
    /// <returns>An asynchronous Task.</returns>
    private async Task<string> CreateReportTableSQLAsync()
    {
        var builder = new StringBuilder();

        var numericIndicators = new string[] { "pct", "conc" };
        var intIndicators = "all,old,other".Split(',');
        var varChar10 = "cftc_region_code,cftc_market_code,cftc_contract_market_code".Split(',');
        int i = 0;

        foreach (string name in await GetHeadersFromAPIAsync().ConfigureAwait(false))
        {
            if (++i > 1) builder.Append("\n,");

            if (name.Contains("yyyy_mm_dd"))
            {
                builder.Append($"{name} DATE");
            }
            else if (numericIndicators.Any(name.Contains))
            {
                if (name.StartsWith("pct_of_open_interest"))
                {
                    // It is one of oi_all/old/other
                    string[] splitName = name.Split('_');
                    builder.Append($"pct_of_oi_{splitName[^1]} TINYINT");
                }
                else
                {
                    builder.Append($"{name} DECIMAL(5,2)");
                }
            }
            else if (name.Contains("trader") || name == "cftc_commodity_code")
            {
                builder.Append($"{name} SMALLINT");
            }
            else if (intIndicators.Any(name.Contains))
            {
                builder.Append($"{name} INT");
            }
            else
            {
                int size = name switch
                {
                    "market_and_exchange_names" => 100,
                    "yyyy_report_week_ww" => 19,
                    "contract_units" => 120,
                    string a when varChar10.Any(a.Equals) => 10,
                    _ => 70
                };
                builder.Append($"{name} VARCHAR({size})");
            }
        }
        builder.Append(",PRIMARY KEY (report_date_as_yyyy_mm_dd,cftc_contract_market_code)");
        return $"CREATE TABLE {_tableNameWithinDatabase} ({builder});";
    }
    private readonly struct FieldInfo(int columnIndex, string columnName, string editedColumnName)
    {
        /// <summary>
        /// Column Index of the given field within an array of data.
        /// </summary>
        public int ColumnIndex { get; } = columnIndex;

        /// <summary>
        /// Name of the field within the database.
        /// </summary>
        public string ColumnName { get; } = columnName;

        /// <summary>
        /// Edited version of <see cref="ColumnName"/> 
        /// </summary>
        public string EditedColumnName { get; } = editedColumnName;

        /// <summary>
        /// Adjusts <see cref="ColumnName"/> so that it is database compatible.
        /// </summary>
        /// <value>Returns <see cref="ColumnName"/> enclosed in square brackets.</value>
        public string DatabaseAdjustedName => $"[{ColumnName}]";

        /// <summary>
        /// String used to target an auto generated parameter when uploading to the database.
        /// </summary>
        public string ParamName { get; } = $"@{columnName}";
    }

    [GeneratedRegex("[,]{1}(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))")]
    private static partial Regex SplitOnCommaNotWithinQuotesRegex();
}