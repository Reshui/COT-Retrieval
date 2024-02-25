using ReportRetriever;
using System.Text;
using System.Text.Json;

var totalElapsedTimeWatch = System.Diagnostics.Stopwatch.StartNew();
bool debugMode = false;

if (OperatingSystem.IsWindows() && (args.Length == 2 || args.Length == 0))
{
    string databaseString;
    string priceString;

    if (args.Length == 2)
    {
        if (debugMode) throw new InvalidOperationException($"{nameof(debugMode)} must be false to continue with command arguments");
        // Database path strings from Excel should already be formatted to use double \ 
        databaseString = args[0];
        priceString = args[1];
    }
    else
    {
        // When creating a string that will be parsed by the JsonSerializer 4 \ are needed for each \.  
        databaseString = $"{{\"Legacy\":{GenerateDefaultDatabasePath(ReportType.Legacy)},\"Disaggregated\":{GenerateDefaultDatabasePath(ReportType.Disaggregated)},\"TFF\":{GenerateDefaultDatabasePath(ReportType.TFF)}}}".Replace("\\", "\\\\");

        priceString = "{\"191693\":\"AUP=F\",\"232741\":\"6A=F\",\"221602\":\"AW=F\",\"133741\":\"BTC=F\",\"00160F\":\"BWF=F\",\"102741\":\"6L=F\",\"06765T\":\"BZ=F\",\"096742\":\"6B=F\",\"050642\":\"CB=F\",\"090741\":\"6C=F\"" +
            ",\"063642\":\"CSC=F\",\"052644\":\"GDK=F\",\"024656\":\"MTF=F\",\"073732\":\"CC=F\",\"083731\":\"KC=F\",\"06665T\":\"A8KZ23.NYM\",\"085692\":\"HG=F\",\"002602\":\"ZC=F\",\"033661\":\"CT=F\",\"12460+\":\"YM=F\"" +
            ",\"124603\":\"YM=F\",\"124606\":\"RX=F\",\"052645\":\"DY=F\",\"239744\":\"RSV=F\",\"33874A\":\"EMD=F\",\"13874A\":\"ES=F\",\"138748\":\"XAP=F\",\"138749\":\"XAE=F\",\"13874C\":\"XAF=F\",\"13874E\":\"XAV=F\"" +
            ",\"13874F\":\"XAI=F\",\"13874H\":\"XAB=F\",\"13874J\":\"XAU=F\",\"099741\":\"6E=F\",\"299741\":\"KGB=F\"" +
            ",\"399741\":\"EURJPY=X\",\"132741\":\"GE=F\",\"045601\":\"ZQ=F\",\"061641\":\"GF=F\",\"040701\":\"OJ=F\",\"111659\":\"RB=F\",\"088691\":\"GC=F\",\"097741\":\"6J=F\",\"054642\":\"HE=F\",\"057642\":\"LE=F\"" +
            ",\"095741\":\"6M=F\",\"124608\":\"MYM=F\",\"209747\":\"MNQ=F\",\"239747\":\"M2K=F\",\"13874U\":\"MES=F\",\"052641\":\"DC=F\",\"209742\":\"NQ=F\",\"20974+\":\"NQ=F\",\"023651\":\"NG=F\",\"240741\":\"NKD=F\"" +
            ",\"240743\":\"NIY=F\",\"052642\":\"GN=F\",\"022651\":\"HO=F\",\"112741\":\"6N=F\",\"004603\":\"ZO=F\",\"075651\":\"PA=F\",\"076651\":\"PL=F\",\"058643\":\"LBS=F\",\"039601\":\"ZR=F\",\"239742\":\"RTY=F\"" +
            ",\"089741\":\"6R=F\",\"43874A\":\"SDA=F\",\"13874+\":\"ES=F\",\"138741\":\"ES=F\",\"084691\":\"SI=F\",\"122741\":\"6Z=F\",\"026603\":\"ZM=F\",\"007601\":\"ZL=F\",\"005602\":\"ZS=F\",\"192651\":\"HRC=F\"" +
            ",\"080732\":\"SB=F\",\"092741\":\"6S=F\",\"043607\":\"TN=F\",\"020604\":\"UB=F\",\"098662\":\"DX=F\",\"043602\":\"ZN=F\",\"042601\":\"ZT=F\",\"044601\":\"ZF=F\",\"020601\":\"ZB=F\",\"1170E1\":\"^VIX\"" +
            ",\"001612\":\"KE=F\",\"001602\":\"ZW=F\",\"067651\":\"CL=F\",\"191691\":\"ALI=F\"}";
    }

    var filePathByReportType = JsonSerializer.Deserialize<Dictionary<string, string>>(databaseString)!;
    var priceSymbolByContractCode = JsonSerializer.Deserialize<Dictionary<string, string>>(priceString)!;
    /*
        if (args.Length == 2)
        {
            foreach (var kvp in filePathByReportType)
            {
                Console.WriteLine(kvp.ToString());
            }

            foreach (var kvp in priceSymbolByContractCode)
            {
                Console.WriteLine(kvp.ToString());
            }
            Console.ReadKey();
        }
    */
    Dictionary<Report, Task> updatingTasks = new();
    bool testUpload = false;

    if (debugMode)
    {
        var validInput = new ConsoleKey[] { ConsoleKey.Y, ConsoleKey.N };
        bool exitLoop = false;
        do
        {
            Console.WriteLine("\n\nDo you want to test uploading to the database(y/n)?\t\t");
            var userInput = Console.ReadKey(true);
            exitLoop = validInput.Contains(userInput.Key);
            if (exitLoop && userInput.Key == ConsoleKey.Y) testUpload = true;
        } while (!exitLoop);
    }

    foreach (ReportType reportType in Enum.GetValues(typeof(ReportType)))
    {
        if (debugMode && reportType != ReportType.Legacy) continue;

        foreach (bool retrieveCombinedData in new bool[] { true, false })
        {
            if (filePathByReportType.TryGetValue(reportType.ToString(), out string? filePath) && File.Exists(filePath))
            {
                var tableToTarget = $"{reportType}_{(retrieveCombinedData == true ? "Combined" : "Futures_Only")}";
                var reportInstance = new Report(reportType, retrieveCombinedData, filePath, tableToTarget, debugMode);
                updatingTasks.Add(reportInstance, reportInstance.CommitmentsOfTradersRetrievalAndUploadAsync(reportInstance.IsLegacyCombined ? priceSymbolByContractCode : null, testUpload));
            }
            if (debugMode) break;
        }
        //if (debugMode) break;
    }

    try
    {
        await Task.WhenAll(updatingTasks.Values);
    }
    catch (Exception)
    {
        foreach (var task in updatingTasks.Values.Where(x => x.IsFaulted))
        {
            Console.WriteLine(task.Exception);
        }
    }
#pragma warning disable CA1416 // Validate platform compatibility

    try
    {
        Report legacyCombinedInstance = updatingTasks.Keys.First(x => x.IsLegacyCombined == true);

        var priceUpdatingTasks = (from instance in updatingTasks.Keys
                                  where instance.AwaitingPriceUpdate
                                  select instance.UpdatePricesWithLegacyDatabase(legacyCombinedInstance!)).ToList();
    }
    catch (InvalidOperationException)
    { // Thrown if none of the keys have a IsLegacyCombined property equal to true.
    }

#pragma warning restore CA1416 // Validate platform compatibility

    totalElapsedTimeWatch.Stop();
    StringBuilder outputText = new();

    foreach (var (instance, retrievalTask) in updatingTasks)
    {
        instance.DisposeConnection();
        string baseText = $"{instance.QueriedReport}:{{Combined: {instance.RetrieveCombinedData}, Time Elapsed: {instance.ActionTimer.Elapsed.Milliseconds}ms, Latest Date: {instance.DatabaseDateAfterUpdate:yyyy-MM-dd}, Status: {(int)instance.CurrentStatus}}}";
        outputText.AppendLine(baseText);
    }
    var elapsedTimeMessage = "\n\nTotal Elapsed:\t" + (totalElapsedTimeWatch.ElapsedMilliseconds / 1000f) + "s\n";
    await Console.Out.WriteAsync(elapsedTimeMessage + outputText.ToString());
}

/// <summary>
/// Geneates a file path string based on <paramref name="wantedReport"/>.
/// </summary>
static string GenerateDefaultDatabasePath(ReportType wantedReport)
{
    return '\"' + Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), $"{wantedReport}.accdb") + '\"';
}


