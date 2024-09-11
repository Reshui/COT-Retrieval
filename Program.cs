using ReportRetriever;
using System.Text;
using System.Text.Json;
using System.Diagnostics;

var totalElapsedTimeWatch = Stopwatch.StartNew();
bool debugMode = false;

if (args.Length == 2 || args.Length == 0)
{
    Console.WriteLine($"Program started. {DateTime.UtcNow}\n");
    //string databaseString;
    string symbolInfoJson;
    bool downloadPriceDate = false;
    if (args.Length == 2)
    {
        if (debugMode) throw new InvalidOperationException($"{nameof(debugMode)} must be false to continue with command arguments");
        // Database path strings from Excel should already be formatted to use double \ 
        //databaseString = args[0];
        symbolInfoJson = args[0];
        downloadPriceDate = int.Parse(args[1]) == -1;
    }
    else
    {
        // When creating a string that will be parsed by the JsonSerializer 4 \ are needed for each \.  
        //databaseString = $"{{\"Legacy\":{GenerateDefaultDatabasePath(ReportType.Legacy)},\"Disaggregated\":{GenerateDefaultDatabasePath(ReportType.Disaggregated)},\"TFF\":{GenerateDefaultDatabasePath(ReportType.TFF)}}}".Replace("\\", "\\\\");

        var symbolList = new StringBuilder();

        symbolList.Append("{\"191691\":\"ALI=F\",\"191693\":\"AUP=F\",\"232741\":\"6A=F\",\"221602\":\"AW=F\",\"133741\":\"BTC=F\",");
        symbolList.Append("\"102741\":\"6L=F\",\"06765T\":\"BZ=F\",\"096742\":\"6B=F\",\"050642\":\"CB=F\",\"090741\":\"6C=F\",\"063642\":\"CSC=F\",");
        symbolList.Append("\"052644\":\"GDK=F\",\"073732\":\"CC=F\",\"083731\":\"KC=F\",\"06665T\":\"A8KZ23.NYM\",\"085692\":\"HG=F\",");
        symbolList.Append("\"002602\":\"ZC=F\",\"033661\":\"CT=F\",\"12460+\":\"YM=F\",\"124603\":\"YM=F\",\"124606\":\"RX=F\",\"052645\":\"DY=F\",");
        symbolList.Append("\"239744\":\"RSV=F\",\"33874A\":\"EMD=F\",\"13874A\":\"ES=F\",\"138748\":\"XAP=F\",\"138749\":\"XAE=F\",\"13874C\":\"XAF=F\",");
        symbolList.Append("\"13874E\":\"XAV=F\",\"13874J\":\"XAU=F\",\"099741\":\"6E=F\",\"299741\":\"KGB=F\",\"045601\":\"ZQ=F\",\"061641\":\"GF=F\",");
        symbolList.Append("\"040701\":\"OJ=F\",\"111659\":\"RB=F\",\"088691\":\"GC=F\",\"097741\":\"6J=F\",\"054642\":\"HE=F\",\"057642\":\"LE=F\",");
        symbolList.Append("\"095741\":\"6M=F\",\"209747\":\"MNQ=F\",\"13874U\":\"MES=F\",\"052641\":\"DC=F\",\"209742\":\"NQ=F\",\"20974+\":\"NQ=F\",");
        symbolList.Append("\"023651\":\"NG=F\",\"240741\":\"NKD=F\",\"240743\":\"NIY=F\",\"052642\":\"GN=F\",\"022651\":\"HO=F\",\"112741\":\"6N=F\",");
        symbolList.Append("\"004603\":\"ZO=F\",\"075651\":\"PA=F\",\"076651\":\"PL=F\",\"039601\":\"ZR=F\",\"239742\":\"RTY=F\",\"43874A\":\"SDA=F\",");
        symbolList.Append("\"13874+\":\"ES=F\",\"084691\":\"SI=F\",\"122741\":\"6Z=F\",\"026603\":\"ZM=F\",\"007601\":\"ZL=F\",\"005602\":\"ZS=F\",");
        symbolList.Append("\"192651\":\"HRC=F\",\"080732\":\"SB=F\",\"092741\":\"6S=F\",\"043607\":\"TN=F\",\"020604\":\"UB=F\",\"098662\":\"DX=F\",");
        symbolList.Append("\"043602\":\"ZN=F\",\"042601\":\"ZT=F\",\"044601\":\"ZF=F\",\"020601\":\"ZB=F\",\"1170E1\":\"^VIX\",\"001612\":\"KE=F\",");
        symbolList.Append("\"001602\":\"ZW=F\",\"067651\":\"CL=F\"}");

        symbolInfoJson = symbolList.ToString();
        symbolList.Clear();
    }

    //var filePathByReportType = JsonSerializer.Deserialize<Dictionary<string, string>>(databaseString)!;
    var priceSymbolByContractCode = JsonSerializer.Deserialize<Dictionary<string, string>>(symbolInfoJson)!;
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

    var updatingTasksByReport = new Dictionary<Report, Task?>();

    foreach (bool retrieveCombinedData in new bool[] { true, false })
    {
        foreach (ReportType reportType in Enum.GetValues(typeof(ReportType)))
        {
            if (debugMode && false == (reportType == ReportType.Legacy && retrieveCombinedData == true)) continue;
            var reportInstance = new Report(reportType, retrieveCombinedData, debugMode);
            updatingTasksByReport.Add(reportInstance, reportInstance.CommitmentsOfTradersRetrievalAndUploadAsync(reportInstance.IsLegacyCombined ? priceSymbolByContractCode : null, testUpload, downloadPriceDate));
            if (debugMode) break;
        }
        //if (debugMode) break;
    }

    try
    {
        await Task.WhenAll(updatingTasksByReport?.Values!);
    }
    catch (Exception)
    {
        foreach (var task in updatingTasksByReport.Values.Where(x => x!.IsFaulted))
        {
            Console.WriteLine(task!.Exception);
        }
    }

    totalElapsedTimeWatch.Stop();
    StringBuilder outputText = new();

    var elapsedTimeMessage = "Total Elapsed:\t" + (totalElapsedTimeWatch.ElapsedMilliseconds / 1000f) + 's';
    outputText.AppendLine(elapsedTimeMessage);

    var summary = new Dictionary<bool, Dictionary<char, Dictionary<string, object>>>();
    foreach (var instance in updatingTasksByReport!.Keys)
    {
        Report.DisposeConnection();

        bool reportKey = instance.RetrieveCombinedData;
        if (!summary.TryGetValue(reportKey, out var innerDict))
        {
            innerDict = summary[reportKey] = new();
        }

        innerDict.Add(instance.QueriedReport.ToString()[0], instance.Summarized());

        string baseText = $"{instance.QueriedReport,-13}:{{Combined: {instance.RetrieveCombinedData,-5}, Time Elapsed: {instance.ActionTimer.ElapsedMilliseconds,-4}ms, Latest Date: {instance.DatabaseDateAfterUpdate:yyyy-MM-dd}, Status: {(int)instance.CurrentStatus}}}";
        outputText.AppendLine(baseText);
    }

    await Console.Out.WriteAsync($"<json>\n{JsonSerializer.Serialize(summary)}\n</json>\n{outputText}");
}