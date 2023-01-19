using Services;
using CsvHelper;
using System.Globalization;

try
{
    var ps = new PowerService();

    IEnumerable<PowerTrade> pts = ps.GetTrades(DateTime.Now);

    using var writer = new StreamWriter("Load.csv");
    using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
    {
        Console.WriteLine("Generating Data File...");

        foreach (var pt in pts)
        {
            csv.WriteRecords(pt.Periods);
        }
        Console.WriteLine("Done");
    }
}
catch(Exception ex)
{
    Console.WriteLine(ex.Message);
}