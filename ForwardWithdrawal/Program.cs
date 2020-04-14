using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AzureStorage.Tables;
using AzureStorage.Tables.Templates.Index;
using Common.Log;
using Lykke.SettingsReader.ReloadingManager;
using Microsoft.Extensions.Configuration;

namespace ForwardWithdrawal
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();
            
            var settings = new AppSettings();
            config.Bind(settings);

            var repository = new CashOperationsRepository(
                AzureTableStorage<CashInOutOperationEntity>.Create(
                    ConstantReloadingManager.From(settings.OperationsCashConnString),
                    "OperationsCash", EmptyLog.Instance),
                AzureTableStorage<AzureIndex>.Create(ConstantReloadingManager.From(settings.OperationsCashConnString),
                    "OperationsCash", EmptyLog.Instance));

            var list = new List<CashoutInfo>();

            Console.WriteLine("Getting data...");

            await repository.GetDataByChunksAsync(operations =>
            {
                var cashouts = operations
                    .Where(item => settings.AssetIds.Values.Contains(item.AssetId) && item.Type == CashOperationType.ForwardCashOut/* && item.DateTime.Year < 2020*/)
                    .Select(x => new CashoutInfo
                        {
                            Date = x.DateTime,
                            ClientId = x.ClientId,
                            AssetId = x.AssetId,
                            Amount = x.Amount,
                            Id = x.Id
                        });
                
                list.AddRange(cashouts);
                Console.WriteLine($"Found total cashouts: {list.Count}");
                return Task.CompletedTask;
            });

            var dist = list.GroupBy(x => x.Id).Select(x => x.First());
            var filename = $"forward-cashout-{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.csv";

            Console.WriteLine($"Found {dist.Count()} forward cashouts. Saving results to {filename}...");

            using (var sw = new StreamWriter(filename))
            {
                var now = DateTime.UtcNow;
                var sb = new StringBuilder();
                sb.AppendLine("Date,ClientId,AssetId,Amount,Settled");
                
                foreach (var cashout in dist)
                {
                    sb.AppendLine($"{cashout.Id},{cashout.Date},{cashout.ClientId},{cashout.AssetId},{cashout.Amount.ToString(CultureInfo.InvariantCulture)},{IsSettled(cashout, now, settings.AssetIds)}");
                }
                
                sw.Write(sb.ToString());
            }
            
            Console.WriteLine("Done!");
        }

        private static bool IsSettled(CashoutInfo cashout, in DateTime now, IReadOnlyDictionary<string, string> settingsAssetIds)
        {
            var settledDate = cashout.AssetId == settingsAssetIds["LKK1Y"]
                ? cashout.Date.AddYears(1)
                : cashout.Date.AddYears(2);

            return settledDate < now;
        }
    }
}
