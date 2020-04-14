using System.Collections.Generic;

namespace ForwardWithdrawal
{
    public class AppSettings
    {
        public string OperationsCashConnString { get; set; }
        public IReadOnlyDictionary<string, string> AssetIds { get; set; }
    }
}
