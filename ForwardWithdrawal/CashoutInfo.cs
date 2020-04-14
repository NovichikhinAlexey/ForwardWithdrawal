using System;

namespace ForwardWithdrawal
{
    public class CashoutInfo
    {
        public string Id { get; set; }
        public DateTime Date { get; set; }
        public string ClientId { get; set; }
        public string AssetId { get; set; }
        public double Amount { get; set; }
    }
}
