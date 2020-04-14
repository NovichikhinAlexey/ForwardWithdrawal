using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage;
using AzureStorage.Tables.Templates.Index;
using Microsoft.WindowsAzure.Storage.Table;

namespace ForwardWithdrawal
{
    public enum CashOperationType
    {
        None,
        ForwardCashOut,
        ForwardCashIn
    }

    public interface ICashInOutOperation : IBaseCashBlockchainOperation
    {
        bool IsRefund { get; set; }
        CashOperationType Type { get; set; }
    }
    
    public enum TransactionStates
    {
        InProcessOnchain,
        SettledOnchain,
        InProcessOffchain,
        SettledOffchain,
        SettledNoChain
    }
    
    public interface IBaseCashOperation
    {
        /// <summary>
        /// Record Id
        /// </summary>
        string Id { get; }

        string AssetId { get; }

        string ClientId { get; }

        double Amount { get; set; }

        DateTime DateTime { get; }

        bool IsHidden { get; }
    }
    
    public interface IBaseCashBlockchainOperation : IBaseCashOperation
    {
        string BlockChainHash { get; set; }

        string Multisig { get; }

        /// <summary>
        /// Bitcoin queue record id (BitCointTransaction)
        /// </summary>
        string TransactionId { get; }

        string AddressFrom { get; set; }

        string AddressTo { get; set; }

        bool? IsSettled { get; set; }

        TransactionStates State { get; set; }

        double FeeSize { get; }

        FeeSizeType FeeType { get; }
    }
    
    public enum FeeSizeType
    {
        Unknown,
        Absolute,
        Percentage
    }
    
    public class CashInOutOperationEntity : TableEntity, ICashInOutOperation
    {
        public string Id => RowKey;
        public DateTime DateTime { get; set; }
        public bool IsHidden { get; set; }
        public string AssetId { get; set; }
        public string ClientId { get; set; }
        public double Amount { get; set; }
        public string BlockChainHash { get; set; }
        public string Multisig { get; set; }
        public string TransactionId { get; set; }
        public string AddressFrom { get; set; }
        public string AddressTo { get; set; }
        public bool? IsSettled { get; set; }

        public string StateField { get; set; }
        public TransactionStates State
        {
            get
            {
                TransactionStates type = TransactionStates.InProcessOnchain;
                if (!string.IsNullOrEmpty(StateField))
                {
                    Enum.TryParse(StateField, out type);
                }
                return type;
            }
            set { StateField = value.ToString(); }
        }

        public bool IsRefund { get; set; }

        public string TypeField { get; set; }
        public CashOperationType Type
        {
            get
            {
                CashOperationType type = CashOperationType.None;
                if (!string.IsNullOrEmpty(TypeField))
                {
                    Enum.TryParse(TypeField, out type);
                }
                return type;
            }
            set { TypeField = value.ToString(); }
        }

        public double FeeSize { get; set; }

        public string FeeTypeText { get; set; }
        public FeeSizeType FeeType
        {
            get
            {
                FeeSizeType type = FeeSizeType.Unknown;
                if (!string.IsNullOrEmpty(FeeTypeText))
                {
                    Enum.TryParse(FeeTypeText, out type);
                }
                return type;
            }
        }

        public static class ByClientId
        {
            public static string GeneratePartitionKey(string clientId)
            {
                return clientId;
            }

            internal static string GenerateRowKey(string id)
            {
                return id;
            }

            public static CashInOutOperationEntity Create(ICashInOutOperation src)
            {
                return new CashInOutOperationEntity
                {
                    PartitionKey = GeneratePartitionKey(src.ClientId),
                    RowKey = GenerateRowKey(src.Id),
                    DateTime = src.DateTime,
                    AssetId = src.AssetId,
                    Amount = src.Amount,
                    BlockChainHash = src.BlockChainHash,
                    IsHidden = src.IsHidden,
                    IsRefund = src.IsRefund,
                    AddressFrom = src.AddressFrom,
                    AddressTo = src.AddressTo,
                    Multisig = src.Multisig,
                    ClientId = src.ClientId,
                    IsSettled = src.IsSettled,
                    Type = src.Type,
                    TransactionId = src.TransactionId
                };
            }
        }

        public static class ByMultisig
        {
            public static string GeneratePartitionKey(string multisig)
            {
                return multisig;
            }

            internal static string GenerateRowKey(string id)
            {
                return id;
            }

            public static CashInOutOperationEntity Create(ICashInOutOperation src)
            {
                return new CashInOutOperationEntity
                {
                    PartitionKey = GeneratePartitionKey(src.Multisig),
                    RowKey = GenerateRowKey(src.Id),
                    DateTime = src.DateTime,
                    AssetId = src.AssetId,
                    Amount = src.Amount,
                    BlockChainHash = src.BlockChainHash,
                    IsHidden = src.IsHidden,
                    IsRefund = src.IsRefund,
                    AddressFrom = src.AddressFrom,
                    AddressTo = src.AddressTo,
                    Multisig = src.Multisig,
                    ClientId = src.ClientId,
                    IsSettled = src.IsSettled,
                    Type = src.Type,
                    State = src.State,
                    TransactionId = src.TransactionId
                };
            }
        }
    }
    
    public class CashOperationsRepository
    {
        private readonly INoSQLTableStorage<CashInOutOperationEntity> _tableStorage;
        private readonly INoSQLTableStorage<AzureIndex> _blockChainHashIndices;

        public CashOperationsRepository(INoSQLTableStorage<CashInOutOperationEntity> tableStorage, INoSQLTableStorage<AzureIndex> blockChainHashIndices)
        {
            _tableStorage = tableStorage;
            _blockChainHashIndices = blockChainHashIndices;
        }

        public async Task<string> RegisterAsync(ICashInOutOperation operation)
        {
            var newItem = CashInOutOperationEntity.ByClientId.Create(operation);
            var byMultisig = CashInOutOperationEntity.ByMultisig.Create(operation);
            await _tableStorage.InsertAsync(newItem);
            await _tableStorage.InsertAsync(byMultisig);

            if (!string.IsNullOrEmpty(operation.BlockChainHash))
            {
                var indexEntity = AzureIndex.Create(operation.BlockChainHash, newItem.Id, newItem);
                await _blockChainHashIndices.InsertAsync(indexEntity);
            }

            return newItem.Id;
        }

        public async Task<IEnumerable<ICashInOutOperation>> GetAsync(string clientId)
        {
            var partitionkey = CashInOutOperationEntity.ByClientId.GeneratePartitionKey(clientId);
            return await _tableStorage.GetDataAsync(partitionkey);
        }
        
        public async Task<IEnumerable<ICashInOutOperation>> GetByChunksAsync(string clientId)
        {
            var partitionkey = CashInOutOperationEntity.ByClientId.GeneratePartitionKey(clientId);
            var result = new List<ICashInOutOperation>();
            
            await _tableStorage.GetDataByChunksAsync(partitionkey, entities => 
                result.AddRange(entities)
            );

            return result;
        }

        public async Task<ICashInOutOperation> GetAsync(string clientId, string recordId)
        {
            var partitionkey = CashInOutOperationEntity.ByClientId.GeneratePartitionKey(clientId);
            var rowKey = CashInOutOperationEntity.ByClientId.GenerateRowKey(recordId);
            return await _tableStorage.GetDataAsync(partitionkey, rowKey);
        }

        public async Task UpdateBlockchainHashAsync(string clientId, string id, string hash)
        {
            var partitionkey = CashInOutOperationEntity.ByClientId.GeneratePartitionKey(clientId);
            var rowKey = CashInOutOperationEntity.ByClientId.GenerateRowKey(id);

            var record = await _tableStorage.GetDataAsync(partitionkey, rowKey);

            var multisigPartitionkey = CashInOutOperationEntity.ByMultisig.GeneratePartitionKey(record.Multisig);
            var multisigRowKey = CashInOutOperationEntity.ByMultisig.GenerateRowKey(id);

            var indexEntity = AzureIndex.Create(hash, rowKey, partitionkey, rowKey);
            await _blockChainHashIndices.InsertOrReplaceAsync(indexEntity);

            await _tableStorage.MergeAsync(partitionkey, rowKey, entity =>
            {
                entity.BlockChainHash = hash;
                entity.State = TransactionStates.SettledOnchain;
                return entity;
            });

            await _tableStorage.MergeAsync(multisigPartitionkey, multisigRowKey, entity =>
            {
                entity.BlockChainHash = hash;
                entity.State = TransactionStates.SettledOnchain;
                return entity;
            });
        }

        public async Task SetBtcTransaction(string clientId, string id, string bcnTransactionId)
        {
            var partitionkey = CashInOutOperationEntity.ByClientId.GeneratePartitionKey(clientId);
            var rowKey = CashInOutOperationEntity.ByClientId.GenerateRowKey(id);

            var record = await _tableStorage.GetDataAsync(partitionkey, rowKey);

            var multisigPartitionkey = CashInOutOperationEntity.ByMultisig.GeneratePartitionKey(record.Multisig);
            var multisigRowKey = CashInOutOperationEntity.ByMultisig.GenerateRowKey(id);

            await _tableStorage.MergeAsync(partitionkey, rowKey, entity =>
            {
                entity.TransactionId = bcnTransactionId;
                return entity;
            });

            await _tableStorage.MergeAsync(multisigPartitionkey, multisigRowKey, entity =>
            {
                entity.TransactionId = bcnTransactionId;
                return entity;
            });
        }

        public async Task SetIsSettledAsync(string clientId, string id, bool offchain)
        {
            var partitionkey = CashInOutOperationEntity.ByClientId.GeneratePartitionKey(clientId);
            var rowKey = CashInOutOperationEntity.ByClientId.GenerateRowKey(id);

            var record = await _tableStorage.GetDataAsync(partitionkey, rowKey);

            var multisigPartitionkey = CashInOutOperationEntity.ByMultisig.GeneratePartitionKey(record.Multisig);
            var multisigRowKey = CashInOutOperationEntity.ByMultisig.GenerateRowKey(id);

            await Task.WhenAll(
                _tableStorage.MergeAsync(partitionkey, rowKey, entity =>
                {
                    if (offchain)
                        entity.State = TransactionStates.SettledOffchain;
                    else
                        entity.IsSettled = true;
                    return entity;
                }),

                _tableStorage.MergeAsync(multisigPartitionkey, multisigRowKey, entity =>
                {
                    if (offchain)
                        entity.State = TransactionStates.SettledOffchain;
                    else
                        entity.IsSettled = true;
                    return entity;
                })
            );
        }

        public async Task<IEnumerable<ICashInOutOperation>> GetByHashAsync(string blockchainHash)
        {
            var indices = await _blockChainHashIndices.GetDataAsync(blockchainHash);
            var keyValueTuples = indices?.Select(x => new Tuple<string, string>(x.PrimaryPartitionKey, x.PrimaryRowKey));
            return await _tableStorage.GetDataAsync(keyValueTuples);
        }

        public async Task<IEnumerable<ICashInOutOperation>> GetByMultisigAsync(string multisig)
        {
            var partitionkey = CashInOutOperationEntity.ByMultisig.GeneratePartitionKey(multisig);
            return await _tableStorage.GetDataAsync(partitionkey);
        }

        public async Task<IEnumerable<ICashInOutOperation>> GetByMultisigsAsync(string[] multisigs)
        {
            var tasks =
                multisigs.Select(x => _tableStorage.GetDataAsync(CashInOutOperationEntity.ByMultisig.GeneratePartitionKey(x)));

            var cashOpsByMultisigArr = await Task.WhenAll(tasks);

            var result = new List<ICashInOutOperation>();

            foreach (var arr in cashOpsByMultisigArr)
            {
                result.AddRange(arr);
            }

            return result;
        }

        public Task GetDataByChunksAsync(Func<IEnumerable<ICashInOutOperation>, Task> chunk)
        {
            return _tableStorage.GetDataByChunksAsync(chunk);
        }
    }
}
