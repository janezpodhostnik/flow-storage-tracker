package flow_storage_tracker

import (
	"context"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"

	scan "github.com/onflow/flow-batch-scan"
)

type DBTX interface {
	Get(address []flow.Address) (map[flow.Address]StoredAccountData, error)
	Set(blockHeight uint64, address map[flow.Address]StoredAccountData) error
	GetTotalAccounts() (uint64, error)
	GatTotalStorageUsed() (uint64, error)
}

type StoredAccountData struct {
	StorageUsed uint64
}

type ScriptResultHandler struct {
	*scan.ComponentBase

	db          DB[DBTX]
	limiterChan chan struct{}

	reporter       Reporter
	notFirstReport bool
}

func NewScriptResultHandler(
	db DB[DBTX],
	reporter Reporter,
	logger zerolog.Logger,
) *ScriptResultHandler {
	h := &ScriptResultHandler{
		db:          db,
		limiterChan: make(chan struct{}, 5), // concurrency limiter

		reporter: reporter,
	}
	h.ComponentBase = scan.NewComponentWithStart("script_result_handler", h.start, logger)

	return h
}

func (r *ScriptResultHandler) start(ctx context.Context) {
	go func() {
		<-ctx.Done()
		r.Finish(ctx.Err())
	}()
}

func (r *ScriptResultHandler) Handle(out scan.ProcessedAddressBatch) error {
	r.limiterChan <- struct{}{}
	defer func() { <-r.limiterChan }()

	if !r.notFirstReport {
		r.notFirstReport = true

		err := r.firstReport()
		if err != nil {
			return err
		}
	}

	blockHeight, batch := ParseProcessedAddressBatch(out)

	updateResults, err := r.UpdateState(blockHeight, batch)
	if err != nil {
		return err
	}

	err = r.HandleUpdateResults(updateResults)

	return err
}

type dataChanges struct {
	newAddresses       uint64
	storageUsedChanged int64
}

func (r *ScriptResultHandler) UpdateState(blockHeight uint64, batch ParsedProcessedAddressBatch) (dataChanges, error) {
	changes := dataChanges{}

	err := r.db.TX(
		func(txn DBTX) error {
			allAddresses := make([]flow.Address, 0, len(batch))
			for address := range batch {
				allAddresses = append(allAddresses, address)
			}
			allAccounts, err := txn.Get(allAddresses)
			if err != nil {
				return err
			}

			// new addresses
			changes.newAddresses = uint64(len(allAddresses) - len(allAccounts))

			newStorageUsed := uint64(0)
			for _, newAccountData := range batch {
				newStorageUsed += newAccountData.StorageUsed
			}
			oldStorageUsed := uint64(0)
			for _, oldAccountData := range allAccounts {
				oldStorageUsed += oldAccountData.StorageUsed
			}
			if newStorageUsed < oldStorageUsed {
				changes.storageUsedChanged = -int64(oldStorageUsed - newStorageUsed)
			} else {
				changes.storageUsedChanged = int64(newStorageUsed - oldStorageUsed)
			}

			return txn.Set(blockHeight, batch)
		})

	return changes, err
}

func (r *ScriptResultHandler) HandleUpdateResults(results dataChanges) error {
	r.reporter.ReportTotalAccountsIncreased(results.newAddresses)

	r.reporter.ReportStorageUsedChanged(results.storageUsedChanged)

	return nil
}

func (r *ScriptResultHandler) firstReport() error {
	totalAccounts := uint64(0)
	totalStorageUsed := uint64(0)
	err := r.db.TX(func(txn DBTX) error {
		var err error

		totalAccounts, err = txn.GetTotalAccounts()
		if err != nil {
			return err
		}
		totalStorageUsed, err = txn.GatTotalStorageUsed()
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		r.Logger.Error().Err(err).Msg("failed to get total accounts and storage used from db")
		return err
	}

	r.reporter.ReportTotalAccountsIncreased(totalAccounts)
	r.reporter.ReportStorageUsedChanged(int64(totalStorageUsed))

	return nil
}

type ParsedProcessedAddressBatch = map[flow.Address]StoredAccountData

func ParseProcessedAddressBatch(batch scan.ProcessedAddressBatch) (uint64, ParsedProcessedAddressBatch) {
	parsedBatch := make(ParsedProcessedAddressBatch, len(batch.Addresses))

	for _, account := range batch.Result.(cadence.Array).Values {
		address, accountData := ParseProcessedSingleAddressBatchResult(account)
		parsedBatch[address] = accountData
	}

	return batch.BlockHeight, parsedBatch
}

func ParseProcessedSingleAddressBatchResult(value cadence.Value) (flow.Address, StoredAccountData) {
	accountStruct := value.(cadence.Struct)
	address := flow.BytesToAddress(accountStruct.Fields[0].(cadence.Address).Bytes())
	data := StoredAccountData{
		StorageUsed: accountStruct.Fields[1].(cadence.UInt64).ToGoValue().(uint64),
	}

	return address, data
}
