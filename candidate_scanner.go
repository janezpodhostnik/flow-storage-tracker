package flow_storage_tracker

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"

	"github.com/onflow/flow-go/engine/common/rpc/convert"
	"github.com/onflow/flow-go/ledger"
	"github.com/onflow/flow/protobuf/go/flow/entities"

	"github.com/onflow/flow-go-sdk"
	flowModel "github.com/onflow/flow-go/model/flow"

	"github.com/onflow/flow-batch-scan/candidates"
	"github.com/onflow/flow-batch-scan/client"

	execution "github.com/onflow/flow/protobuf/go/flow/executiondata"
)

type CandidateScanner struct {
	chain         flowModel.Chain
	exeDataClient execution.ExecutionDataAPIClient
	logger        zerolog.Logger
}

var _ candidates.CandidateScanner = CandidateScanner{}

func NewCandidateScanner(
	chain flowModel.Chain,
	exeDataClient execution.ExecutionDataAPIClient,
	logger zerolog.Logger,
) CandidateScanner {
	return CandidateScanner{
		chain:         chain,
		exeDataClient: exeDataClient,
		logger:        logger.With().Str("component", "candidate_scanner").Logger(),
	}
}

func (s CandidateScanner) Scan(
	ctx context.Context,
	client client.Client,
	blocks candidates.BlockRange,
) candidates.CandidatesResult {
	candidatesChan := make(chan candidates.CandidatesResult, blocks.End-blocks.Start+1)
	defer close(candidatesChan)

	blockHeight := blocks.Start
	for blockHeight <= blocks.End {
		go func(blockHeight uint64) {
			candidatesChan <- s.scanBlock(
				ctx,
				client,
				blockHeight,
			)
		}(blockHeight)
		blockHeight++
	}

	return candidates.WaitForCandidateResults(candidatesChan, int(blocks.End-blocks.Start+1))
}

func (s CandidateScanner) scanBlock(
	ctx context.Context,
	client client.Client,
	blockHeight uint64,
) candidates.CandidatesResult {
	blockID, err := s.getBlockID(ctx, client, blockHeight)
	if err != nil {
		return candidates.NewCandidatesResultError(
			fmt.Errorf("could not get block ID: %w", err),
		)
	}

	executionData, err := s.getExecutionData(ctx, blockID)
	if err != nil {
		return candidates.NewCandidatesResultError(
			fmt.Errorf("could not get execution data: %w", err),
		)
	}

	updates, err := s.extractTrieUpdates(executionData)
	if err != nil {
		return candidates.NewCandidatesResultError(
			fmt.Errorf("could not convert execution data: %w", err),
		)
	}

	addresses := map[flow.Address]struct{}{}
	for _, update := range updates {
		for _, payload := range update.Payloads {
			key, err := payload.Key()
			if err != nil {
				return candidates.NewCandidatesResultError(
					fmt.Errorf("could not get payload key: %w", err),
				)
			}
			if len(key.KeyParts[0].Value) != flow.AddressLength {
				continue
			}

			address := flow.BytesToAddress(key.KeyParts[0].Value)
			addresses[address] = struct{}{}
		}
	}

	return candidates.NewCandidatesResult(addresses)
}

func (s CandidateScanner) getBlockID(
	ctx context.Context,
	c client.Client,
	height uint64,
) (flow.Identifier, error) {
	block, err := c.GetBlockByHeight(ctx, height)
	if err != nil {
		return flow.EmptyID, err
	}
	return block.ID, nil
}

func (s CandidateScanner) getExecutionData(
	ctx context.Context,
	blockID flow.Identifier,
) (*entities.BlockExecutionData, error) {
	resp, err := s.exeDataClient.GetExecutionDataByBlockID(
		ctx,
		&execution.GetExecutionDataByBlockIDRequest{
			BlockId: blockID.Bytes(),
		})
	if err != nil {
		s.logger.
			Error().
			Err(err).
			Str("block_id", blockID.String()).
			Msg("could not get execution data")
		return nil, err
	}

	return resp.BlockExecutionData, nil
}

func (s CandidateScanner) extractTrieUpdates(
	m *entities.BlockExecutionData,
) ([]*ledger.TrieUpdate, error) {
	if m == nil {
		return nil, convert.ErrEmptyMessage
	}

	var updates []*ledger.TrieUpdate
	for i, c := range m.GetChunkExecutionData() {
		chunk, err := convert.MessageToChunkExecutionData(c, s.chain)
		if err != nil {
			return nil, fmt.Errorf("could not convert chunk %d: %w", i, err)
		}

		if chunk.TrieUpdate != nil {
			updates = append(updates, chunk.TrieUpdate)
		}
	}

	return updates, nil
}
