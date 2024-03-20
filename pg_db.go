package flow_storage_tracker

import (
	"database/sql"
	"sync"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/hashicorp/go-multierror"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go/fvm/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type DB[DBTX any] interface {
	TX(func(tx DBTX) error) error
}

type PGDB struct {
	mu  sync.Mutex
	db  *sql.DB
	log zerolog.Logger
}

var _ DB[DBTX] = (*PGDB)(nil)

func NewPGDB(connectionString string, log zerolog.Logger) (*PGDB, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Warn().Err(err).Msg("failed to open db")
		return nil, err
	}

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		log.Warn().Err(err).Msg("failed to create db driver")
		return nil, err
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://migrations",
		"postgres",
		driver)
	if err != nil {
		log.Warn().Err(err).Msg("failed to create db migrator")
		return nil, err
	}
	err = m.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		log.Warn().Err(err).Msg("failed to migrate db")
		return nil, err
	}

	return &PGDB{
		db:  db,
		log: log.With().Str("component", "pgdb").Logger(),
	}, nil
}

func (db *PGDB) TX(f func(tx DBTX) error) error {
	// this should be necessary!?
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	err = f(&PGDBTX{tx: tx, log: db.log})
	if err != nil {
		log.Error().Err(err).Msg("failed to execute tx")
		return multierror.Append(err, tx.Rollback()).ErrorOrNil()
	}
	return tx.Commit()
}

func (db *PGDB) Close() error {
	return db.db.Close()
}

type PGDBTX struct {
	tx  *sql.Tx
	log zerolog.Logger
}

func (p *PGDBTX) Get(allAddresses []flow.Address) (map[flow.Address]StoredAccountData, error) {
	stmt, err := p.tx.Prepare(`
		SELECT address, storage_used, block_height
		FROM storage
		WHERE address = ANY($1)
		`)
	if err != nil {
		return nil, err
	}

	results := make(map[flow.Address]StoredAccountData)

	batchSize := 100
	for i := 0; i < len(allAddresses); i += batchSize {
		last := i + batchSize
		if last > len(allAddresses) {
			last = len(allAddresses)
		}
		addresses := allAddresses[i:last]

		hexAddresses := make([]string, len(addresses))
		for i, addr := range addresses {
			hexAddresses[i] = addr.String()
		}
		rows, err := stmt.Query(pq.Array(hexAddresses))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			data := &addressData{}
			err = rows.Scan(&data.Address, &data.StorageUsed, &data.BlockHeight)
			if err != nil {
				return nil, err
			}

			results[flow.HexToAddress(data.Address)] = StoredAccountData{
				StorageUsed: data.StorageUsed,
			}
		}
		err = rows.Close()
		if err != nil {
			p.log.Error().Err(err).Msg("failed to close rows")
		}
	}
	return results, nil
}

func (p *PGDBTX) Set(blockHeight uint64, address map[flow.Address]StoredAccountData) error {
	updstmt, err := p.tx.Prepare(`
		INSERT INTO storage(address, storage_used, block_height)
		VALUES ($1, $2, $3)
		ON CONFLICT (address)
		DO
		UPDATE SET storage_used = EXCLUDED.storage_used,
				   block_height = EXCLUDED.block_height
		    `)
	if err != nil {
		p.log.Error().Err(err).Msg("failed to prepare insert statement")
		return err
	}

	for address, data := range address {
		_, err := updstmt.Exec(address.Hex(), data.StorageUsed, blockHeight)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *PGDBTX) GetTotalAccounts() (uint64, error) {
	stmt, err := p.tx.Prepare(`
		select count(1)
		from storage
		`)
	if err != nil {
		return 0, err
	}

	rows, err := stmt.Query()
	if err != nil {
		return 0, err
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			p.log.Error().Err(err).Msg("failed to close rows")
		}
	}(rows)

	for rows.Next() {
		var value *uint64
		err = rows.Scan(&value)
		if err != nil {
			return 0, err
		}
		if value != nil {
			return *value, nil
		}
		return 0, nil
	}
	return 0, nil
}

func (p *PGDBTX) GatTotalStorageUsed() (uint64, error) {
	stmt, err := p.tx.Prepare(`
		select sum(storage_used)
		from storage
		`)
	if err != nil {
		return 0, err
	}

	rows, err := stmt.Query()
	if err != nil {
		return 0, err
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			p.log.Error().Err(err).Msg("failed to close rows")
		}
	}(rows)

	for rows.Next() {
		var value *uint64
		err = rows.Scan(&value)
		if err != nil {
			return 0, err
		}
		if value != nil {
			return *value, nil
		}
		return 0, nil
	}
	return 0, nil
}

var _ DBTX = (*PGDBTX)(nil)

type addressData struct {
	Address     string
	StorageUsed uint64
	BlockHeight uint64
}
