package flow_storage_tracker

// Config defines that application's config
type Config struct {
	// BachSize is the number of Addresses for which to run each script
	BatchSize         int    `default:"2000"`
	FlowAccessNodeURL string `default:"access-008.mainnet24.nodes.onflow.org:9000"`
}

type DBConfig struct {
	Port     string `default:"5432"`
	Host     string `default:"localhost"`
	Name     string `default:"storage-tracker"`
	User     string `default:"postgres"`
	Password string `default:"postgres"`
}
