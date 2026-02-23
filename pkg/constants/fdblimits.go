package constants

const (
	// https://apple.github.io/foundationdb/known-limitations.html
	MaxTransactionAffectedSizeBytes = 10_000_000
	MaxKeySizeBytes                 = 10_000
	MaxValueSizeBytes               = 100_000
	TargetIterateBatchBytes         = MaxTransactionAffectedSizeBytes / 10
)
