package quic

type clientPreferredAddressMigration struct {
	queuedProbes  []byte
	pendingProbes []byte
}
