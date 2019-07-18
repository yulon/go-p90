package p90

import "github.com/google/uuid"

type header struct {
	MagNum byte
	ConID  uuid.UUID
	PktID  uint64
	Type   byte
}
