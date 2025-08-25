package bitemporal

import (
	"fmt"
	"time"
)

var EndOfTime time.Time

func init() {
	EndOfTime, _ = time.Parse(time.DateTime, "9999-12-31 23:59:59")
}

type Entity struct {
	ValidOpen  time.Time `json:"valid_open"`
	ValidClose time.Time `json:"valid_close"`
	TxnOpen    time.Time `json:"txn_open"`
	TxnClose   time.Time `json:"txn_close"`
}

func (e Entity) String() string {
	return fmt.Sprintf("[VALID: %s -> %s TXN: %s -> %s]",
		e.ValidOpen.Format(time.DateTime), e.ValidClose.Format(time.DateTime),
		e.TxnOpen.Format(time.DateTime), e.TxnClose.Format(time.DateTime))
}
