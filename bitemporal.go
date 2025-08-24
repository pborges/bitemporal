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
	ValidFrom       time.Time `json:"valid_from"`
	ValidTo         time.Time `json:"valid_to"`
	TransactionFrom time.Time `json:"transaction_from"`
	TransactionTo   time.Time `json:"transaction_to"`
}

func (e Entity) String() string {
	return fmt.Sprintf("[VALID: %s -> %s TXN: %s -> %s]",
		e.ValidFrom.Format(time.DateTime), e.ValidTo.Format(time.DateTime),
		e.TransactionFrom.Format(time.DateTime), e.TransactionTo.Format(time.DateTime))
}
