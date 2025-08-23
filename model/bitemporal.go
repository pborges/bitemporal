package model

import (
	"fmt"
	"time"
)

var EndOfTime time.Time

func init() {
	EndOfTime, _ = time.Parse(time.DateTime, "9999-12-31 23:59:59")
}

type BitemporalEntity struct {
	ValidFrom       time.Time `json:"valid_from"`
	ValidTo         time.Time `json:"valid_to"`
	TransactionFrom time.Time `json:"transaction_from"`
	TransactionEnd  time.Time `json:"transaction_to"`
}

func (e BitemporalEntity) String() string {
	return fmt.Sprintf("[VALID: %s -> %s TXN: %s -> %s]",
		e.ValidFrom.Format(time.DateTime), e.ValidTo.Format(time.DateTime),
		e.TransactionFrom.Format(time.DateTime), e.TransactionEnd.Format(time.DateTime))
}
