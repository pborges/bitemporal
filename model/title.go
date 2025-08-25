package model

import (
	"context"
	"fmt"

	"github.com/pborges/bitemporal"
)

func init() {
	bitemporal.Schema = append(bitemporal.Schema, bitemporal.Table{
		Name: "titles",
		Columns: []string{
			"emp_no",
			"title",
		},
	})
}

type Title struct {
	EmpNo int64  `json:"emp_no"`
	Title string `json:"title"`
	bitemporal.Entity
}

func (t Title) String() string {
	return fmt.Sprintf("Title{EmpNo: %d, Title: %s}", t.EmpNo, t.Title)
}

func NewTitleRepository(repo *bitemporal.TemporalDB) *TitleRepository {
	return &TitleRepository{
		repo: repo,
	}
}

type TitleRepository struct {
	repo *bitemporal.TemporalDB
}

func (r TitleRepository) ForEmployee(ctx context.Context, empNo int64) ([]Title, error) {
	rows, err := r.repo.Query(ctx, "SELECT emp_no, title, valid_close, valid_open, txn_open, txn_close FROM titles$ WHERE emp_no=@emp_no ORDER BY txn_open, valid_close", map[string]any{"emp_no": empNo})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var titles []Title
	for rows.Next() {
		title := Title{}
		err := rows.Scan(&title.EmpNo, &title.Title, &title.ValidClose, &title.ValidOpen, &title.TxnOpen, &title.TxnClose)
		if err != nil {
			return nil, err
		}
		titles = append(titles, title)
	}
	return titles, nil
}

func (r TitleRepository) AllRecords(ctx context.Context, empNo int64) ([]Title, error) {
	rows, err := r.repo.Query(ctx, "SELECT emp_no, title, valid_open, valid_close, txn_open, txn_close FROM titles WHERE emp_no=@emp_no ORDER BY txn_open, valid_open", map[string]any{"emp_no": empNo})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var titles []Title
	for rows.Next() {
		var title Title
		err = rows.Scan(&title.EmpNo, &title.Title, &title.ValidOpen, &title.ValidClose, &title.TxnOpen, &title.TxnClose)
		if err != nil {
			return nil, err
		}
		titles = append(titles, title)
	}
	return titles, nil
}
