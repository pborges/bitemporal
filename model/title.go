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
	bitemporal.BitemporalEntity
}

func (t Title) String() string {
	return fmt.Sprintf("Title{EmpNo: %d, Title: %s}", t.EmpNo, t.Title)
}

func NewTitleRepository(repo *bitemporal.Repository) *TitleRepository {
	return &TitleRepository{
		repo: repo,
	}
}

type TitleRepository struct {
	repo *bitemporal.Repository
}

func (r TitleRepository) ForEmployee(ctx context.Context, empNo int64) ([]Title, error) {
	rows, err := r.repo.Query(ctx, "SELECT emp_no, title, valid_to, valid_from, transaction_from, transaction_to FROM titles$ WHERE emp_no=? ORDER BY transaction_from, valid_to", empNo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var titles []Title
	for rows.Next() {
		title := Title{}
		err := rows.Scan(&title.EmpNo, &title.Title, &title.ValidTo, &title.ValidFrom, &title.TransactionFrom, &title.TransactionEnd)
		if err != nil {
			return nil, err
		}
		titles = append(titles, title)
	}
	return titles, nil
}
