package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Create(doc CreateDocumentRequest) (string, error) {
	var id string

	err := r.db.QueryRow(
		context.Background(),
		`INSERT INTO documents (tenant_id, title, content)
		 VALUES ($1, $2, $3)
		 RETURNING id`,
		doc.TenantID,
		doc.Title,
		doc.Content,
	).Scan(&id)

	return id, err
}

func (r *Repository) Get(id string) (*Document, error) {
	var doc Document

	err := r.db.QueryRow(
		context.Background(),
		`SELECT id, tenant_id, title, content, created_at
		 FROM documents
		 WHERE id=$1`,
		id,
	).Scan(
		&doc.ID,
		&doc.TenantID,
		&doc.Title,
		&doc.Content,
		&doc.CreatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &doc, nil
}

func (r *Repository) Delete(id string) error {
	_, err := r.db.Exec(
		context.Background(),
		`DELETE FROM documents WHERE id=$1`,
		id,
	)

	return err
}

func (r *Repository) Search(tenant, query string) ([]SearchResult, error) {

	rows, err := r.db.Query(
		context.Background(),
		`SELECT
			id,
			title,
			ts_rank(search_vector, plainto_tsquery($2)) AS rank
		FROM documents
		WHERE tenant_id=$1
		AND search_vector @@ plainto_tsquery($2)
		ORDER BY rank DESC`,
		tenant,
		query,
	)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var results []SearchResult

	for rows.Next() {
		var rlt SearchResult

		if err := rows.Scan(
			&rlt.ID,
			&rlt.Title,
			&rlt.Rank,
		); err != nil {
			return nil, err
		}

		results = append(results, rlt)
	}

	return results, nil
}
