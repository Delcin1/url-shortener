package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/v5/stdlib"
	"url-shortener/internal/storage"
)

type Storage struct {
	db *sql.DB
}

func New(dbUrl string) (*Storage, error) {
	const op = "storage.postgres.New"

	db, err := sql.Open("pgx", dbUrl)
	if err != nil {
		return nil, fmt.Errorf("%s : %w", op, err)
	}

	stmt, err := db.Prepare(`
	CREATE TABLE IF NOT EXISTS alias_url(
	    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	    alias TEXT NOT NULL UNIQUE,
	    url_address TEXT NOT NULL);
	`)
	if err != nil {
		return nil, fmt.Errorf("%s : %w", op, err)
	}

	_, err = stmt.Exec()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	stmt, err = db.Prepare(`
	CREATE INDEX IF NOT EXISTS idx_alias ON alias_url(alias);
	`)
	if err != nil {
		return nil, fmt.Errorf("%s : %w", op, err)
	}

	_, err = stmt.Exec()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Storage{db: db}, nil
}

func (s *Storage) SaveURL(urlToSave string, alias string) error {
	const op = "storage.postgres.SaveURL"
	const UniqueViolation = "23505"

	_, err := s.db.Exec("INSERT INTO alias_url(alias, url_address) VALUES ($1, $2)", alias, urlToSave)
	if err != nil {
		var pgErr pgx.PgError
		if errors.As(err, &pgErr) && pgErr.Code == UniqueViolation {
			return fmt.Errorf("%s: %w", op, storage.ErrURLExists)
		}
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (s *Storage) GetURL(alias string) (string, error) {
	const op = "storage.postgres.GetURL"

	rows, err := s.db.Query("SELECT url_address FROM alias_url WHERE alias=$1", alias)
	if err != nil {
		return "", fmt.Errorf("%s: %w", op, storage.ErrURLNotFound)
	}

	var urlStr string
	if rows.Next() == false {
		return "", fmt.Errorf("url not found")
	}
	err = rows.Scan(&urlStr)
	if err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}
	if rows.Next() {
		return "", fmt.Errorf("storage contains multiply aliases")
	}

	return urlStr, nil
}

func (s *Storage) DeleteURL(alias string) error {
	const op = "storage.postgres.DeleteURL"

	_, err := s.db.Exec("DELETE FROM alias_url WHERE alias=$1", alias)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}
