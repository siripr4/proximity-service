package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Masterminds/squirrel"
)

// TODO: replace db errors returned from funcs in this file to standard errors

type PSQLGeospatialIndexQueries struct {
	db *sql.DB
}

const (
	GEOSPATIAL_INDEX_TABLE = "geospatial_index"
	GEOHASH_COLUMN         = "geohash_column"
	BUSINESS_ID_COLUMN     = "business_id_column"
)

func (queries *PSQLGeospatialIndexQueries) GetBusinessIdsByGeohash(ctx context.Context, geohash string) ([]int, error) {
	query, args, err := psql.Select(GEOHASH_COLUMN, BUSINESS_ID_COLUMN).
		From(GEOSPATIAL_INDEX_TABLE).
		Where(squirrel.Eq{GEOHASH_COLUMN: geohash}).
		ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := queries.db.QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}

	businessIds := make([]int, 0)
	for rows.Next() {
		var businessId int
		err := rows.Scan(&businessId)
		if err != nil {
			// TODO: log error
			continue
		}
		businessIds = append(businessIds, businessId)
	}
	return businessIds, nil
}

func (queries *PSQLGeospatialIndexQueries) GetGeohashesForBusinessId(ctx context.Context, businessId int) ([]string, error) {
	query, args, err := psql.Select(GEOHASH_COLUMN, BUSINESS_ID_COLUMN).
		From(GEOSPATIAL_INDEX_TABLE).
		Where(squirrel.Eq{BUSINESS_ID_COLUMN: businessId}).
		ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := queries.db.QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}

	geohashes := make([]string, 0)
	for rows.Next() {
		var geohash string
		err := rows.Scan(&geohash)
		if err != nil {
			// TODO: log error
			continue
		}
		geohashes = append(geohashes, geohash)
	}
	return geohashes, nil
}

func (queries *PSQLBusinessQueries) AddGeospatialIndex(ctx context.Context, geohash string, businessId int) error {
	sql, args, err := psql.Insert(GEOSPATIAL_INDEX_TABLE).
		Columns(GEOHASH_COLUMN, BUSINESS_ID_COLUMN).
		Values(geohash, businessId).
		ToSql()
	if err != nil {
		return err
	}

	result, err := queries.db.ExecContext(ctx, sql, args...)
	if err != nil {
		return err
	} else if _, err := result.LastInsertId(); err != nil {
		return err
	} else {
		return nil
	}
}

func (queries *PSQLGeospatialIndexQueries) DeleteGeospatialIndex(ctx context.Context, geohash string, businessId int) error {
	query, args, err := psql.Delete(GEOSPATIAL_INDEX_TABLE).
		Where(squirrel.Eq{GEOHASH_COLUMN: geohash, BUSINESS_ID_COLUMN: businessId}).
		ToSql()
	if err != nil {
		return err
	}

	result, err := queries.db.ExecContext(ctx, query, args)
	if err != nil {
		return err
	} else if rows, err := result.RowsAffected(); err != nil {
		return err
	} else if rows == 0 {
		return errors.New(fmt.Sprintf("no matching rows found for geohash: %v, businessId: %d", geohash, businessId))
	}
	return nil
}
