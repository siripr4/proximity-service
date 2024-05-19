package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	datastore "proximity-service/pkg/datastore"

	"github.com/Masterminds/squirrel"
)

// TODO: replace db errors returned from funcs in this file to standard errors

type PSQLBusinessQueries struct {
	db *sql.DB
}

const (
	BUSINESS_TABLE   = "businesses"
	ID_COLUMN        = "id"
	NAME_COLUMN      = "name"
	ADDRESS_COLUMN   = "address"
	CITY_COLUMN      = "city"
	STATE_COLUMN     = "state"
	COUNTRY_COLUMN   = "country"
	LATITUDE_COLUMN  = "latitude"
	LONGITUDE_COLUMN = "longitude"
)

func (queries *PSQLBusinessQueries) getBusinessById(ctx context.Context, id int) (*datastore.Business, error) {
	sql, args, err := psql.Select(ID_COLUMN, NAME_COLUMN, ADDRESS_COLUMN, CITY_COLUMN, STATE_COLUMN, COUNTRY_COLUMN, LATITUDE_COLUMN, LONGITUDE_COLUMN).
		From(BUSINESS_TABLE).
		Where(squirrel.Eq{ID_COLUMN: id}).
		ToSql()
	if err != nil {
		return nil, err
	}

	row, err := queries.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	} else {
		var business datastore.Business
		if row.Next() {
			err := row.Scan(&business.Id,
				&business.Name,
				&business.Address,
				&business.City,
				&business.State,
				&business.Country,
				&business.Latitude,
				&business.Longitude)
			if err != nil {
				return nil, err
			}
			return &business, nil
		} else if err := row.Err(); err != nil {
			return nil, err
		} else {
			return nil, errors.New("no matching row found")
		}
	}
}

func (queries *PSQLBusinessQueries) addBusiness(ctx context.Context, business datastore.Business) (int64, error) {
	sql, args, err := psql.Insert(BUSINESS_TABLE).
		Columns(NAME_COLUMN, ADDRESS_COLUMN, CITY_COLUMN, STATE_COLUMN, COUNTRY_COLUMN, LATITUDE_COLUMN, LONGITUDE_COLUMN).
		Values(business.Name, business.Address, business.City, business.State, business.Country, business.Latitude, business.Longitude).
		ToSql()
	if err != nil {
		return -1, err
	}

	result, err := queries.db.ExecContext(ctx, sql, args...)
	if err != nil {
		return -1, err
	} else if id, err := result.LastInsertId(); err != nil {
		return -1, err
	} else {
		return id, nil
	}
}

func (queries *PSQLBusinessQueries) updateBusiness(ctx context.Context, business datastore.UpdateBusiness) (*datastore.Business, error) {
	updatesMap := getUpdatesMap(business)
	updateBuilder := psql.Update(BUSINESS_TABLE).Where(squirrel.Eq{ID_COLUMN: business.Id})
	for column, value := range updatesMap {
		if value != nil {
			updateBuilder.Set(column, value)
		}
	}

	query, args, err := updateBuilder.
		Suffix("RETURNING *").
		ToSql()
	if err != nil {
		return nil, err
	}
	row, err := queries.db.QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	} else {
		var business datastore.Business
		if row.Next() {
			err := row.Scan(&business.Id,
				&business.Name,
				&business.Address,
				&business.City,
				&business.State,
				&business.Country,
				&business.Latitude,
				&business.Longitude)
			if err != nil {
				return nil, err
			}
			return &business, nil
		} else if err := row.Err(); err != nil {
			return nil, err
		} else {
			return nil, errors.New("no matching row found")
		}
	}
}

func (queries *PSQLBusinessQueries) deleteBusiness(ctx context.Context, id int) error {
	sql, args, err := psql.Delete(BUSINESS_TABLE).Where(squirrel.Eq{ID_COLUMN: id}).ToSql()
	if err != nil {
		return err
	}

	result, err := queries.db.ExecContext(ctx, sql, args...)
	if err != nil {
		return err
	} else if rows, err := result.RowsAffected(); err != nil {
		return err
	} else if rows == 0 {
		return errors.New(fmt.Sprintf("no matching rows found for id: %d", id))
	}
	return nil
}

func getUpdatesMap(updateBusiness datastore.UpdateBusiness) map[string]interface{} {
	updatesMap := make(map[string]interface{})
	updatesMap[NAME_COLUMN] = updateBusiness.Name
	updatesMap[ADDRESS_COLUMN] = updateBusiness.Address
	updatesMap[CITY_COLUMN] = updateBusiness.City
	updatesMap[STATE_COLUMN] = updateBusiness.State
	updatesMap[COUNTRY_COLUMN] = updateBusiness.Country
	updatesMap[LATITUDE_COLUMN] = updateBusiness.Latitude
	updatesMap[LONGITUDE_COLUMN] = updateBusiness.Longitude
	return updatesMap
}
