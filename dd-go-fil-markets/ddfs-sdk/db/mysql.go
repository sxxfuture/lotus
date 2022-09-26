//DD add
package db

import (
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"sync"
)

func NewMysqlDB(url string) (*gorm.DB, error) {
	var err error
	var Eloquent *gorm.DB
	Eloquent, err = gorm.Open(mysql.Open(url), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	sqlDB, err := Eloquent.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(4)
	return Eloquent, nil
}

const FILECOIN_MARKET_DB_ENV = "FILECOIN_MARKET_DB"

var InitDB = func() func() (*gorm.DB, error) {
	var (
		GormDBMysql    *gorm.DB
		GormDBMysqlErr error
		onceInitDB     sync.Once
	)
	return func() (*gorm.DB, error) {
		onceInitDB.Do(func() {
			if dbUrl := os.Getenv(FILECOIN_MARKET_DB_ENV); dbUrl != "" {
				mysqlDB, err := NewMysqlDB(dbUrl)
				if err != nil {
					GormDBMysqlErr = err
					return
				}
				GormDBMysql = mysqlDB
			}
		})
		return GormDBMysql, GormDBMysqlErr
	}
}()
