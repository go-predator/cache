/*
 * @Author:    thepoy
 * @Email:     thepoy@163.com
 * @File Name: sqlite.go
 * @Created:   2021-11-24 19:44:36
 * @Modified:  2022-10-30 20:09:37
 */

package sqlite

import (
	"errors"

	"github.com/go-predator/predator"
	"github.com/go-predator/tools"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type SQLiteCache struct {
	URI        string
	db         *gorm.DB
	compressed bool
}

func (sc *SQLiteCache) Init() error {
	if sc.URI == "" {
		sc.URI = "predator-cache.sqlite"
	}

	db, err := gorm.Open(sqlite.Open(sc.URI), &gorm.Config{
		PrepareStmt: true,
		Logger:      logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return err
	}
	sc.db = db

	err = sc.db.AutoMigrate(&predator.CacheModel{})
	if err != nil {
		return err
	}
	return nil
}

func (sc *SQLiteCache) IsCached(key string) ([]byte, bool) {
	var cache predator.CacheModel
	err := sc.db.Where("`key` = ?", key).First(&cache).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false
		}
		panic(err)
	}

	if cache.Value != nil {
		if sc.compressed {
			dec, err := tools.Decompress(cache.Value)
			if err != nil {
				panic(err)
			}
			return dec, true
		}
		return cache.Value, true
	}
	return nil, false
}

func (sc *SQLiteCache) Cache(key string, val []byte) error {
	var count int
	err := sc.db.Model(&predator.CacheModel{}).Select("COUNT(*)").Where("`key` = ?", key).Scan(&count).Error
	if err != nil {
		return err
	}

	if count == 0 {
		if sc.compressed {
			val = tools.Compress(val)
		}
		return sc.db.Create(&predator.CacheModel{
			Key:   key,
			Value: val,
		}).Error
	}

	return nil
}

func (sc *SQLiteCache) Clear() error {
	return sc.db.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&predator.CacheModel{}).Error
}

func (sc *SQLiteCache) Compressed(yes bool) {
	sc.compressed = yes
}
