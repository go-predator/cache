/*
 * @Author:    thepoy
 * @Email:     thepoy@163.com
 * @File Name: postgres.go
 * @Created:   2022-10-30 20:39:20
 * @Modified:  2022-10-30 20:47:24
 */

package postgres

import (
	"errors"
	"strings"

	"github.com/go-predator/predator"
	"github.com/go-predator/tools"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	URIRequiredError = errors.New("uri is required")
)

type PostgreSQLCache struct {
	Host, Port, Database, Username, Password, SSLMode, TimeZone string
	db                                                          *gorm.DB
	compressed                                                  bool
}

func (pc *PostgreSQLCache) uri() string {
	if pc.SSLMode == "" {
		pc.SSLMode = "disable"
	}

	if pc.TimeZone == "" {
		pc.TimeZone = "Asia/Shanghai"
	}

	var s strings.Builder

	s.WriteString("host=")
	s.WriteString(pc.Host)
	s.WriteString(" user=")
	s.WriteString(pc.Username)
	s.WriteString(" password=")
	s.WriteString(pc.Password)
	s.WriteString(" dbname=")
	s.WriteString(pc.Database)
	s.WriteString(" port=")
	s.WriteString(pc.Port)
	s.WriteString(" sslmode=")
	s.WriteString(pc.SSLMode)
	s.WriteString(" TimeZone=")
	s.WriteString(pc.TimeZone)

	return s.String()
}

func (pc *PostgreSQLCache) Init() error {
	uri := pc.uri()
	if uri == "" {
		return URIRequiredError
	}

	db, err := gorm.Open(postgres.Open(uri), &gorm.Config{
		PrepareStmt: true,
		Logger:      logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return err
	}

	pc.db = db

	err = pc.db.AutoMigrate(&predator.CacheModel{})
	if err != nil {
		return err
	}

	return nil
}

func (pc *PostgreSQLCache) IsCached(key string) ([]byte, bool) {
	var cache predator.CacheModel
	err := pc.db.Where("key = ?", key).First(&cache).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false
		}

		panic(err)
	}

	if cache.Value != nil {
		if pc.compressed {
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

func (pc *PostgreSQLCache) Cache(key string, val []byte) error {
	// 这里不能用 CheckCache，因为 value 值很长，获取 Value 和解压过程耗时较长
	var count int
	err := pc.db.Model(&predator.CacheModel{}).
		Select("COUNT(*)").
		Where("key = ?", key).
		Scan(&count).
		Error
	if err != nil {
		return err
	}

	if count == 0 {
		if pc.compressed {
			val = tools.Compress(val)
		}

		return pc.db.Create(&predator.CacheModel{
			Key:   key,
			Value: val,
		}).Error
	}

	return nil
}

func (pc *PostgreSQLCache) Clear() error {
	return pc.db.Session(&gorm.Session{AllowGlobalUpdate: true}).
		Delete(&predator.CacheModel{}).
		Error
}

func (pc *PostgreSQLCache) Compressed(yes bool) {
	pc.compressed = yes
}
