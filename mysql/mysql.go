/*
 * @Author:    thepoy
 * @Email:     thepoy@163.com
 * @File Name: mysql.go
 * @Created:   2022-10-30 20:17:35
 * @Modified:  2022-10-30 20:29:34
 */

package mysql

import (
	"errors"
	"strings"

	"github.com/go-predator/predator"
	"github.com/go-predator/tools"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	URIRequiredError = errors.New("uri is required")
)

type MySQLCache struct {
	Host, Port, Database, Username, Password string
	db                                       *gorm.DB
	compressed                               bool
}

func (mc *MySQLCache) uri() string {
	var s strings.Builder

	s.WriteString(mc.Username)
	s.WriteString(":")
	s.WriteString(mc.Password)
	s.WriteString("@")
	s.WriteString("tcp(")
	s.WriteString(mc.Host)
	s.WriteString(":")
	s.WriteString(mc.Port)
	s.WriteString(")/")
	s.WriteString(mc.Database)
	s.WriteString("?charset=utf8mb4&parseTime=True&loc=Local")

	return s.String()
}

func (mc *MySQLCache) Init() error {
	uri := mc.uri()
	if uri == "" {
		return URIRequiredError
	}

	db, err := gorm.Open(mysql.Open(uri), &gorm.Config{
		PrepareStmt: true,
		Logger:      logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return err
	}

	mc.db = db

	err = mc.db.AutoMigrate(&predator.CacheModel{})
	if err != nil {
		return err
	}

	return nil
}

func (mc *MySQLCache) IsCached(key string) ([]byte, bool) {
	var cache predator.CacheModel

	err := mc.db.Where("`key` = ?", key).First(&cache).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false
		}

		panic(err)
	}

	if cache.Value != nil {
		if mc.compressed {
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

func (mc *MySQLCache) Cache(key string, val []byte) error {
	// 这里不能用 CheckCache，因为 value 值很长，获取 Value 和解压过程耗时较长
	var count int
	err := mc.db.Model(&predator.CacheModel{}).Select("COUNT(*)").Where("`key` = ?", key).Scan(&count).Error
	if err != nil {
		return err
	}

	if count == 0 {
		if mc.compressed {
			val = tools.Compress(val)
		}

		return mc.db.Create(&predator.CacheModel{
			Key:   key,
			Value: val,
		}).Error
	}

	return nil
}

func (mc *MySQLCache) Clear() error {
	return mc.db.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&predator.CacheModel{}).Error
}

func (mc *MySQLCache) Compressed(yes bool) {
	mc.compressed = yes
}
