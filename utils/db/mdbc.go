/*
@author '彼时思默'
@time 2020/4/8 16:35
@describe:
*/
package db

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bishisimo/supernova/utils"
	"github.com/bishisimo/supernova/utils/option"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	//_ " github.com/mattn/go-oci8"
	//_ " github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strconv"
	"time"
	"unicode"
	"xorm.io/xorm"
)

type Mdbc struct {
	DbEngine    *xorm.Engine
	MongoEngine *mongo.Client
	DbMongo     *mongo.Database
}

func NewMdbc(dco *option.ConnectOption) *Mdbc {
	defaultPort := map[string]uint{
		"mysql":      3306,
		"mongo":      27017,
		"sqlserver":  1433,
		"oracle":     1521,
		"postgresql": 5432,
	}
	sqlEngineSupper := map[string]bool{
		"mysql":      true,
		"sqlserver":  true,
		"oracle":     false,
		"postgresql": false,
	}
	if dp := defaultPort[dco.DriveName]; dp != 0 && dco.Port == 0 {
		dco.Port = dp
	} else {
		logrus.Panic(dco.DriveName, "数据库暂未支持自动获取默认端口")
	}
	var db *xorm.Engine
	var mongoClient *mongo.Client
	var dbMongo *mongo.Database
	var err error
	switch dco.DbName {
	case "mongo":
		dataSourceName := fmt.Sprintf("mongodb://%s:%d", dco.Ip, dco.Port)
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		mongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI(dataSourceName))
		if err != nil {
			logrus.Panic("mongo连接失败:", err)
		}
		dbMongo = mongoClient.Database(dco.DbName)
	default:
		if sqlEngineSupper[dco.DriveName] {
			dataSourceName := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8",
				dco.UserName, dco.Password, dco.Ip, dco.Port, dco.DbName)
			db, _ = xorm.NewEngine(dco.DriveName, dataSourceName)
			err := db.Ping()
			if err != nil {
				logrus.Panic("连接数据库失败！:", err)
			}
		} else {
			logrus.Panic("不支持的数据库类型:driveName")
		}
	}

	return &Mdbc{
		DbEngine:    db,
		MongoEngine: mongoClient,
		DbMongo:     dbMongo,
	}
}

//按指定条件过滤查询
func (m Mdbc) SelectFilter(qo *option.QueryOption) *[]map[string]string {
	var results *[]map[string]string
	if m.DbEngine != nil {
		results = m.OrmQuery(qo)
	} else if m.DbMongo != nil {
		results = m.MongoQuery(qo)
	} else {
		logrus.Panic("数据查失败:不支持的数据库类型!")
	}
	return results
}

func (m Mdbc) OrmQuery(qo *option.QueryOption) *[]map[string]string {
	var results []map[string]string
	var err error
	var queryContext string
	if qo.FieldSelect == "" {
		qo.FieldSelect = "*"
	}
	queryContext = fmt.Sprintf("select %s from `%s` ", qo.FieldSelect, qo.TableName)
	if qo.QueryContext != "" {
		queryContext = qo.QueryContext
	} else if qo.FieldFlag != "" && qo.StartFlag != "" {
		if qo.IsStartContain {
			queryContext += fmt.Sprintf("where `%s`>='%s' ", qo.FieldFlag, qo.StartFlag)
		} else {
			queryContext += fmt.Sprintf("where `%s`>'%s' ", qo.FieldFlag, qo.StartFlag)
		}
		if t, err := time.Parse("2006-01-02 15:04:05", qo.StartFlag); err == nil {
			t = utils.EndTimeCalculate(t, qo.UnitFlag)
			queryContext += fmt.Sprintf("and `%s` < '%s' ", qo.FieldFlag, t.Format("2006-01-02 15:04:05"))
		}
	}
	fmt.Println("queryContext:", queryContext)
	results, err = m.DbEngine.QueryString(queryContext)
	if err != nil {
		logrus.Panic("查询失败", err)
	}
	return &results
}

func (m Mdbc) MongoQuery(qo *option.QueryOption) *[]map[string]string {
	results := make([]map[string]string, 0, 10000)
	collection := m.DbMongo.Collection(qo.TableName)
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	filter := bson.M{}
	if qo.QueryContext != "" { // 指定查询语句
		err := json.Unmarshal([]byte(qo.QueryContext), &filter)
		if err != nil {
			fmt.Println(qo.QueryContext)
			logrus.Panic("反序列化失败:", err)
		}
	} else if qo.FieldFlag != "" && qo.StartFlag != "" { // 指定查询字段
		isNumber := true
		for _, c := range qo.StartFlag {
			if !unicode.IsNumber(c) {
				isNumber = false
			}
		}
		if isNumber {
			sti, _ := strconv.Atoi(qo.StartFlag)
			if qo.IsStartContain {
				filter[qo.FieldFlag] = bson.M{"$gte": sti}
			} else {
				filter[qo.FieldFlag] = bson.M{"$gt": sti}
			}
		} else {
			if qo.IsStartContain {
				filter[qo.FieldFlag] = bson.M{"$gte": qo.StartFlag}
			} else {
				filter[qo.FieldFlag] = bson.M{"$gt": qo.StartFlag}
			}
			if t, err := time.Parse("2006-01-02 15:04:05", qo.StartFlag); err == nil {
				switch qo.UnitFlag {
				case "hour":
					t = t.Add(time.Second * 3600)
					t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.Local)
				case "day":
					t = t.AddDate(0, 0, 1)
					t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local)
				case "month":
					t = t.AddDate(0, 0, 1)
					t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.Local)
				case "year":
					t = t.AddDate(1, 0, 0)
					t = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.Local)
				default:
					t = time.Now()
				}
				filter[qo.FieldFlag].(bson.M)["$lt"] = t.Format("2006-01-02")
			}
		}
	}
	cur, err := collection.Find(ctx, filter, options.Find())
	if err != nil {
		logrus.Panic("mongo 查找错误:", err)
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var re map[string]interface{}
		err := cur.Decode(&re)
		delete(re, "_id")
		if err != nil {
			logrus.Panic(err)
		}
		result := m.row2str(&re)
		results = append(results, result)
	}
	if err := cur.Err(); err != nil {
		logrus.Panic(err)
	}
	return &results
}

func (m Mdbc) row2str(dp *map[string]interface{}) map[string]string {
	result := make(map[string]string, len(*dp))
	for key, value := range *dp {
		result[key] = fmt.Sprintf("%v", value)
		//switch v:=value.(type) {
		//case int64:
		//	result[key]=strconv.FormatInt(v,10)
		//case int32:
		//	result[key]=strconv.FormatInt(v,10)
		//case int16:
		//	result[key]=number
		//case int8:
		//	result[key]=strconv.FormatInt(v,10)
		//case int:
		//	result[key]=strconv.FormatInt(v,10)
		//case float64:
		//	result[key]=strconv.FormatFloat(v,'f',-1,64)
		//case string:
		//	result[key]=v
		//default:
		//	fmt.Printf("数据类型暂未支持:%T\n",v)
		//}
	}
	return result
}
