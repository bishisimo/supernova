/*
@author '彼时思默'
@time 2020/4/8 15:30
@describe:
*/
package main

import (
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	. "github.com/bishisimo/s3c"
	"github.com/bishisimo/supernova/utils"
	"github.com/sirupsen/logrus"
	"os"
	"path"
	"time"
)

func main() {
	// 配置命令
	init := flag.Bool("init", false, "初始化配置文件")
	fPath := flag.String("file", "", "配置文件path")
	// 启动参数
	s3Option := utils.NewS3OptionWithFlag()
	connectOption := utils.NewConnectOptionWithFlag()
	queryOption := utils.NewQueryOptionWithFlag()
	storeOption := utils.NewStoreOptionWithFlag()
	flag.Parse()
	// 初始化配置文件
	if *init {
		option := utils.NewOption()
		fp, err := os.OpenFile("startup.toml", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)
		if err != nil {
			logrus.Panic("配置文件生成失败:", err)
		}
		tomlEncoder := toml.NewEncoder(fp)
		err = tomlEncoder.Encode(option)
		if err != nil {
			logrus.Panic("配置文件生成失败:", err)
		}
		fmt.Println("生成配置文件成功: startup.toml")
		return
	}
	// 读取配置文件
	if *fPath != "" {
		option := utils.NewOption()
		_, err := toml.DecodeFile(*fPath, option)
		if err != nil {
			logrus.Panic("解析配置文件异常:", err)
		}
		connectOption.Merge(option.ConnectOption)
		queryOption.Merge(option.QueryOption)
		storeOption.Merge(option.StoreOption)
		s3Option.Merge(option.S3Option)
	}
	// 配置S3环境变量
	s3Option.SetEnv()
	// 其他参数初始化
	var timePoint string
	if t, err := time.Parse("2006-01-02 15:04:05", *queryOption.StartFlag); err == nil {
		timePoint = utils.EndTimeCalculate(t, *queryOption.UnitFlag).Format("2006-01-02_15-04-05")
	} else {
		timePoint = time.Now().Format("2006-01-02_15-04-05")
	}

	if *storeOption.DestPath == "" {
		*storeOption.DestPath = path.Join(*connectOption.DbName, *queryOption.TableName, timePoint+".csv")
	}
	filePath := *queryOption.TableName + "_" + timePoint + ".csv"
	// 数据查询
	db := utils.NewMdbc(connectOption)
	data := db.SelectFilter(queryOption)
	defer func() {
		if !*storeOption.IsRetainLocal {
			err := os.Remove(filePath)
			if err != nil {
				logrus.Error("文件删除失败:", err)
			}
		}
	}()
	fp, err := utils.WriteCsv(filePath, data)
	if err != nil {
		logrus.Panic("转换为csv文件错误:", err)
	}
	defer func() {
		err := fp.Close()
		if err != nil {
			logrus.Error("关闭文件错误:", err)
		}
	}()
	_, _ = fp.Seek(0, 0)
	// 数据处理
	switch *storeOption.DestType {
	case "":
		logrus.Info("文件储存在本地:", filePath)
		*storeOption.IsRetainLocal = true
	case "s3":
		logrus.Info("文件上传中...:", *storeOption.DestPath)
		s3Connector := NewS3Connector()
		_ = s3Connector
		s3Connector.UploadFileByFP(fp, path.Join("data", *storeOption.DestPath))
	default:
		logrus.Error("暂未支持,储存至本地:", filePath)
		*storeOption.IsRetainLocal = true
	}
}
