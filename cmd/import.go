/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"github.com/BurntSushi/toml"
	"github.com/bishisimo/s3c"
	"github.com/bishisimo/supernova/utils"
	"github.com/bishisimo/supernova/utils/db"
	"github.com/bishisimo/supernova/utils/option"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
	"path"
	"time"
)

var importCfgPath string
var connectOption option.ConnectOption
var queryOption option.QueryOption
var storeOption option.StoreOption
var s3Option option.S3Option

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "导入文件",
	Long: `
--导入文件选项配置--
connectOption
queryOption
storeOption
s3Option
`,
	Run: func(cmd *cobra.Command, args []string) {
		if importCfgPath != "" {
			option := option.NewOption()
			_, err := toml.DecodeFile(importCfgPath, option)
			if err != nil {
				logrus.Panic("解析配置文件异常:", err)
			}
			connectOption.Merge(option.ConnectOption)
			queryOption.Merge(option.QueryOption)
			storeOption.Merge(option.StoreOption)
			s3Option.Merge(option.S3Option)
		}
		s3Option.SetEnv()
		var timePoint string
		if t, err := time.Parse("2006-01-02 15:04:05", queryOption.StartFlag); err == nil {
			timePoint = utils.EndTimeCalculate(t, queryOption.UnitFlag).Format("2006-01-02_15-04-05")
		} else {
			timePoint = time.Now().Format("2006-01-02_15-04-05")
		}

		if storeOption.DestPath == "" {
			storeOption.DestPath = path.Join(connectOption.DbName, queryOption.TableName, timePoint+".csv")
		}
		filePath := queryOption.TableName + "_" + timePoint + ".csv"
		// 数据查询
		db := db.NewMdbc(&connectOption)
		data := db.SelectFilter(&queryOption)
		defer func() {
			if !storeOption.IsRetainLocal {
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
		switch storeOption.DestType {
		case "":
			logrus.Info("文件储存在本地:", filePath)
			storeOption.IsRetainLocal = true
		case "s3":
			logrus.Info("文件上传中...:", storeOption.DestPath)
			s3Connector := s3c.NewS3Connector()
			_ = s3Connector
			s3Connector.UploadFileByFP(fp, path.Join("data", storeOption.DestPath))
		default:
			logrus.Error("暂未支持,储存至本地:", filePath)
			storeOption.IsRetainLocal = true
		}
	},
}

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.Flags().StringVarP(&importCfgPath, "config", "p", "", "指定配置文件")

	importCmd.Flags().StringVarP(&connectOption.DriveName, "drive", "d", "", "数据库类型")
	importCmd.Flags().StringVar(&connectOption.UserName, "user", "", "数据库用户名")
	importCmd.Flags().StringVar(&connectOption.Password, "psw", "", "数据库密码")
	importCmd.Flags().StringVar(&connectOption.Ip, "ip", "", "数据库ip地址")
	importCmd.Flags().UintVar(&connectOption.Port, "port", 0, "数据库端口,默认使用数据库的默认端口")
	importCmd.Flags().StringVar(&connectOption.DbName, "db", "", "数据库名")

	importCmd.Flags().StringVarP(&queryOption.TableName, "table", "t", "", "数据表名(必填)")
	importCmd.Flags().StringVarP(&queryOption.FieldSelect, "select", "e", "", "需求字段(用','分割)")
	importCmd.Flags().StringVarP(&queryOption.FieldFlag, "field", "f", "", "增量汇集数据的字段(当需要增量更新时必须指定)")
	importCmd.Flags().StringVarP(&queryOption.StartFlag, "start", "s", "", "增量汇集的标记起始值(可选类型为数字与时间)")
	importCmd.Flags().StringVarP(&queryOption.UnitFlag, "unit", "u", "", "增量汇集定时单位(支持选项:hour,day,month,year)")
	importCmd.Flags().StringVarP(&queryOption.QueryContext, "query", "q", "", "自定义查询sql")
	importCmd.Flags().BoolVarP(&queryOption.IsStartContain, "isStartContain", "c", false, "是否包含增量汇集起始数据")

	importCmd.Flags().StringVar(&storeOption.DestType, "destType", "", "目标路径类型")
	importCmd.Flags().StringVar(&storeOption.DestPath, "destPath", "", "目标路径")
	importCmd.Flags().BoolVarP(&storeOption.IsRetainLocal, "isRetainLocal", "l", false, "是否保留本地临时文件(默认不保存)")

	importCmd.Flags().StringVar(&s3Option.S3Id, "s3Id", "", "s3的accessId")
	importCmd.Flags().StringVar(&s3Option.S3Secret, "s3Secret", "", "s3的accessSecret")
	importCmd.Flags().StringVar(&s3Option.S3Endpoint, "s3Endpoint", "", "s3的endpoint")
	importCmd.Flags().StringVar(&s3Option.S3Region, "s3Region", "", "s3的region")
	importCmd.Flags().StringVar(&s3Option.S3Bucket, "s3Bucket", "", "s3的bucket")
}
