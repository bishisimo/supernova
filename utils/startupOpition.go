/*
@author '彼时思默'
@time 2020/4/13 17:04
@describe:
*/
package utils

import (
	"flag"
	"os"
)

type Option struct {
	*ConnectOption `toml:"connectOption"`
	*QueryOption   `toml:"queryOption"`
	*StoreOption   `toml:"storeOption"`
	*S3Option      `toml:"s3Option"`
}

func NewOption() *Option {
	return &Option{
		ConnectOption: NewConnectOption(),
		QueryOption:   NewQueryOption(),
		StoreOption:   NewStoreOption(),
		S3Option:      NewS3Option(),
	}
}

type ConnectOption struct {
	DriveName *string `toml:"drive"` // 数据库名
	UserName  *string `toml:"user"`  // 用户名
	Password  *string `toml:"password"`  // 密码
	Ip        *string `toml:"ip"`        // 数据库IP
	Port      *uint   `toml:"port"`      // 端口
	DbName    *string `toml:"db"`    // 数据库名
}

type QueryOption struct {
	TableName      *string `toml:"table"`          // 表名
	FieldSelect    *string `toml:"select"`               // 查询字段
	FieldFlag      *string `toml:"field"`      // 增量标记字段
	StartFlag      *string `toml:"start"`      // 增量起始标志
	UnitFlag       *string `toml:"unit"`       // 增量起始标志增加单位
	IsStartContain *bool   `toml:"isStartContain"` // 增量是否包含起始值
	QueryContext   *string `toml:"query"`   // 自定义查询文本
}

type StoreOption struct {
	DestType      *string `toml:"destType"`      // 目的地类型
	DestPath      *string `toml:"destPath"`      // 目的地路径
	IsRetainLocal *bool   `toml:"isRetainLocal"` //是否删除本地文件
}

type S3Option struct {
	S3Id       *string `toml:"s3Id"`
	S3Secret   *string `toml:"s3Secret"`
	S3Endpoint *string `toml:"s3Endpoint"`
	S3Region   *string `toml:"s3Region"`
	S3Bucket   *string `toml:"s3Bucket"`
}

func NewConnectOption() *ConnectOption {
	return &ConnectOption{
		DriveName: new(string),
		UserName:  new(string),
		Password:  new(string),
		Ip:        new(string),
		Port:      new(uint),
		DbName:    new(string),
	}
}

func NewQueryOption() *QueryOption {
	return &QueryOption{
		TableName:      new(string),
		FieldSelect:    new(string),
		FieldFlag:      new(string),
		StartFlag:      new(string),
		UnitFlag:       new(string),
		IsStartContain: new(bool),
		QueryContext:   new(string),
	}
}

func NewStoreOption() *StoreOption {
	return &StoreOption{
		DestType:      new(string),
		DestPath:      new(string),
		IsRetainLocal: new(bool),
	}
}

func NewS3Option() *S3Option {
	getEnv:=func (key string) *string {
		result:=os.Getenv(key)
		return &result
	}
	return &S3Option{
		S3Id:       getEnv("S3Id"),
		S3Secret:   getEnv("S3Secret"),
		S3Endpoint: getEnv("S3Endpoint"),
		S3Region:   getEnv("S3Region"),
		S3Bucket:   getEnv("S3Bucket"),
	}
}

func NewConnectOptionWithFlag() *ConnectOption {
	result := &ConnectOption{
		DriveName: flag.String("drive", "", "数据库类型"),
		UserName:  flag.String("user", "", "数据库用户名"),
		Password:  flag.String("password", "", "数据库密码"),
		Ip:        flag.String("ip", "", "数据库ip地址"),
		Port:      flag.Uint("port", 0, "数据库端口,默认使用数据库的默认端口"),
		DbName:    flag.String("db", "", "数据库名"),
	}
	return result
}

func NewQueryOptionWithFlag() *QueryOption {
	result := &QueryOption{
		TableName:      flag.String("table", "", "数据表名(必填)"),
		FieldSelect:    flag.String("select", "", "需求字段(用','分割)"),
		FieldFlag:      flag.String("field", "", "增量汇集数据的字段(当需要增量更新时必须指定)"),
		StartFlag:      flag.String("start", "", "增量汇集的标记起始值(可选类型为数字与时间)"),
		UnitFlag:       flag.String("unit", "", "增量汇集定时单位(支持选项:hour,day,month,year)"),
		QueryContext:   flag.String("query", "", "自定义查询sql"),
		IsStartContain: flag.Bool("isStartContain", false, "是否包含增量汇集起始数据"),
	}
	return result
}

func NewStoreOptionWithFlag() *StoreOption {
	result := &StoreOption{
		DestType:      flag.String("destType", "", "目标路径类型"),
		DestPath:      flag.String("destPath", "", "目标路径"),
		IsRetainLocal: flag.Bool("isRetainLocal", false, "是否保留本地临时文件(默认不保存)"),
	}
	return result
}

func NewS3OptionWithFlag() *S3Option {
	result:=&S3Option{
		S3Id:       flag.String("s3Id", "", "s3的accessId"),
		S3Secret:   flag.String("s3Secret", "", "s3的accessSecret"),
		S3Endpoint: flag.String("s3Endpoint", "", "s3的endpoint"),
		S3Region:   flag.String("s3Region", "", "s3的region"),
		S3Bucket:   flag.String("s3Bucket", "", "s3的bucket"),
	}
	return result
}