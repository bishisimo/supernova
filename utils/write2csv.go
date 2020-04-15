/*
@author '彼时思默'
@time 2020/4/9 15:08
@describe:
*/
package utils

import (
	"encoding/csv"
	"errors"
	"github.com/sirupsen/logrus"
	"os"
	"sort"
)

func WriteCsv(path string,data *[]map[string]string) (*os.File,error) {
	rowNum:=len(*data)
	if rowNum==0{
		logrus.Error("无法解析空数据为csv文件！")
		return nil,errors.New("data is nil!")
	}
	columnNum:=len((*data)[0])
	keys:=make([]string,0,columnNum)
	for key:=range (*data)[0]{
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows:=make([][]string,0,rowNum*columnNum)
	for index:=range *data{
		row:=make([]string,0,columnNum)
		for key:=range keys{
			row = append(row, (*data)[index][keys[key]])
		}
		rows = append(rows, row)
	}
	fp, _ := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	w := csv.NewWriter(fp)
	_=w.Write(keys)
	_=w.WriteAll(rows)
	return fp,nil
}

type FlowController struct {
	Title []string
	Fp *os.File
	Writer *csv.Writer
}

func NewFlowController(path string) *FlowController{
	fp, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		logrus.Panic("打开文件错误:",err)
	}
	return &FlowController{
		Fp :fp,
		Writer: csv.NewWriter(fp),
	}
}

func (f FlowController) WriteCsvFlow(data *map[string]string) error {
	if data==nil{
		logrus.Error("无法解析空数据为csv文件！")
		return errors.New("data is nil!")
	}
	columnNum:=len((*data))
	if f.Title==nil{
		f.Title=make([]string,0,columnNum)
		for key:=range (*data){
			f.Title = append(f.Title, key)
		}
		sort.Strings(f.Title)
		err:=f.Writer.Write(f.Title)
		if err != nil {
			logrus.Panic("写入标题出错:",err)
		}
	}
	row:=make([]string,0,columnNum)
	for key:=range f.Title{
		row = append(row, (*data)[f.Title[key]])
	}
	err:=f.Writer.Write(row)
	if err != nil {
		logrus.Panic("写入数据出错:",err)
	}
	return nil
}