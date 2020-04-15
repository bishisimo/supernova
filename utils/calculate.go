/*
@author '彼时思默'
@time 2020/4/15 10:02
@describe:
*/
package utils

import "time"

func EndTimeCalculate(t time.Time,unitFlag string) time.Time {
	switch unitFlag {
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
	return t
}