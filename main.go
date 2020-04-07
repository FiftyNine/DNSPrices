package main

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/sergeilem/xls"
)

func findData(sheet *xls.WorkSheet) (firstRow int, idCol int, nameCol int, priceCol int, bonusCol int, err error) {
	var i int
	for ; i < int(sheet.MaxRow); i++ {
		idCol = -1
		priceCol = -1
		bonusCol = -1
		row := sheet.Row(int(i))
		// FirstCol and LastCol always return 0
		// Number of shops (i-1) + id col + category col + price col + bonus col + some leeway
		for j := 0; j < i+5; j++ {
			val := row.Col(j)
			switch val {
			case "Код":
				idCol = j
			case "Цена, руб":
				priceCol = j
			case "Бонусы":
				bonusCol = j
			}
		}
		if idCol >= 0 && priceCol >= 0 && bonusCol >= 0 {
			break
		}
	}
	if i < int(sheet.MaxRow) {
		return i, idCol, idCol + 1, priceCol, bonusCol, nil
	}
	return -1, -1, -1, -1, -1, errors.New("No entries found")
}

func extractData(sheet *xls.WorkSheet, writer DNSWriter, firstRow int, idCol int, nameCol int, priceCol int, bonusCol int) (int, int, int) {
	// var r *xls.Row
	var extracted int
	var newPrices, newBonuses int
	for i := firstRow; i < int(sheet.MaxRow); i++ {
		r := sheet.Row(i)
		id, err1 := strconv.Atoi(r.Col(idCol))
		price, err2 := strconv.Atoi(r.Col(priceCol))
		bonus, err3 := strconv.Atoi(r.Col(bonusCol))
		name := r.Col(nameCol)
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		extracted++
		if priceChanged, bonusChanged, err := writer.Write(id, name, price, bonus); err == nil {
			if priceChanged {
				newPrices++
			}
			if bonusChanged {
				newBonuses++
			}
		} else if err != nil {
			fmt.Printf("%s (%d/%d/%d)\r\n", err, id, price, bonus)
		}
	}
	return extracted, newPrices, newBonuses
}

func parseSheet(sheet *xls.WorkSheet, writer DNSWriter) {
	fmt.Printf("Processing \"%s\"...\r\n", sheet.Name)
	if firstRow, idCol, nameCol, priceCol, bonusCol, err := findData(sheet); err == nil {
		extracted, prices, bonuses := extractData(sheet, writer, firstRow, idCol, nameCol, priceCol, bonusCol)
		fmt.Printf("Extracted %d, prices changed %d, bonuses changed %d\r\n", extracted, prices, bonuses)
	} else {
		fmt.Println(err)
	}
}

func parseCity(file string) string {
	from := strings.LastIndex(file, "-")
	to := strings.LastIndex(file, ".")
	if from >= 0 && to > from {
		return file[from+1 : to]
	}
	return ""
}

var fileFlag = flag.String("f", "", "XLS file containing prices")
var dbFlag = flag.String("d", "", "Sqlite3 file containing database")

func main() {
	flag.Parse()
	book, err := xls.Open(*fileFlag, "utf-8")
	if err != nil {
		fmt.Println(err)
		return
	}
	city := parseCity(*fileFlag)
	if city == "" {
		fmt.Println("Failed to extract name of the city")
		return
	}
	// t1 := time.Now()
	w := NewSqliteWriter(*dbFlag, 0, city)
	for i := 0; i < book.NumSheets(); i++ {
		sheet := book.GetSheet(i)
		parseSheet(sheet, w)
	}
	// fmt.Println(time.Since(t1).Seconds())
	w.Close()
}
