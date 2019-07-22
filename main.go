package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"time"
)

const hostUrl = "https://binance.com"
const apiResourceUri = "/api/v1/klines"
const apiGetSymbolUri = "/api/v1/exchangeInfo"

const downloadLocation = "data"

type Symbol struct {
	Symbol string
}

type GetSymbolsResponse struct {
	Symbols []Symbol
}

//Gets all available symbols from binance
func GetSymbols() []string {
	resp, err := http.Get(hostUrl + apiGetSymbolUri)

	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	response := new(GetSymbolsResponse)
	json.NewDecoder(resp.Body).Decode(response)
	fmt.Println(len(response.Symbols))
	result := make([]string, 0)
	for _, val := range response.Symbols {
		result = append(result, val.Symbol)
	}
	return result
}

func checkDownloadLocation() {
	if _, err := os.Stat(downloadLocation); os.IsNotExist(err) {
		os.Mkdir(downloadLocation, os.ModeDir)
	}
}

func createGetSymbolUrl(symbol string, start, stop int64) string {

	ret := fmt.Sprintf("%s%s?symbol=%s&interval=1d&startTime=%d&endTime=%d", hostUrl, apiResourceUri, symbol, start, stop)
	return ret
}

func downloadMonthOfData(symbol string, year, month int, c chan bool) {

	signal := <-c // wait until signal to start

	if !signal {
		return
	}
	start := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(year, time.Month(month+1), 1, 0, 0, 0, 0, time.UTC)

	if start.After(time.Now()) {
		return
	}

	fileName := fmt.Sprintf("%s_%d_%02d.json", symbol, year, month)

	fmt.Println("Downloading: " + fileName)

	pathToFile := path.Join(downloadLocation, fileName)

	if _, err := os.Stat(pathToFile); os.IsNotExist(err) {
		r, e := http.Get(createGetSymbolUrl(symbol, start.Unix()*1000, end.Unix()*1000))
		if e != nil {
			panic(e)
		}
		defer r.Body.Close()
		out, _ := os.Create(pathToFile)
		defer out.Close()
		io.Copy(out, r.Body)
	}

}

func loadHistoricalData() {

	checkDownloadLocation()

	fmt.Println("Getting symbols")
	r := GetSymbols()
	count := len(r)

	syncChannel := make(chan bool, 3) // concurrent jobs at most
	jobsCount := 0
	fmt.Println("Found %v symbols", count)
	for _, b := range r {
		for year := time.Now().Year(); year > 2010; year-- {
			for month := 12; month >= 1; month-- {
				jobsCount++
				go downloadMonthOfData(b, year, month, syncChannel)
			}
		}
	}

	for i := 0; i < jobsCount; i++ {
		syncChannel <- true
	}
	fmt.Println("All done")
}

func main() {

	loadHistoricalData()
}
