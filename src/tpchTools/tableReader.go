package tpchTools

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	colSeparator = '|'
)

func ReadTable(fileLoc string, nParts int, nEntries int) (tableBuf [][]string) {
	if file, err := getFile(fileLoc); err == nil {
		defer file.Close()
		tableBuf = make([][]string, nEntries, nEntries)
		scanner := bufio.NewScanner(file)
		i := 0
		for ; scanner.Scan(); i++ {
			tableBuf[i] = processLine(scanner.Text(), nParts)
		}
	}
	return
}

func processLine(line string, nParts int) (result []string) {
	result = make([]string, nParts)

	var curr strings.Builder
	i := 0
	for _, char := range line {
		if char != colSeparator {
			curr.WriteRune(char)
		} else {
			result[i] = curr.String()
			curr.Reset()
			i++
		}
	}

	return
}

func ReadHeaders(headerLoc string, nTables int) (headers [][]string, keys [][]int) {
	if file, err := getFile(headerLoc); err == nil {
		defer file.Close()
		//table -> array of fieldNames
		headers = make([][]string, nTables)
		//For each table, contains the position in the header of the primary key components
		keys = make([][]int, nTables)
		scanner := bufio.NewScanner(file)

		nLine, nCol := -1, -1
		key := strings.Builder{}
		for scanner.Scan() {
			processHeaderLine(scanner.Text(), headers, keys, &nLine, &nCol, &key)
		}
	}
	return
}

func processHeaderLine(line string, headers [][]string, keys [][]int, nLine *int, nCol *int, key *strings.Builder) {
	if len(line) == 0 {
		return
	}
	//*: tableName. $: number of entries/fields. #: parts of primary key
	switch line[0] {
	case '*':
		*nLine++
		keys[*nLine] = make([]int, 0, 4)
	case '$':
		nEntries, _ := strconv.Atoi(line[1:])
		*nCol = 0
		headers[*nLine] = make([]string, nEntries)
	case '#':
		headers[*nLine][*nCol] = line[1:]
		keys[*nLine] = append(keys[*nLine], *nCol)
		*nCol++
	default:
		headers[*nLine][*nCol] = line
		*nCol++
	}
	return
}

//File order: orders, lineitem, delete
//fileLocs and nEntries are required for the 3 files. nParts is only required for the first two.
func ReadUpdates(fileLocs []string, nEntries []int, nParts []int) (ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string) {
	ordersUpds = processUpdFile(fileLocs[0], nEntries[0], nParts[0])
	lineItemUpds = processUpdFile(fileLocs[1], nEntries[1], nParts[1])
	deleteKeys = processDeleteFile(fileLocs[2], nEntries[2])
	return
}

func processUpdFile(fileLoc string, nEntries int, nParts int) (tableUpds [][]string) {
	if file, err := getFile(fileLoc); err == nil {
		defer file.Close()
		tableUpds = make([][]string, nEntries, nEntries)
		scanner := bufio.NewScanner(file)
		i := 0
		for ; scanner.Scan(); i++ {
			tableUpds[i] = processLine(scanner.Text(), nParts)
		}
	}
	return
}

func processDeleteFile(fileLoc string, nEntries int) (deleteKeys []string) {
	if file, err := getFile(fileLoc); err == nil {
		defer file.Close()
		deleteKeys = make([]string, nEntries, nEntries)
		scanner := bufio.NewScanner(file)
		i := 0
		currLine := ""
		for ; scanner.Scan(); i++ {
			currLine = scanner.Text()
			//deleteKeys[i], _ = strconv.Atoi(currLine[:len(currLine)-1])
			//Removing the "|" at the end
			deleteKeys[i] = currLine[:len(currLine)-1]
		}
	}
	return
}

func getFile(fileLoc string) (file *os.File, err error) {
	file, err = os.Open(fileLoc)
	if err != nil {
		fmt.Println("Failed to open file", fileLoc)
		fmt.Println(err)
	}
	return
}
