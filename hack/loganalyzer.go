package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/olivere/ndjson"
)

//
//var fileName string
//
//func init() {
//	flag.StringVar(&fileName, "f", "", "the file to parse")
//}

func main() {
	log.Println(os.Args)
	info, err := os.Stdin.Stat()
	if err != nil {
		log.Fatalln(err)
	}

	var in io.Reader = os.Stdin
	finish := func() {}
	if info.Mode()&os.ModeCharDevice != 0 || info.Size() <= 0 {
		// no pipe, check arg if it contains a file
		var valid bool
		if os.Args[1] != "" {
			f, err := os.Open(os.Args[1])
			if err != nil {
				log.Fatalln(err)
			}
			in, finish = f, func() { _ = f.Close() }
		}
		if !valid {
			fmt.Println("The command is intended to work with pipes.")
			fmt.Println("Usage: loganalyzer ./path/to/file or some_process | loganalyzer")
			return
		}
	}
	defer finish()

	rdr := ndjson.NewReader(in)
	for rdr.Next() {

	}
}
