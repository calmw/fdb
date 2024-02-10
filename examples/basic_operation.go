package main

import (
	"github.com/calmw/fdb"
	"log"
	"os"
)

func main() {
	opts := fdb.DefaultOption
	db, err := fdb.Open(opts)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(opts.DirPath)
	}()
	if err != nil {
		return
	}

	err = db.Put([]byte("name"), []byte("hello"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	log.Println("val=:", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}

	val2, err := db.Get([]byte("name"))
	if err != nil {
		log.Println("error :", err)
	} else {
		log.Println("val=:", string(val2))
	}
}
