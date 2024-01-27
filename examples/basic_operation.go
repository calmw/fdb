package main

import (
	"fdb"
	"fmt"
)

func main() {
	opts := fdb.DefaultOption
	db, err := fdb.Open(opts)
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
	fmt.Println("val=:", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}

	val2, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val=:", string(val2))
}
