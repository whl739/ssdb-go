package main

import "fmt"
import "os"
import "ssdb"

func main() {
    db := ssdb.Conn("127.0.0.1", 8888)
    defer db.Close()
    if db.Err != nil {
        fmt.Println(db.Err.Error())
        os.Exit(1)
    }

    db.Set("test", "456")

    ret, err := db.Get("test")
    fmt.Println(ret, err)

    db.Incr("test", 100)

    ret, err = db.Get("test")
    fmt.Println(ret, err)
}
