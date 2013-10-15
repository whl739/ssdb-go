# ssdb-go

## Description

go client for [SSDB](https://github.com/ideawu/ssdb/)


## Usage

    import "ssdb"

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

    
    //pipeline
    pipe = db.Pipeline()
    pipe.Set("test", "123")
    pipe.Incr("test", 123)
    pipe.Get("test")
    ret, err := pipe.Exec()


