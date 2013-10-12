package ssdb

import (
    "bytes"
    "errors"
    "fmt"
    "net"
    "strconv"
    "strings"
)

type SSDB struct {
    con net.Conn
    Err error
}

func Conn(host string, port int) *SSDB {
    remote := host + ":" + strconv.Itoa(port)
    con, err := net.Dial("tcp", remote)
    return &SSDB{con, err}
}

func (ssdb *SSDB) Close() error {
    return ssdb.con.Close()
}

func (ssdb *SSDB) Set(key, value string) error {
    if err := ssdb.request("set", key, value); err != nil {
        return err
    }

    ret, err := ssdb.recieve()
    if err != nil {
        return err
    }

    if !strings.EqualFold(ret[0], "ok") {
        return errors.New("Internal error, set failed")
    }

    return nil
}

func (ssdb *SSDB) Incr(key string, increment int) error {
    if err := ssdb.request("incr", key, strconv.Itoa(increment)); err != nil {
        return err
    }

    ret, err := ssdb.recieve()
    if err != nil {
        return err
    }

    if !strings.EqualFold(ret[1], "ok") {
        return errors.New("Internal error, incr failed")
    }

    return nil
}

func (ssdb *SSDB) Zincr(name string, key string, increment int) error {
    if err := ssdb.request("zincr", name, key, strconv.Itoa(increment)); err != nil {
        return err
    }

    ret, err := ssdb.recieve()
    if err != nil {
        return err
    }

    if !strings.EqualFold(ret[0], "ok") {
        return errors.New("Internal error, zincr failed")
    }

    return nil
}

func (ssdb *SSDB) Get(key string) (string, error) {
    if err := ssdb.request("get", key); err != nil {
        return "", err
    }

    ret, err := ssdb.recieve()
    if err != nil {
        return "", err
    }

    if !strings.EqualFold(ret[0], "ok") {
        return "", errors.New("Internal error: get failed")
    }

    return ret[1], nil
}

func (ssdb *SSDB) Zrscan(name string) (map[string]int, error) {
    if err := ssdb.request("zrscan", name, "", "", "", "100000000"); err != nil {
        return nil, err
    }

    ret, err := ssdb.recieve()
    if err != nil {
        return nil, err
    }

    if !strings.EqualFold(ret[0], "ok") {
        return nil, errors.New("Internal error: zrscan failed")
    }

    rmap := make(map[string]int)

    retlen := len(ret)
    if retlen%2 == 0 {
        return nil, errors.New("Server error: invalid response")
    }

    for i := 1; i < retlen; i += 2 {
        if strings.EqualFold(ret[i], "\n") {
            break
        }

        rmap[ret[i]], _ = strconv.Atoi(ret[i+1])
    }

    return rmap, nil
}

func (ssdb *SSDB) request(args ...string) error {
    data := bytes.NewBuffer(nil)
    for _, arg := range args {
        p := strconv.Itoa(len(arg))
        data.WriteString(p)
        data.WriteByte('\n')
        data.WriteString(arg)
        data.WriteByte('\n')
    }
    data.WriteByte('\n')

    _, err := ssdb.con.Write(data.Bytes())
    return err
}

func (ssdb *SSDB) recieve() ([]string, error) {
    result := bytes.NewBuffer(nil)
    var buf [512]byte
    for {
        n, err := ssdb.con.Read(buf[0:])

        result.Write(buf[0:n])

        if bytes.Index(buf[0:], []byte{'\n', '\n'}) != -1 {
            break
        }

        if err != nil {
            return nil, err
        }
    }

    ret := parse(result.Bytes())

    return ret, nil
}

func parse(result []byte) []string {
    sl := strings.Split(string(result), "\n")
    ret := []string{}

    for i, v := range sl {
        if strings.EqualFold(v, "") {
            fmt.Println("break")
            break
        }

        if i%2 == 1 {
            ret = append(ret, v)
        }
    }

    return ret
}
