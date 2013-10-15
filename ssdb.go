package ssdb

import (
    "bytes"
    "errors"
    //"fmt"
    "net"
    "strconv"
    "strings"
)

type SSDB struct {
    con net.Conn
    Err error
}

type Pipe struct {
    ssdb     *SSDB
    cmds     []string
    recv_buf []byte
}

func Conn(host string, port int) *SSDB {
    remote := host + ":" + strconv.Itoa(port)
    con, err := net.Dial("tcp", remote)
    return &SSDB{con, err}
}

func (ssdb *SSDB) Close() error {
    return ssdb.con.Close()
}

func (ssdb *SSDB) Pipeline() *Pipe {
    cmds := []string{}
    recv_buf := []byte{}
    return &Pipe{ssdb, cmds, recv_buf}
}

func (pipe *Pipe) request(args ...string) {
    data := bytes.NewBuffer(nil)
    for _, arg := range args {
        p := strconv.Itoa(len(arg))
        data.WriteString(p)
        data.WriteByte('\n')
        data.WriteString(arg)
        data.WriteByte('\n')
    }
    data.WriteByte('\n')
    pipe.cmds = append(pipe.cmds, data.String())
}

func (pipe *Pipe) Set(key, value string) {
    pipe.request("set", key, value)
}

func (pipe *Pipe) Get(key string) {
    pipe.request("get", key)
}

func (pipe *Pipe) Incr(key string, increment int) {
    pipe.request("incr", key, strconv.Itoa(increment))
}

func (pipe *Pipe) Hincr(name string, key string, increment int) {
    pipe.request("hincr", name, key, strconv.Itoa(increment))
}

func (pipe *Pipe) Zincr(name string, key string, increment int) {
    pipe.request("zincr", name, key, strconv.Itoa(increment))
}

func (pipe *Pipe) Zrscan(name string) {
    pipe.request("zrscan", name, "", "", "", "10000000")
}

func (pipe *Pipe) Hscan(name string) {
    pipe.request("hscan", name, "", "", "10000000")
}

func (pipe *Pipe) send() {
    for _, v := range pipe.cmds {
        pipe.ssdb.con.Write([]byte(v))
    }
}

func (pipe *Pipe) recv_one() []string {
    var last byte

    n := len(pipe.recv_buf)
    for i := 0; i < n; i++ {
        if last == '\n' && pipe.recv_buf[i] == '\n' {
            str := parse(pipe.recv_buf[:i+1])
            pipe.recv_buf = pipe.recv_buf[i+1:]
            return str
        }

        last = pipe.recv_buf[i]
    }

    return nil
}

func (pipe *Pipe) recieve() []string {
    for {
        ret := pipe.recv_one()
        if ret == nil {
            var buf [1024]byte
            n, err := pipe.ssdb.con.Read(buf[0:])
            if err != nil {
                return nil
            }
            pipe.recv_buf = append(pipe.recv_buf, buf[0:n]...)
        } else {
            return ret
        }
    }
    return nil
}

func (pipe *Pipe) Exec() ([][]string, error) {
    pipe.send()

    ret := [][]string{}

    for i := 0; i < len(pipe.cmds); i++ {
        ret = append(ret, pipe.recieve())
    }

    pipe.cmds = pipe.cmds[:0]
    return ret, nil
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

func (ssdb *SSDB) Hincr(name string, key string, increment int) error {
    if err := ssdb.request("hincr", name, key, strconv.Itoa(increment)); err != nil {
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

        if bytes.Index(buf[0:n], []byte{'\n', '\n'}) != -1 {
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
            break
        }

        if i%2 == 1 {
            ret = append(ret, v)
        }
    }

    return ret
}
