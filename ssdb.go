package ssdb

import (
    "bytes"
    //"fmt"
    "net"
    "strconv"
    "strings"
)

type SsdbError string

func (err SsdbError) Error() string { return "SSDB Error: " + string(err) }

type SSDB struct {
    con        net.Conn
    Err        error
    batch_mode bool
    cmds       [][]byte
    recv_buf   []byte
}

func Conn(host string, port int) *SSDB {
    remote := host + ":" + strconv.Itoa(port)
    con, err := net.Dial("tcp", remote)
    return &SSDB{con, err, false, [][]byte{}, []byte{}}
}

func parse(result []byte) []string {
    sl := strings.Split(string(result), "\n")
    ret := []string{}

    for i, v := range sl {
        if v == “” {
            break
        }

        if i%2 == 1 {
            ret = append(ret, v)
        }
    }

    return ret
}

func (ssdb *SSDB) recv_one() []string {
    var last byte

    n := len(ssdb.recv_buf)
    for i := 0; i < n; i++ {
        if last == '\n' && ssdb.recv_buf[i] == '\n' {
            str := parse(ssdb.recv_buf[:i+1])
            ssdb.recv_buf = ssdb.recv_buf[i+1:]
            return str
        }

        last = ssdb.recv_buf[i]
    }

    return nil
}

func (ssdb *SSDB) recv() []string {
    for {
        ret := ssdb.recv_one()
        if ret == nil {
            var buf [1024 * 128]byte
            n, err := ssdb.con.Read(buf[0:])
            if err != nil {
                return nil
            }
            ssdb.recv_buf = append(ssdb.recv_buf, buf[0:n]...)
        } else {
            return ret
        }
    }
    return nil
}

func (ssdb *SSDB) send_req(data []byte) error {
    _, err := ssdb.con.Write(data)
    return err
}

func (ssdb *SSDB) request(cmd string, args ...string) (interface{}, error) {
    data := bytes.NewBuffer(nil)
    p := strconv.Itoa(len(cmd))
    data.WriteString(p)
    for _, arg := range args {
        p = strconv.Itoa(len(arg))
        data.WriteString(p)
        data.WriteByte('\n')
        data.WriteString(arg)
        data.WriteByte('\n')
    }
    data.WriteByte('\n')

    if ssdb.batch_mode {
        ssdb.cmds = append(ssdb.cmds, data.Bytes())
        return nil, nil
    }

    err := ssdb.send_req(data.Bytes())
    if err != nil {
        return nil, SsdbError(err.Error())
    }

    return ssdb.recv_resp(cmd)
}

func (ssdb *SSDB) recv_resp(cmd string) (interface{}, error) {
    resp := ssdb.recv()

    switch cmd {
    case "set":
        fallthrough
    case "zset":
        fallthrough
    case "hset":
        fallthrough
    case "del":
        fallthrough
    case "zdel":
        fallthrough
    case "hdel":
        fallthrough
    case "hsize":
        fallthrough
    case "zsize":
        fallthrough
    case "exists":
        fallthrough
    case "hexists":
        fallthrough
    case "zexists":
        fallthrough
    case "multi_set":
        fallthrough
    case "multi_del":
        fallthrough
    case "multi_hset":
        fallthrough
    case "multi_hde":
        fallthrough
    case "multi_zset":
        fallthrough
    case "multi_zdel":
        fallthrough
    case "incr":
        fallthrough
    case "decr":
        fallthrough
    case "zincr":
        fallthrough
    case "zdecr":
        fallthrough
    case "hincr":
        fallthrough
    case "hdecr":
        if resp[0] == "ok" {
            return nil, nil
        }
        return nil, SsdbError(cmd + " failed")

    case "zget":
        fallthrough
    case "get":
        fallthrough
    case "hget":
        if resp[0] != "ok" {
            return nil, SsdbError(resp[1])
        }

        if len(resp) == 2 {
            return resp[1], nil
        } else {
            return nil, SsdbError("Invalid response")
        }

    case "keys":
        fallthrough
    case "zkeys":
        fallthrough
    case "hkeys":
        fallthrough
    case "hlist":
        fallthrough
    case "zlist":
        if resp[0] != "ok" {
            return nil, SsdbError(cmd + " failed")
        }

        data := []string{}
        for i := 1; i < len(resp); i++ {
            data = append(data, resp[i])
        }
        return data, nil

    case "scan":
        fallthrough
    case "rscan":
        fallthrough
    case "zscan":
        fallthrough
    case "zrscan":
        fallthrough
    case "hscan":
        fallthrough
    case "hrscan":
        fallthrough
    case "multi_hsize":
        fallthrough
    case "multi_zsize":
        fallthrough
    case "multi_get":
        fallthrough
    case "multi_hget":
        fallthrough
    case "multi_zget":
        fallthrough
    case "multi_exists":
        fallthrough
    case "multi_hexists":
        fallthrough
    case "multi_zexists":
        if resp[0] != "ok" || len(resp)%2 != 1 {
            return nil, SsdbError(cmd + " failed")
        }

        data := []map[string]string{}
        for i := 1; i < len(resp); i += 2 {
            m := map[string]string{resp[i]: resp[i+1]}
            data = append(data, m)
        }
        return data, nil
    default:
        return resp[0], nil
    }

    return nil, SsdbError("Unknown command: " + cmd)
}

func (ssdb *SSDB) Close() error {
    return ssdb.con.Close()
}

func (ssdb *SSDB) Batch() {
    ssdb.batch_mode = true
}

func (ssdb *SSDB) Exec() ([]interface{}, error) {

    for _, v := range ssdb.cmds {
        ssdb.send_req(v)
    }

    ret := []interface{}{}

    for _, v := range ssdb.cmds {
        resp, _ := ssdb.recv_resp(string(v))
        ret = append(ret, resp)
    }

    ssdb.cmds = ssdb.cmds[:0]
    ssdb.batch_mode = false

    return ret, nil
}

func (ssdb *SSDB) Set(key, value string) error {
    _, err := ssdb.request("set", key, value)

    return err
}

func (ssdb *SSDB) Get(key string) (string, error) {
    ret, err := ssdb.request("get", key)
    if ret != nil {
        return ret.(string), err
    }

    return "", err
}

func (ssdb *SSDB) Del(key string) error {
    _, err := ssdb.request("del", key)

    return err
}

func (ssdb *SSDB) Incr(key string, increment int) error {
    _, err := ssdb.request("incr", key, strconv.Itoa(increment))

    return err
}

func (ssdb *SSDB) Hincr(name string, key string, increment int) error {
    _, err := ssdb.request("hincr", name, key, strconv.Itoa(increment))

    return err
}

func (ssdb *SSDB) Zincr(name string, key string, increment int) error {
    _, err := ssdb.request("zincr", name, key, strconv.Itoa(increment))

    return err
}

func (ssdb *SSDB) Zrscan(name string, key_start string, score_start int,
    score_end int, limit int) ([]map[string]string, error) {
    ret, err := ssdb.request("zrscan", name, key_start, strconv.Itoa(score_start),
        strconv.Itoa(score_end), strconv.Itoa(limit))

    if ret != nil {
        return ret.([]map[string]string), err
    }

    return nil, nil
}
