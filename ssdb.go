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

func (ssdb *SSDB) send() {
	for _, v := range ssdb.cmds {
		ssdb.con.Write(v)
	}
}

func (ssdb *SSDB) request(args ...string) {
	data := bytes.NewBuffer(nil)
	for _, arg := range args {
		p := strconv.Itoa(len(arg))
		data.WriteString(p)
		data.WriteByte('\n')
		data.WriteString(arg)
		data.WriteByte('\n')
	}
	data.WriteByte('\n')

	ssdb.cmds = append(ssdb.cmds, data.Bytes())
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

func (ssdb *SSDB) recieve() []string {
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

func (ssdb *SSDB) Close() error {
	return ssdb.con.Close()
}

func (ssdb *SSDB) Batch() {
	ssdb.batch_mode = true
}

func (ssdb *SSDB) Exec() ([][]string, error) {
	ssdb.send()

	ret := [][]string{}

	for i := 0; i < len(ssdb.cmds); i++ {
		ret = append(ret, ssdb.recieve())
	}

	ssdb.cmds = ssdb.cmds[:0]
	ssdb.batch_mode = false

	return ret, nil
}

func (ssdb *SSDB) Set(key, value string) error {
	ssdb.request("set", key, value)

	if ssdb.batch_mode {
		return nil
	}

	ssdb.send()
	ret := ssdb.recieve()

	if !strings.EqualFold(ret[0], "ok") {
		return SsdbError("set failed")
	}

	return nil
}

func (ssdb *SSDB) Get(key string) (string, error) {
	ssdb.request("get", key)

	if ssdb.batch_mode {
		return "", nil
	}

	ssdb.send()
	ret := ssdb.recieve()

	if !strings.EqualFold(ret[0], "ok") {
		return "", SsdbError("incr failed")
	}

	return ret[1], nil
}

func (ssdb *SSDB) Del(key string) error {
	ssdb.request("del", key)

	if ssdb.batch_mode {
		return nil
	}

	ssdb.send()
	ret := ssdb.recieve()

	if !strings.EqualFold(ret[0], "ok") {
		return SsdbError("del failed")
	}

	return nil
}

func (ssdb *SSDB) Incr(key string, increment int) error {
	ssdb.request("incr", key, strconv.Itoa(increment))

	if ssdb.batch_mode {
		return nil
	}

	ssdb.send()
	ret := ssdb.recieve()

	if !strings.EqualFold(ret[0], "ok") {
		return SsdbError("incr failed")
	}

	return nil
}

func (ssdb *SSDB) Hincr(name string, key string, increment int) error {
	ssdb.request("hincr", name, key, strconv.Itoa(increment))

	if ssdb.batch_mode {
		return nil
	}

	ssdb.send()
	ret := ssdb.recieve()

	if !strings.EqualFold(ret[0], "ok") {
		return SsdbError("hincr failed")
	}

	return nil
}

func (ssdb *SSDB) Zincr(name string, key string, increment int) error {
	ssdb.request("zincr", name, key, strconv.Itoa(increment))

	if ssdb.batch_mode {
		return nil
	}

	ssdb.send()
	ret := ssdb.recieve()

	if !strings.EqualFold(ret[0], "ok") {
		return SsdbError("zincr failed")
	}

	return nil
}

func (ssdb *SSDB) Zrscan(name string, key_start string, score_start int,
	score_end int, limit int) (map[string]int, error) {
	ssdb.request("zrscan", name, key_start, strconv.Itoa(score_start),
		strconv.Itoa(score_end), strconv.Itoa(limit))

	if ssdb.batch_mode {
		return nil, nil
	}

	ssdb.send()
	ret := ssdb.recieve()

	if !strings.EqualFold(ret[0], "ok") {
		return nil, SsdbError("zrscan failed")
	}

	rmap := make(map[string]int)

	retlen := len(ret)
	if retlen%2 == 0 {
		return nil, SsdbError("invalid zrscan response")
	}

	for i := 1; i < retlen; i += 2 {
		if strings.EqualFold(ret[i], "\n") {
			break
		}

		rmap[ret[i]], _ = strconv.Atoi(ret[i+1])
	}

	return rmap, nil
}
