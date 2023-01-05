package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// https://zenn.dev/nakabonne/articles/cc0cde2bd94639
	// “enc変数はレコードサイズとインデックスエントリを永続化するため”
	// This material may be protected by copyright.
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex // sync.Mutexでリソースへのアクセスの排他的な制御を行う
	buf  *bufio.Writer
	size uint64
}

// factoy 関数
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (u uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// ファイルに直接書き込むのではなく、バッファ付きのライターに直接書き込み
	// https://note.crohaco.net/2019/golang-buffer/#[object%20Object]
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// “Read(pos uint64)は、指定された位置に格納されているレコードを返します。
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 強制書き込みがエラーの場合
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)

	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(size))

	//FileのReadAtで指定した位置から、データを読み込みbに書き込む
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// off = offset range　return p from off
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	// ファイルをcloseする前にバッファされたdataを永続化する
	return s.File.Close()
}
