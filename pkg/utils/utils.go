package utils

import (
	"crypto/md5" //nolint:gosec
	"encoding/hex"
	"math/big"
	"math/rand" //nolint:gosec

	"k8s.io/klog/v2"
)

const letters = "abcdefghijklmnopqrstuvwxyz0123456789"

// MakePassword generates length n password from str
func MakePassword(str string, n int) string {

	// md5 of the string
	sum := md5.Sum([]byte(str)) //nolint:gosec
	hexstr := hex.EncodeToString(sum[:])

	// get random seed with big
	bi := big.NewInt(0)
	bi.SetString(hexstr, 16)

	// generate password
	rand.Seed(bi.Int64())
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))] //nolint:gosec
	}
	return string(b)
}

// NamedLog inserts log name in klog
type NamedLog struct {
	Name string
}

// NewNamedLog returns new NamedLog
func NewNamedLog(name string) *NamedLog {
	return &NamedLog{Name: name + " "}
}

// Infof for klog.Infof
func (b *NamedLog) Infof(format string, v ...interface{}) {
	klog.Infof(b.Name+format, v...)
}

// Info for klog.Info
func (b *NamedLog) Info(string string) {
	klog.Info(b.Name + string)
}

// Warningf for klog.Warningf
func (b *NamedLog) Warningf(format string, v ...interface{}) {
	klog.Warningf(b.Name+format, v...)
}

// Warning for klog.Warning
func (b *NamedLog) Warning(string string) {
	klog.Warning(b.Name + string)
}
