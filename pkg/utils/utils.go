package utils

import (
	"math/rand"
	"time"

	"k8s.io/klog"
)

const letters = "abcdefghijklmnopqrstuvwxyz0123456789"

// RandString generates length n random string
func RandString(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
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
