package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

type Logger interface {
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})

	Error(v ...interface{})
	Errorf(format string, v ...interface{})

	Info(v ...interface{})
	Infof(format string, v ...interface{})

	Warning(v ...interface{})
	Warningf(format string, v ...interface{})

	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})

	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
}

func SetLogger(l Logger) {
	raftLogger = l
}

var (
	defaultLogger = &DefaultLogger{Logger: log.New(os.Stderr, "raft", log.LstdFlags)}
	discardLogger = &DefaultLogger{Logger: log.New(ioutil.Discard, "", 0)}
	raftLogger    = Logger(defaultLogger)
)

const (
	calldepth = 2
)

type DefaultLogger struct {
	*log.Logger
	debug bool
}

func (l *DefaultLogger) EnableTimeStamps() {
	l.SetFlags(l.Flags() | log.Ldate | log.Ltime)
}
func (l *DefaultLogger) EnableDebug() {
	l.debug = true
}

func (l *DefaultLogger) Debug(v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("DEBUG", fmt.Sprint(v...)))
	}
}
func (l *DefaultLogger) Debugf(format string, v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("DEBUG", fmt.Sprintf(format, v...)))
	}
}
func (l *DefaultLogger) Info(v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("INFO", fmt.Sprint(v...)))
	}
}
func (l *DefaultLogger) Infof(format string, v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("INFO", fmt.Sprintf(format, v...)))
	}
}

func (l *DefaultLogger) Error(v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("ERROR", fmt.Sprint(v...)))
	}
}
func (l *DefaultLogger) Errorf(format string, v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("ERROR", fmt.Sprintf(format, v...)))
	}
}
func (l *DefaultLogger) Warning(v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("WARN", fmt.Sprint(v...)))
	}
}
func (l *DefaultLogger) Warningf(format string, v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("WARN", fmt.Sprintf(format, v...)))
	}
}

func (l *DefaultLogger) Fatal(v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("FATAL", fmt.Sprint(v...)))
	}
}
func (l *DefaultLogger) Fatalf(format string, v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("FATAL", fmt.Sprintf(format, v...)))
	}
}

func (l *DefaultLogger) Panic(v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("PANIC", fmt.Sprint(v...)))
	}
}
func (l *DefaultLogger) Panicf(format string, v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("PANIC", fmt.Sprintf(format, v...)))
	}
}

func header(lvl, msg string) string {
	return fmt.Sprintf("%s: %s", lvl, msg)
}
