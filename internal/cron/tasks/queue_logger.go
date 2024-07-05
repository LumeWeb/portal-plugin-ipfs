package tasks

import (
	"fmt"
	"go.lumeweb.com/portal/core"
)

type ZapAdapter struct {
	logger *core.Logger
}

func NewZapLogAdapter(logger *core.Logger) *ZapAdapter {
	return &ZapAdapter{logger: logger}
}

func (za *ZapAdapter) Infof(format string, args ...interface{}) {
	za.logger.Info(fmt.Sprintf(format, args...))
}

func (za *ZapAdapter) Errorf(format string, args ...interface{}) {
	za.logger.Error(fmt.Sprintf(format, args...))
}

func (za *ZapAdapter) Fatalf(format string, args ...interface{}) {
	za.logger.Fatal(fmt.Sprintf(format, args...))
}

func (za *ZapAdapter) Info(args ...interface{}) {
	za.logger.Info(fmt.Sprint(args...))
}

func (za *ZapAdapter) Error(args ...interface{}) {
	za.logger.Error(fmt.Sprint(args...))
}

func (za *ZapAdapter) Fatal(args ...interface{}) {
	za.logger.Fatal(fmt.Sprint(args...))
}
